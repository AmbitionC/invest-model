"""投顾信号实战战绩记分卡：统计每条投顾信号「买入后」的真实前瞻收益，落库 signal_scorecard。

这是「跟着系统到底有没有用」的真答案——系统靠投顾选股，所以真正该考核的是投顾信号
的前瞻收益，而不是量化引擎的历史回测（那衡量的是参谋、且非实盘）。

口径（与研报速通/退出栈验证同源）：
  - 每只 code 取「首次被推荐」那条信号（同票多次推荐只算最早一次，避免重复计数）；
  - 入场＝信号日之后第一个交易日收盘；收益＝最新收盘 / 入场收盘 - 1（买入并持有至今）；
  - 超额＝减去沪深300同窗口涨跌；
  - 按 来源(research/intraday) × 等级(A/B/C) 及汇总分桶。
只读 advisor_reco / stock_daily / index_daily，写 signal_scorecard。

用法：python scripts/build_signal_scorecard.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

BENCH = "000300.SH"


def _bucket_rows(df: pd.DataFrame, as_of: str) -> list[dict]:
    """按 来源×等级 + 来源汇总 + 全体 分桶聚合。df 列：stype, grade, ret, excess, hold."""
    out: list[dict] = []

    def agg(sub: pd.DataFrame, bucket: str, label: str) -> None:
        if sub.empty:
            return
        out.append({
            "as_of": as_of, "bucket": bucket, "label": label, "n": int(len(sub)),
            "win_rate": round(float((sub["ret"] > 0).mean()), 4),
            "mean_ret": round(float(sub["ret"].mean()), 6),
            "median_ret": round(float(sub["ret"].median()), 6),
            "mean_excess": round(float(sub["excess"].mean()), 6),
            "mean_hold_days": round(float(sub["hold"].mean()), 2),
        })

    labels = {"research": "研报", "intraday": "盘中"}
    for st in ("research", "intraday"):
        s = df[df["stype"] == st]
        agg(s, st, labels[st])
        for g in ("A", "B", "C"):
            agg(s[s["grade"] == g], f"{st}/{g}", f"{labels[st]}{g}级")
    agg(df, "ALL", "全部信号")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="投顾信号实战战绩记分卡")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db) if args.db else make_engine()
    create_schema(engine)
    repo = BaseRepository(engine)

    as_of = str(repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0])
    reco = repo.read_sql(
        "SELECT rec_date, code, source_type, grade FROM advisor_reco WHERE direction='long'")
    if reco.empty:
        print("advisor_reco 无 long 信号，跳过。")
        return
    # 首次推荐去重
    reco["rec_date"] = reco["rec_date"].astype(str)
    first = reco.sort_values("rec_date").drop_duplicates("code", keep="first")

    codes = sorted(set(first["code"]))
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    px = repo.read_sql(
        f"SELECT code, trade_date, close FROM stock_daily WHERE code IN ({ph}) "
        f"ORDER BY code, trade_date", params)
    px["close"] = pd.to_numeric(px["close"], errors="coerce")
    bench = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c ORDER BY trade_date",
        {"c": BENCH})
    bench["close"] = pd.to_numeric(bench["close"], errors="coerce")
    bmap = dict(zip(bench["trade_date"].astype(str), bench["close"]))

    def bench_at(d: str) -> float:
        m = bench[bench["trade_date"].astype(str) <= d]
        return float(m["close"].iloc[-1]) if len(m) else float("nan")

    by_code = {c: g.reset_index(drop=True) for c, g in px.groupby("code")}
    rows = []
    for _, e in first.iterrows():
        g = by_code.get(e["code"])
        if g is None or g.empty:
            continue
        dts = g["trade_date"].astype(str).tolist()
        cls = g["close"].tolist()
        nxt = [i for i, t in enumerate(dts) if t > e["rec_date"]]
        if not nxt:
            continue
        i0 = nxt[0]
        entry, entdate = cls[i0], dts[i0]
        last, lastdate = cls[-1], dts[-1]
        if not (entry and last and entry > 0):
            continue
        ret = last / entry - 1
        b0, b1 = bench_at(entdate), bench_at(lastdate)
        excess = ret - (b1 / b0 - 1 if b0 and b1 and b0 > 0 else 0.0)
        rows.append({"stype": str(e["source_type"]), "grade": str(e["grade"] or ""),
                     "ret": ret, "excess": excess, "hold": len(dts) - i0})

    if not rows:
        print("无可计算样本。")
        return
    df = pd.DataFrame(rows)
    cards = _bucket_rows(df, as_of)
    repo.upsert("signal_scorecard", pd.DataFrame(cards), ["as_of", "bucket"])
    print(f"✓ 信号战绩记分卡 as_of={as_of} 落库 {len(cards)} 桶，共 {len(df)} 条信号")
    for c in cards:
        print(f"  {c['label']:8s} n={c['n']:3d} 胜率{c['win_rate']:.0%} "
              f"均值{c['mean_ret']:+.1%} 超额{c['mean_excess']:+.1%}")


if __name__ == "__main__":
    main()
