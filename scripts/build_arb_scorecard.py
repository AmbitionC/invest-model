"""套利战绩记分卡：统计盲区 α 候选与 carry 信号的真实前瞻收益，落库 arb_scorecard。

口径对齐 signal_scorecard：入场＝信号日之后首个交易日收盘，收益＝最新收盘/入场-1，
超额＝减去沪深300同窗口。按 sleeve（carry）与 grade（α）分桶。只读派生表 + 行情，
写 arb_scorecard。观察态数据不足时安静跳过。

用法：python scripts/build_arb_scorecard.py
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


def _fwd_return(repo: BaseRepository, code: str, entry_date: str) -> tuple[float, float, int] | None:
    """从 entry_date 后首个交易日收盘买入，持有至最新收盘的收益/超额/持有交易日。"""
    px = repo.read_sql(
        "SELECT trade_date, close FROM stock_daily WHERE code=:c AND trade_date>:d "
        "ORDER BY trade_date", {"c": code, "d": entry_date})
    if px.empty or len(px) < 2:
        return None
    p0 = float(pd.to_numeric(px["close"].iloc[0], errors="coerce"))
    p1 = float(pd.to_numeric(px["close"].iloc[-1], errors="coerce"))
    if not (p0 > 0 and np.isfinite(p1)):
        return None
    ret = p1 / p0 - 1.0
    d0, d1 = str(px["trade_date"].iloc[0]), str(px["trade_date"].iloc[-1])
    bench = repo.read_sql(
        "SELECT close FROM index_daily WHERE code=:b AND trade_date IN (:a,:z) ORDER BY trade_date",
        {"b": BENCH, "a": d0, "z": d1})
    excess = ret
    if len(bench) == 2:
        b0, b1 = float(bench["close"].iloc[0]), float(bench["close"].iloc[-1])
        if b0 > 0:
            excess = ret - (b1 / b0 - 1.0)
    return ret, excess, len(px)


def _bucket(rows: list[dict], bucket: str, label: str, as_of: str) -> dict | None:
    if not rows:
        return None
    rets = np.array([r["ret"] for r in rows], float)
    exc = np.array([r["excess"] for r in rows], float)
    hold = np.array([r["hold"] for r in rows], float)
    return {"as_of": as_of, "bucket": bucket, "label": label, "n": len(rows),
            "win_rate": float((rets > 0).mean()), "mean_ret": float(rets.mean()),
            "median_ret": float(np.median(rets)), "mean_excess": float(exc.mean()),
            "mean_hold_days": float(hold.mean())}


def build(engine) -> int:
    repo = BaseRepository(engine)
    asof = repo.read_sql("SELECT MAX(trade_date) m FROM stock_daily")
    as_of = str(asof["m"].iloc[0]) if not asof.empty and asof["m"].iloc[0] else None
    if not as_of:
        return 0
    out: list[dict] = []

    # 盲区 α：按 grade + 全体
    if repo.table_exists("alpha_candidate"):
        ac = repo.read_sql("SELECT code, as_of_date, grade, falsified FROM alpha_candidate")
        buckets: dict[str, list[dict]] = {}
        for _, r in ac.iterrows():
            fr = _fwd_return(repo, str(r["code"]), str(r["as_of_date"]))
            if fr is None:
                continue
            rec = {"ret": fr[0], "excess": fr[1], "hold": fr[2]}
            buckets.setdefault("alpha_ALL", []).append(rec)
            g = str(r["grade"] or "NA")
            buckets.setdefault(f"alpha_{g}", []).append(rec)
        for b, rows in buckets.items():
            row = _bucket(rows, b, b.replace("alpha_", "盲区α·"), as_of)
            if row:
                out.append(row)

    # carry：按 sleeve（红利/可转债的标的前瞻收益；逆回购近似无价差略过）
    if repo.table_exists("carry_signal"):
        cs = repo.read_sql(
            "SELECT code, trade_date, sleeve FROM carry_signal "
            "WHERE sleeve IN ('dividend_carry','convertible')")
        buckets = {}
        for _, r in cs.iterrows():
            fr = _fwd_return(repo, str(r["code"]), str(r["trade_date"]))
            if fr is None:
                continue
            buckets.setdefault(str(r["sleeve"]), []).append(
                {"ret": fr[0], "excess": fr[1], "hold": fr[2]})
        for b, rows in buckets.items():
            row = _bucket(rows, b, {"dividend_carry": "红利carry",
                                    "convertible": "可转债双低"}.get(b, b), as_of)
            if row:
                out.append(row)

    if not out:
        return 0
    return repo.upsert("arb_scorecard", pd.DataFrame(out), ["as_of", "bucket"])


def main() -> None:
    ap = argparse.ArgumentParser(description="套利战绩记分卡")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db)
    create_schema(engine)
    n = build(engine)
    print(f"arb_scorecard 落库 {n} 个分桶。")


if __name__ == "__main__":
    main()
