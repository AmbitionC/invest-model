"""个股·顶部识别（C overlay）是否也适用？——把指数级的顶部识别检验下沉到个股。

用户命题（2026-07-13）："顶部识别是个不错的视角，看下是不是也适用于个股。"

做法：对 stock_daily 里有足够历史的个股，逐票**前复权**（P11，qfq_close_hist；除权缺口否则
会假造波动骤升/假破 MA60），检测每票的所有 MA60 上涨周期（同 multicycle 口径），每个周期跑：

  C   顶部识别（P16 候选，判据预登记：浮盈≥15% 后 20 日波动≥近250日80分位 且 5/60量比≥1.5）
  A'' 重远·有效跌破（连 CONFIRM_DAYS 日确认，忠实口径，非单日裸破）
  基线 买入持有到周期终点

聚合关注**个股上 C 的触发率与触发后是否跑赢买入持有 / 少回吐**——这才是"顶部识别是否适用于个股"。
不跑系统 risk.py 全回放（个股噪声大、逐日复权回放代价高，本脚本聚焦 C 的可迁移性）。

预登记：周期资格与 C 判据沿用 multicycle / chinext 常量（跑数前写死，勿按结果回调）。
只读、不落库、不改生产。需在有 DB + stock_daily 的环境（Actions/FC）跑。
  python scripts/analysis/stock_top_detection_backtest.py [--db ...] [--limit 300] [--out ...]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.data.adjust import apply_qfq  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from chinext_cycle_backtest import (  # noqa: E402
    _stats, _exit_top, _exit_confirmed_ma60, CONFIRM_DAYS,
)
from multicycle_exit_backtest import _detect_all_cycles, _above_frac, _agg  # noqa: E402

MIN_ROWS = 400          # 至少 400 交易日才纳入（保证能出周期）
MIN_CYCLES_TOTAL = 20   # 少于此周期数则提示样本偏薄（不阻断）


def _load_stock(repo: BaseRepository, code: str) -> pd.DataFrame:
    """取个股前复权收盘 + 原始成交量，对齐成 df(trade_date, close, volume)。"""
    raw = repo.read_sql(
        "SELECT trade_date, close, volume FROM stock_daily WHERE code=:c ORDER BY trade_date",
        {"c": code})
    if raw.empty:
        return raw
    raw["trade_date"] = raw["trade_date"].astype(str)
    s = pd.to_numeric(raw.set_index("trade_date")["close"], errors="coerce")
    start, end = raw["trade_date"].iloc[0], raw["trade_date"].iloc[-1]
    qfq = apply_qfq(repo, code, s, start, end)          # 前复权（fail-open 原样返回）
    out = pd.DataFrame({
        "trade_date": raw["trade_date"].values,
        "close": pd.to_numeric(qfq.values, errors="coerce"),
        "volume": pd.to_numeric(raw["volume"], errors="coerce").values,
    })
    return out.dropna(subset=["close"]).reset_index(drop=True)


def run(repo: BaseRepository, limit: int | None) -> str:
    codes_df = repo.read_sql(
        "SELECT code, COUNT(*) AS n FROM stock_daily GROUP BY code HAVING n>=:m ORDER BY code",
        {"m": MIN_ROWS})
    codes = codes_df["code"].tolist() if not codes_df.empty else []
    if limit:
        codes = codes[:limit]

    L = ["# 个股·顶部识别（C overlay）适用性回测", ""]
    L.append(f"- 个股池：stock_daily 中 ≥{MIN_ROWS} 交易日者，共 {len(codes)} 只"
             f"{f'（截断前 {limit}）' if limit else ''}；收盘**前复权**（P11），量取原始")
    L.append(f"- 周期资格与 C 判据沿用 multicycle/chinext 预登记常量；A'' 为忠实「有效跌破」（连{CONFIRM_DAYS}日确认）")
    if not codes:
        L.append(f"\n⚠️ stock_daily 无 ≥{MIN_ROWS} 日的个股，检查数据回填。")
        return "\n".join(L)

    print(f"[progress] 个股池 {len(codes)} 只，开始逐票回测…", flush=True)
    cycles: list[dict] = []
    n_stocks_with_cycle = 0
    for idx, code in enumerate(codes, 1):
        if idx % 100 == 0:
            print(f"[progress] {idx}/{len(codes)} 只已处理，累计周期 {len(cycles)}", flush=True)
        df = _load_stock(repo, code)
        if len(df) < 200:
            continue
        ma60 = df["close"].rolling(60).mean().to_numpy()
        cyc = _detect_all_cycles(df)
        if cyc:
            n_stocks_with_cycle += 1
        for s, e in cyc:
            ec, rc = _exit_top(df.iloc[:e + 1], s)
            ec = min(ec, e)
            ea2, ra2 = _exit_confirmed_ma60(df, ma60, s, e, 0.0)
            C = _stats(df["close"], s, ec, "C", rc)
            A2 = _stats(df["close"], s, ea2, "A''", ra2)
            hold_ret = float(df["close"].iloc[e]) / float(df["close"].iloc[s]) - 1
            peak_ret = float(df["close"].iloc[s:e + 1].max()) / float(df["close"].iloc[s]) - 1
            cycles.append({
                "code": code, "days": e - s,
                "peak_ret": peak_ret, "hold_ret": hold_ret,
                "above_frac": _above_frac(df, s, e),
                "C": C, "A2": A2, "c_fired": "未" not in rc,
            })

    n = len(cycles)
    L.append(f"\n## 一、样本")
    L.append(f"- 有效个股（检出≥1 周期）：{n_stocks_with_cycle} 只；**合格周期合计 {n} 个**"
             f"{'（样本偏薄，谨慎解读）' if n < MIN_CYCLES_TOTAL else ''}")
    if n == 0:
        L.append("\n⚠️ 未检出个股周期，检查 stock_daily 覆盖或放宽阈值（阈值本次不动）。")
        return "\n".join(L)

    hold_base = [r["hold_ret"] for r in cycles]

    def winrate(vals):
        w = sum(1 for v, h in zip(vals, hold_base) if np.isfinite(v) and v > h)
        return w / n

    def colstat(key, sub):
        return _agg([r[key][sub] for r in cycles])

    # 买入持有基线的回撤/捕获
    hold_gb = [(1 + r["hold_ret"]) / (1 + r["peak_ret"]) - 1 for r in cycles]
    hold_cap = [r["hold_ret"] / r["peak_ret"] if r["peak_ret"] > 0 else float("nan") for r in cycles]

    L.append(f"\n## 二、跨个股周期聚合（{n} 个周期）")
    L.append("\n| 策略 | 中位持有收益 | 平均持有收益 | 平均自峰值回撤 | 平均捕获率 | 对买入持有胜率 |")
    L.append("|---|---|---|---|---|---|")
    for key, disp in [("C", "C 顶部识别"), ("A2", f"A'' 重远·有效跌破(连{CONFIRM_DAYS}日)")]:
        med_r, mean_r = colstat(key, "持有收益")
        _, mean_gb = colstat(key, "自峰值回撤")
        _, mean_cap = colstat(key, "捕获率")
        rets = [r[key]["持有收益"] for r in cycles]
        L.append(f"| {disp} | {med_r:+.1%} | {mean_r:+.1%} | {mean_gb:+.1%} | {mean_cap:.0%} | {winrate(rets):.0%} |")
    L.append(f"| 基线·买入持有到终点 | {_agg(hold_base)[0]:+.1%} | {_agg(hold_base)[1]:+.1%} | "
             f"{_agg(hold_gb)[1]:+.1%} | {_agg(hold_cap)[1]:.0%} | — |")

    # 三、C 的可迁移性——触发率 + 触发后表现
    fired = [r for r in cycles if r["c_fired"]]
    L.append(f"\n## 三、顶部识别（C）在个股上是否适用")
    L.append(f"- **触发率**：{len(fired)}/{n} 周期触发（{len(fired)/n:.0%}）"
             f"{'——个股放量+波动顶特征比指数更常见' if len(fired)/n > 0.5 else ''}")
    if fired:
        fh = [r["hold_ret"] for r in fired]
        c_win = sum(1 for r in fired if r["C"]["持有收益"] > r["hold_ret"])
        c_cap = _agg([r["C"]["捕获率"] for r in fired])[1]
        c_gb = _agg([r["C"]["自峰值回撤"] for r in fired])[1]
        h_gb = _agg([(1 + r["hold_ret"]) / (1 + r["peak_ret"]) - 1 for r in fired])[1]
        L.append(f"- **触发周期里**：C 平均捕获率 {c_cap:.0%}，平均自峰值回撤 {c_gb:+.1%}"
                 f"（同组买入持有回撤 {h_gb:+.1%}）；{c_win}/{len(fired)} 个跑赢买入持有"
                 f"（{c_win/len(fired):.0%}）")
        L.append(f"- 未触发周期 C 退化为买入持有，不计入 C 的有效证据。")
        verdict_ok = (len(fired) >= 8 and c_win / len(fired) >= 0.55 and c_gb > h_gb)
        L.append(f"- **个股适用性初判**：{'✅ 触发率与触发后表现均正向，顶部识别在个股上方向成立' if verdict_ok else '⚠️ 方向性存在但未达 E12 类判据（触发≥8 且跑赢≥55% 且回撤更优），需更大个股样本确认'}"
                 f"（判据比照 E12，非正式裁决）。")
    else:
        L.append("- 本样本个股 C 从未触发 → 个股上无有效证据。")

    L.append("\n## 四、诚实标注")
    L.append("- 个股噪声远大于指数：周期检测靠 MA60 区制启发式，个股假突破更多、周期更碎。")
    L.append("- 收盘已前复权（P11），量未复权——送转在 60 日窗口内会略扰动量比（罕见，已知偏差）。")
    L.append("- 个股池=有历史行情的票（含已调出/退池者），非当前持仓；作**可迁移性方向判断**，非选股或实盘裁决。")
    L.append(f"- C 判据为跑数前预登记（沿用指数级常量），未按个股结果回调——若晋升仍须过 E12 正式检验。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="个股顶部识别适用性回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=None, help="限制个股数（调试用；默认全池）")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())
    md = run(repo, args.limit)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
