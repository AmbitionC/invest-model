# -*- coding: utf-8 -*-
"""E21：沪深300 中位线高抛低吸纪律（P26）预登记验证。

判据写死于 docs/model_change_proposals.md P26 段（2026-07-30 跑数前登记），
源自重远投资观《五层能力圈》第二层主张：中位线上方只卖不买、下方只买不卖。

数据：results/index_dump_000300_SH.csv（tushare 000300.SH 全历史收盘，index-dump workflow 产物）。
策略（小白月频口径）：每月最后交易日——收盘<expanding中位线(预热500td)则资金池全额买入；
收盘>中位线则卖持仓 5% 回池；每月外部流入 1 单位、现金 0 收益。基线＝同现金流定投买入持有。
判据：①策略 XIRR ≥ 基线+1pp ②最大回撤 ≤ 基线+5pp ③下方日未来3年年化 ≥ 上方+2pp
④warmup 500/1000 两档①③同向。①必过且 ≥3/4 → 提示行上线。

首跑 2026-07-30 = 4/4 PASS（详见 P26 段回填），提示行已上（action_plan._hs300_median_hint）。
  python scripts/validation/e21_hs300_median.py
"""

from __future__ import annotations

import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
H = 756  # 判据③ 前瞻窗（约3年交易日）


def _yrs(a: str, b: str) -> float:
    da = datetime.datetime.strptime(a, "%Y%m%d")
    db = datetime.datetime.strptime(b, "%Y%m%d")
    return (db - da).days / 365.25


def run(warmup: int, d: np.ndarray, c: np.ndarray) -> dict:
    med = np.full(len(c), np.nan)
    for i in range(warmup, len(c)):
        med[i] = np.median(c[: i + 1])
    ym = pd.Series(d).str[:6]
    month_end = (ym != ym.shift(-1)).values

    def simulate(strategy: str):
        sh = pool = 0.0
        flows: list[tuple[str, float]] = []
        vals: list[float] = []
        for i in range(len(c)):
            if not month_end[i]:
                continue
            pool += 1.0
            flows.append((d[i], -1.0))
            if np.isnan(med[i]) or strategy == "base":
                sh += pool / c[i]
                pool = 0.0
            elif c[i] < med[i]:
                sh += pool / c[i]
                pool = 0.0
            elif c[i] > med[i]:
                pool += sh * 0.05 * c[i]
                sh *= 0.95
            vals.append(sh * c[i] + pool)
        terminal = sh * c[-1] + pool
        flows.append((d[-1], terminal))
        ts = np.array([_yrs(flows[0][0], f[0]) for f in flows])
        cf = np.array([f[1] for f in flows])

        def npv(r: float) -> float:
            return float((cf / (1 + r) ** ts).sum())

        lo, hi = -0.5, 1.0
        for _ in range(200):
            mid = (lo + hi) / 2
            lo, hi = (mid, hi) if npv(mid) > 0 else (lo, mid)
        v = np.array(vals)
        peak = np.maximum.accumulate(v)
        return (lo + hi) / 2, float(((v - peak) / peak).min()), terminal

    x_s, dd_s, term_s = simulate("strat")
    x_b, dd_b, term_b = simulate("base")

    fwd = np.full(len(c), np.nan)
    for i in range(len(c) - H):
        fwd[i] = (c[i + H] / c[i]) ** (252 / H) - 1
    mask = ~np.isnan(med) & ~np.isnan(fwd)
    below = fwd[mask & (c < med)]
    above = fwd[mask & (c > med)]
    return dict(warmup=warmup, xirr_s=x_s, xirr_b=x_b, dd_s=dd_s, dd_b=dd_b,
                term_s=term_s, term_b=term_b,
                below=float(below.mean()), above=float(above.mean()),
                n_below=int(len(below)), n_above=int(len(above)))


def main() -> None:
    df = pd.read_csv(ROOT / "results" / "index_dump_000300_SH.csv", dtype={"trade_date": str})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna().reset_index(drop=True)
    d, c = df["trade_date"].values, df["close"].values
    print(f"样本 {len(df)} 天 {d[0]} ~ {d[-1]}（判据要求 ≥4000）")
    results = [run(w, d, c) for w in (500, 1000)]
    for r in results:
        print(f"\n== warmup={r['warmup']} ==")
        print(f"①收益: 策略 XIRR {r['xirr_s']:.2%} vs 基线 {r['xirr_b']:.2%} Δ={100 * (r['xirr_s'] - r['xirr_b']):+.2f}pp（需≥+1pp）")
        print(f"②回撤: 策略 {r['dd_s']:.1%} vs 基线 {r['dd_b']:.1%}（需 ≤ 基线+5pp）")
        print(f"③区分力: 下方未来3年年化 {r['below']:.2%}(n={r['n_below']}) vs 上方 {r['above']:.2%}(n={r['n_above']}) Δ={100 * (r['below'] - r['above']):+.1f}pp（需≥+2pp）")
    p1 = all(r["xirr_s"] - r["xirr_b"] >= 0.01 for r in results[:1])
    p2 = abs(results[0]["dd_s"]) <= abs(results[0]["dd_b"]) + 0.05
    p3 = results[0]["below"] - results[0]["above"] >= 0.02
    p4 = all((r["xirr_s"] > r["xirr_b"]) and (r["below"] > r["above"]) for r in results)
    n = sum([p1, p2, p3, p4])
    print(f"\n判据: ①{'✅' if p1 else '❌'} ②{'✅' if p2 else '❌'} ③{'✅' if p3 else '❌'} ④{'✅' if p4 else '❌'} → {n}/4 "
          + ("PASS（①过且≥3/4）→ 提示行上线" if p1 and n >= 3 else "FAIL → 记知识库"))


if __name__ == "__main__":
    main()
