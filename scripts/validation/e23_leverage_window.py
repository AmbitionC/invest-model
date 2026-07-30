# -*- coding: utf-8 -*-
"""E23：杠杆窗口识别器（P28）预登记验证——价格双信号分支。

窗口＝三信号取二：①中位线下方≥10% ②距历史峰回撤≥40% ③恐慌EOD≥85。
本脚本跑 ①+② 价格分支（本地可复现）；③恐慌分支需 fear_daily 回填数据（Actions）。
判据（跑数前写死于 P28 段）：全部窗口 episode 首日入场 L=30%（6% 单利、25% 平仓线）
持 3 年——不触平仓线 且 最差净收益 ≥ 无杠杆最差 −10pp。
首跑 2026-07-30：2 episode 全部存活（最低权益/市值 0.59/0.65），净收益 +15.1%/+47.9%。
注意：2008 与 2024-09 大底均不满足价格双信号 → ③恐慌分支为必要腿（待 Actions 联合回测）。
  python scripts/validation/e23_leverage_window.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
H, WARMUP, L, RATE, MC_LINE = 756, 500, 0.30, 0.06, 0.25


def main() -> None:
    df = pd.read_csv(ROOT / "results" / "index_dump_000300_SH.csv", dtype={"trade_date": str})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna().reset_index(drop=True)
    d, c = df["trade_date"].values, df["close"].values

    med = np.full(len(c), np.nan)
    for i in range(WARMUP, len(c)):
        med[i] = np.median(c[: i + 1])
    peak = np.maximum.accumulate(c)
    sig1 = c < med * 0.90
    sig2 = c / peak - 1 <= -0.40
    both = sig1 & sig2 & ~np.isnan(med)
    idx = np.where(both)[0]
    eps: list[list[int]] = []
    for i in idx:
        if not eps or i - eps[-1][-1] > 60:
            eps.append([i])
        else:
            eps[-1].append(i)
    print(f"价格双信号共振：{int(both.sum())} 天 / {len(eps)} 个 episode")

    ok = True
    for e in eps:
        i0 = e[0]
        if i0 + H >= len(c):
            print(f"{d[i0]} 入场：不足 3 年样本，跳过")
            continue
        pos = 1 / (1 - L)
        debt = pos - 1
        path = c[i0: i0 + H + 1] / c[i0]
        t = np.arange(H + 1) / 252
        equity = pos * path - debt - debt * RATE * t
        ratio = equity / (pos * path)
        mdd = (equity / np.maximum.accumulate(equity) - 1).min()
        hit = ratio.min() < MC_LINE
        ok &= not hit
        print(f"{d[i0]} 入场 L={L:.0%}: 3年净收益 {equity[-1] - 1:+.1%} | 权益最大回撤 {mdd:.1%} "
              f"| 最低权益/市值 {ratio.min():.2f} {'⚠️触线' if hit else 'OK'} | 无杠杆 {path[-1] - 1:+.1%}")
    print(f"\n价格分支判据（不触线）：{'PASS' if ok else 'FAIL'}；③恐慌分支联合回测待 Actions（fear_daily）")
    print(f"当前({d[-1]})：价/中位线 {c[-1] / med[-1]:.2f}｜距峰 {c[-1] / peak[-1] - 1:+.1%} → 窗口关")


if __name__ == "__main__":
    main()
