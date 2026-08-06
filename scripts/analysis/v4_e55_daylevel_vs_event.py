"""E55 附：日级平均 vs 事件级平均的差距归因。

主线 §2.5 报的「≤10分位 250日前瞻 +14.66%」是 **367 个重叠日窗的日级平均**。
本脚本复现该日级数字，并逐 episode 拆开看它由谁贡献 —— 回答「这个梯度是不是
被某一段（如 2024-09 之前的连续触发）单独扛起来的」。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v4_common import episodes_from_flags, load_amount, load_legs, rolling_pct

QS = [0.05, 0.10, 0.20, 0.30]
HORIZONS = [20, 60, 120, 250]


def fwd(px, dates, h, lag=1):
    p = px.reindex(dates).to_numpy(float)
    n = len(p)
    out = np.full(n, np.nan)
    for i in range(n):
        a, b = i + lag, i + lag + h
        if b < n:
            out[i] = p[b] / p[a] - 1.0
    return out


def main():
    hs300 = load_legs()["沪深300"]["px"]
    amt = load_amount()
    pct = rolling_pct(amt)
    dates = amt.index.to_numpy()
    elig = pct.notna().to_numpy()
    F = {h: fwd(hs300, dates, h) for h in HORIZONS}

    print("=" * 78)
    print("[A] 复现主线日级读数（全样本基准 = 所有交易日，含预热期）")
    print("=" * 78)
    allbase = {h: np.nanmean(F[h]) for h in HORIZONS}
    print("全样本基准: " + "  ".join(f"{h}d={allbase[h]*100:+.2f}%" for h in HORIZONS))
    for q in QS:
        fl = (pct.to_numpy() <= q) & elig
        print(f"q={q:.0%} (n={int(fl.sum())}日): " +
              "  ".join(f"{h}d={np.nanmean(F[h][fl])*100:+.2f}%" for h in HORIZONS))

    print("\n" + "=" * 78)
    print("[B] 日级平均的 episode 贡献分解（q=10%, 250日）")
    print("=" * 78)
    q = 0.10
    fl = (pct.to_numpy() <= q) & elig
    eps = episodes_from_flags(dates, fl, gap=60)
    tot = np.nanmean(F[250][fl])
    rows = []
    for (s, e, c) in eps:
        m = np.zeros(len(dates), bool)
        m[s:e + 1] = fl[s:e + 1]
        v = F[250][m]
        rows.append(dict(start=int(dates[s]), end=int(dates[e]), ndays=int(m.sum()),
                         mean250=np.nanmean(v), w=int(m.sum()) / int(fl.sum())))
    df = pd.DataFrame(rows)
    df["贡献pp"] = df["mean250"] * df["w"] * 100
    print(df.to_string(index=False, float_format=lambda v: f"{v:+.4f}"))
    print(f"加权合计 = {df['贡献pp'].sum():+.2f}%  (直接算 {tot*100:+.2f}%)")

    print("\n[B2] 去掉贡献最大的单个 episode 后的日级读数：")
    for drop in range(len(eps)):
        m = fl.copy()
        s, e, c = eps[drop]
        m[s:e + 1] = False
        print(f"  去掉 {int(dates[s])}~{int(dates[e])}: "
              f"剩 {int(m.sum())} 日, 250d = {np.nanmean(F[250][m])*100:+.2f}% "
              f"(基准 {allbase[250]*100:+.2f}%)")

    print("\n" + "=" * 78)
    print("[C] 三种 episode 级估计量（每 episode 权重相同）")
    print("=" * 78)
    for q in QS:
        fl = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, fl, gap=60)
        first, emean, elast = [], [], []
        for (s, e, c) in eps:
            m = np.zeros(len(dates), bool)
            m[s:e + 1] = fl[s:e + 1]
            idx = np.where(m)[0]
            first.append(F[250][idx[0]])
            emean.append(np.nanmean(F[250][idx]))
            elast.append(F[250][idx[-1]])
        b = np.nanmean(F[250][elig])

        def rep(v, name):
            v = np.array(v, float)
            ok = np.isfinite(v)
            x = v[ok] - b
            return (f"{name}: 均值 {x.mean()*100:+6.2f}pp  中位 "
                    f"{np.median(x)*100:+6.2f}pp  正比例 {(x>0).mean():.0%}  n={ok.sum()}")
        print(f"\nq={q:.0%} episode={len(eps)}  (基准 {b*100:+.2f}%)")
        print("  " + rep(first, "首触发日 "))
        print("  " + rep(emean, "episode均"))
        print("  " + rep(elast, "末触发日 "))


if __name__ == "__main__":
    main()
