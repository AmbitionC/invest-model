"""E55 主推断：以「episode 均值」为估计量（B4 是周频买入、贯穿整个 episode，
故 episode 内等权是唯一与策略行为一致的事件级估计量）。

做三件事：
  1. 循环移位置换检验（保留信号的聚集结构 + 收益序列自相关）
  2. episode 级 bootstrap CI（n=4~6，故意暴露它有多宽）
  3. 信号精度：触发日里有多少落在真实主要底部附近
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v4_common import episodes_from_flags, load_amount, load_legs, rolling_pct

QS = [0.05, 0.10, 0.20, 0.30]
RNG = np.random.default_rng(20260805)


def fwd(px, dates, h, lag=1):
    p = px.reindex(dates).ffill().to_numpy(float)
    n = len(p)
    out = np.full(n, np.nan)
    for i in range(n):
        if i + lag + h < n:
            out[i] = p[i + lag + h] / p[i + lag] - 1.0
    return out


def zigzag_bottoms(p, drop=0.20, rise=0.20):
    bots, mode, ext, ei = [], "peak", p[0], 0
    for i in range(1, len(p)):
        if mode == "peak":
            if p[i] > ext:
                ext, ei = p[i], i
            elif p[i] / ext - 1 <= -drop:
                mode, ext, ei = "trough", p[i], i
        else:
            if p[i] < ext:
                ext, ei = p[i], i
            elif p[i] / ext - 1 >= rise:
                bots.append(ei)
                mode, ext, ei = "peak", p[i], i
    return bots


def main():
    hs = load_legs()["沪深300"]["px"]
    amt = load_amount()
    pct = rolling_pct(amt)
    dates = amt.index.to_numpy()
    elig = pct.notna().to_numpy()
    F250 = fwd(hs, dates, 250)
    F60 = fwd(hs, dates, 60)
    base250 = np.nanmean(F250[elig])
    base60 = np.nanmean(F60[elig])

    print("=" * 84)
    print("[1] episode 均值估计量：置换检验 + bootstrap CI")
    print("=" * 84)
    print(f"基准（分位可用区间日级均值）：60d {base60*100:+.2f}%  "
          f"250d {base250*100:+.2f}%")
    for q in QS:
        fl = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, fl, gap=60)
        masks = []
        for (s, e, c) in eps:
            m = np.zeros(len(dates), bool)
            m[s:e + 1] = fl[s:e + 1]
            masks.append(np.where(m)[0])
        for F, b, h in [(F60, base60, 60), (F250, base250, 250)]:
            vals = np.array([np.nanmean(F[ix]) for ix in masks])
            ok = np.isfinite(vals)
            obs = vals[ok].mean() - b
            # 循环移位置换：整体平移信号，保留 episode 内部结构
            n = len(dates)
            null = []
            for _ in range(5000):
                sh = RNG.integers(1, n)
                v = []
                for ix in masks:
                    j = (ix + sh) % n
                    vv = F[j]
                    if np.isfinite(vv).any():
                        v.append(np.nanmean(vv))
                if len(v) >= max(2, len(masks) // 2):
                    null.append(np.mean(v) - b)
            null = np.array(null)
            p = float((null >= obs).mean())
            vv = vals[ok]
            bs = np.array([RNG.choice(vv, len(vv), replace=True).mean()
                           for _ in range(5000)]) - b
            print(f"q={q:.0%} h={h:3d}: episode {len(eps)} 个 | 观测 {obs*100:+6.2f}pp"
                  f" | 正比例 {(vv - b > 0).mean():.0%} | 置换 p={p:.3f}"
                  f" | bootstrap 95%CI [{np.percentile(bs,2.5)*100:+.1f},"
                  f"{np.percentile(bs,97.5)*100:+.1f}]pp")

    print("\n" + "=" * 84)
    print("[2] 三种估计量的分歧（同一批 episode，只换『在 episode 里的哪天算收益』）")
    print("=" * 84)
    print("  含义：首日=按信号一出现就判断；episode均=按周频买满整段；末日=事后才知道的最优点")
    for q in QS:
        fl = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, fl, gap=60)
        ixs = []
        for (s, e, c) in eps:
            m = np.zeros(len(dates), bool)
            m[s:e + 1] = fl[s:e + 1]
            ixs.append(np.where(m)[0])
        f_ = np.nanmean([F250[i[0]] for i in ixs]) - base250
        m_ = np.nanmean([np.nanmean(F250[i]) for i in ixs]) - base250
        l_ = np.nanmean([F250[i[-1]] for i in ixs]) - base250
        print(f"q={q:.0%}: 首日 {f_*100:+6.2f}pp → episode均 {m_*100:+6.2f}pp "
              f"→ 末日 {l_*100:+6.2f}pp   （递增 ⟹ 收益集中在 episode 尾部）")

    print("\n" + "=" * 84)
    print("[3] 信号精度：触发日中有多少落在真实主要底部 ±20 交易日内")
    print("=" * 84)
    p_al = hs.reindex(dates).ffill().to_numpy()
    for drop in [0.20, 0.15]:
        bots = zigzag_bottoms(p_al, drop, drop)
        near = np.zeros(len(dates), bool)
        for b in bots:
            near[max(0, b - 20):min(len(dates), b + 21)] = True
        print(f"\n  ZigZag ±{drop:.0%}: {len(bots)} 个主要底 "
              f"{[int(dates[b]) for b in bots]}")
        print(f"  「底部窗口」共 {int(near.sum())} 天（占样本 {near.mean():.1%}）")
        for q in QS:
            fl = (pct.to_numpy() <= q) & elig
            prec = (fl & near).sum() / max(1, fl.sum())
            rec = (fl & near).sum() / max(1, near.sum())
            print(f"    q={q:.0%}: 触发 {int(fl.sum())} 天 | 精度(命中底部窗口) "
                  f"{prec:.1%} | 召回 {rec:.1%} | 相对基线提升 "
                  f"{prec/near.mean():.2f}x")

    print("\n" + "=" * 84)
    print("[4] 事件独立性：episode 起始日两两间隔（交易日）")
    print("=" * 84)
    for q in QS:
        fl = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, fl, gap=60)
        st = [s for (s, e, c) in eps]
        gaps = np.diff(st)
        print(f"q={q:.0%}: 起始日 {[int(dates[s]) for s in st]}")
        print(f"{'':8}间隔 {list(gaps)} 交易日"
              f"（<250 者视为同一宏观周期内：{int((gaps<250).sum())} 对）")


if __name__ == "__main__":
    main()
