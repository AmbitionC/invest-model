"""E53 命题C（底仓 sleeve）——只做统计侧：这个结论有多强？

不重写回测。用最小引擎复现 §2.3 的表，然后回答三个问题：
  Q1 19.5 年沪深300 里有几个不重叠的独立周期？
  Q2 「亏损年 8/20 → 9/20」这 1 年的差别在统计上有意义吗？（配对 McNemar）
  Q3 「年化随底仓单调下降」的效应量有多大、置信区间多宽？（配对分块 bootstrap）
最后给出结论强度分级。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from math import comb

from v4_common import load_fear, load_legs, to_dt
from v4_engine import run_leg, show

RNG = np.random.default_rng(20260805)
FRACS = [0.0, 0.25, 0.50, 0.75, 1.00]


def stationary_bootstrap_mean(x, B=5000, mean_block=250, rng=RNG):
    """平稳分块 bootstrap（几何块长），返回均值的抽样分布。"""
    n = len(x)
    p = 1.0 / mean_block
    out = np.empty(B)
    for b in range(B):
        idx = np.empty(n, dtype=int)
        i = rng.integers(n)
        for t in range(n):
            idx[t] = i
            if rng.random() < p:
                i = rng.integers(n)
            else:
                i = (i + 1) % n
        out[b] = x[idx].mean()
    return out


def zigzag_cycles(p, drop=0.20, rise=0.20):
    """峰谷交替计数：一个完整周期＝一个峰到下一个峰。"""
    peaks, troughs = [], []
    mode, ext, ext_i = "peak", p[0], 0
    for i in range(1, len(p)):
        if mode == "peak":
            if p[i] > ext:
                ext, ext_i = p[i], i
            elif p[i] / ext - 1 <= -drop:
                peaks.append(ext_i)
                mode, ext, ext_i = "trough", p[i], i
        else:
            if p[i] < ext:
                ext, ext_i = p[i], i
            elif p[i] / ext - 1 >= rise:
                troughs.append(ext_i)
                mode, ext, ext_i = "peak", p[i], i
    return peaks, troughs


def main():
    legs = load_legs()
    fear = load_fear()

    print("=" * 92)
    print("[0] 复现 §2.3 底仓表（本轮基线口径 ×1.00 生产闸；同时给 ×1.30 对照）")
    print("=" * 92)
    res = {}
    for S in [None, 1.30]:
        tag = "×1.00" if S is None else "×1.30"
        print(f"\n### 卖出闸 {tag}")
        print(f"{'底仓':<8}" + "".join(f"{n:>34}" for n in legs))
        for f in FRACS:
            rows = []
            for name, cfg in legs.items():
                on = ["B2", "B3"] if cfg["kind"] == "ladder" else ["B1", "B2"]
                r = run_leg(name, cfg, fear, S_mult=S, legs_on=on, base_frac=f)
                res[(tag, f, name)] = r
                rows.append(f"{r['ann']*100:>7.2f}%/{r['sharpe']:.2f}/"
                            f"{r['mdd']*100:.1f}%/{r['pos']*100:.0f}%/{r['loss']}")
            print(f"{f:<8.0%}" + "".join(f"{x:>34}" for x in rows))

    # ---------------- Q1 独立周期数 ----------------
    print("\n" + "=" * 92)
    print("[Q1] 沪深300 19.5 年里有几个不重叠的独立周期？")
    print("=" * 92)
    hs = legs["沪深300"]["px"]
    d = hs.index.to_numpy()
    m = d >= 20070126
    p = hs.to_numpy()[m]
    dd = d[m]
    for drop in [0.20, 0.30, 0.40]:
        pk, tr = zigzag_cycles(p, drop, drop)
        print(f"  ZigZag ±{drop:.0%}: 峰 {len(pk)} 个 {[int(dd[i]) for i in pk]}")
        print(f"{'':14}谷 {len(tr)} 个 {[int(dd[i]) for i in tr]}"
              f"  ⟹ 完整峰-峰周期 {max(0,len(pk)-1)} 个")
    r0 = res[("×1.00", 0.0, "沪深300")]
    yr = r0["yr_ret"].to_numpy()
    from v4_common import eff_sample_size
    print(f"\n  自然年收益 n={len(yr)}，AR(1) 修正等效样本 "
          f"{eff_sample_size(yr):.1f}")
    lr = np.diff(np.log(r0["nav"]))
    ac = [np.corrcoef(lr[:-k], lr[k:])[0, 1] for k in range(1, 6)]
    print(f"  日对数收益 AR(1..5) = {['%.3f' % x for x in ac]}")
    print(f"  19.5 年 / 单个完整周期长度（峰到峰，±20% 口径）"
          f" ⟹ 不重叠独立周期约 {len(zigzag_cycles(p,0.2,0.2)[0])-1} 个")

    # ---------------- Q2 亏损年数的统计意义 ----------------
    print("\n" + "=" * 92)
    print("[Q2] 「亏损年 8/20 → 9/20」有统计意义吗？（配对 McNemar 精确检验）")
    print("=" * 92)
    for name in legs:
        a = res[("×1.00", 0.0, name)]["yr_ret"]
        for f in [0.25, 1.00]:
            b = res[("×1.00", f, name)]["yr_ret"]
            ai = (a < 0).to_numpy()
            bi = (b.reindex(a.index) < 0).to_numpy()
            b01 = int((~ai & bi).sum())   # 现状不亏 → 底仓亏
            b10 = int((ai & ~bi).sum())   # 现状亏 → 底仓不亏
            n = b01 + b10
            if n == 0:
                pv = 1.0
            else:
                k = max(b01, b10)
                pv = sum(comb(n, i) for i in range(k, n + 1)) / 2 ** (n - 1)
                pv = min(1.0, pv)
            print(f"  {name:<8} 底仓{f:.0%}: 亏损年 {int(ai.sum())}/{len(ai)} → "
                  f"{int(bi.sum())}/{len(bi)} | 不一致年 b01={b01} b10={b10} | "
                  f"McNemar 精确双侧 p={pv:.3f}")
    print("\n  说明：n 个不一致年里全部同向时的最小可能 p = 2^-(n-1)。")
    for n in range(1, 7):
        print(f"    不一致年 n={n} ⟹ 最小可达 p={2**-(n-1):.3f}"
              f"{'（永远达不到 0.05）' if 2**-(n-1) > 0.05 else ''}")

    # ---------------- Q3 年化差的效应量与置信区间 ----------------
    print("\n" + "=" * 92)
    print("[Q3] Δ年化（底仓 X% − 现状）的配对分块 bootstrap 95% CI")
    print("=" * 92)
    print(f"{'腿':<8}{'底仓':<7}{'Δ年化pp':>10}{'95%CI':>22}{'p(单侧:恶化)':>14}"
          f"{'块长敏感':>22}")
    for name in legs:
        n0 = res[("×1.00", 0.0, name)]["nav"]
        for f in [0.25, 1.00]:
            n1 = res[("×1.00", f, name)]["nav"]
            dl = np.diff(np.log(n1)) - np.diff(np.log(n0))
            obs = (np.exp(dl.mean() * 250) - 1) * 100
            cis = []
            for mb in [125, 250, 500]:
                bs = stationary_bootstrap_mean(dl, B=2000, mean_block=mb)
                lo, hi = np.percentile((np.exp(bs * 250) - 1) * 100, [2.5, 97.5])
                cis.append((lo, hi))
            bs = stationary_bootstrap_mean(dl, B=4000, mean_block=250)
            pv = float(((np.exp(bs * 250) - 1) * 100 >= 0).mean())
            lo, hi = cis[1]
            print(f"{name:<8}{f:<7.0%}{obs:>+10.2f}"
                  f"{f'[{lo:+.2f},{hi:+.2f}]':>22}{pv:>14.3f}"
                  f"{f'[{cis[0][0]:+.1f},{cis[0][1]:+.1f}]/[{cis[2][0]:+.1f},{cis[2][1]:+.1f}]':>22}")

    # ---------------- Q4 起点敏感性 ----------------
    print("\n" + "=" * 92)
    print("[Q4] 月度滚动起点：Δ年化(25%底仓 − 现状) 的同号比例（判据6 的口径）")
    print("=" * 92)
    for name in legs:
        n0 = res[("×1.00", 0.0, name)]
        n1 = res[("×1.00", 0.25, name)]
        dts = n0["dates"]
        me = np.where((dts // 100)[:-1] != (dts // 100)[1:])[0]
        signs = []
        for i in me:
            rest = len(dts) - i
            if rest < 750:
                continue
            yrs = rest / 250.0
            a0 = (n0["nav"][-1] / n0["nav"][i]) ** (1 / yrs) - 1
            a1 = (n1["nav"][-1] / n1["nav"][i]) ** (1 / yrs) - 1
            signs.append(a1 - a0)
        signs = np.array(signs)
        print(f"  {name:<8} n={len(signs)} 起点 | 为负（底仓更差）比例 "
              f"{(signs<0).mean()*100:>5.1f}% | Δ 中位 {np.median(signs)*100:+.2f}pp"
              f" | 范围 [{signs.min()*100:+.2f},{signs.max()*100:+.2f}]pp")

    # ---------------- Q5 均仓与回撤的机械关系 ----------------
    print("\n" + "=" * 92)
    print("[Q5] 底仓效应的机制核对：Δ回撤 是否可由 Δ均仓 机械解释")
    print("=" * 92)
    for name in legs:
        r0 = res[("×1.00", 0.0, name)]
        xs, ys = [], []
        for f in FRACS:
            r = res[("×1.00", f, name)]
            xs.append(r["pos"])
            ys.append(abs(r["mdd"]))
        cc = np.corrcoef(xs, ys)[0, 1]
        print(f"  {name:<8} 均仓 {['%.0f%%'%(x*100) for x in xs]} | "
              f"|回撤| {['%.1f%%'%(y*100) for y in ys]} | ρ={cc:+.3f}")


if __name__ == "__main__":
    main()
