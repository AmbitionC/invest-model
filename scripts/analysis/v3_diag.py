# -*- coding: utf-8 -*-
"""V3 附加诊断（判据没覆盖到的部分）。依赖同目录 v3_independent_verdict 的引擎。

1. 弹药落点的第三种口径：按窗口价格分位分桶
2. 底仓 sleeve 的首笔买入日期（命题C 的 n=1 择时禀赋）
3. 阶梯腿去掉的同起点公平对照
4. 曲线切半（不重置资金）——与"重跑两半"互补的第二种分半读数
5. 红利腿网格 vs 月频：分块 bootstrap 显著性
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v3_independent_verdict import (LEGS, Cfg, buy_hold, first_tradable_idx, load_legs,
                                    simulate, POT, RF)

legs = load_legs()
BAR = "=" * 100


def ann_of(curve, dates):
    yrs = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days / 365.25
    return (curve[-1] / curve[0]) ** (1 / yrs) - 1


print(BAR)
print("1. 弹药落点：按窗口价格分位分桶的买入金额占比（不依赖任何'最低价'定义）")
print(BAR)
for arm, kw in (("cur", {}), ("init", dict(size="init")), ("ramp", dict(size="ramp")),
                ("cd0", dict(cooldown=0)), ("cd40", dict(cooldown=40))):
    for nm in LEGS:
        r = simulate(legs[nm], Cfg(sell_prod=True, **kw))
        b = r["band"]
        print(f"{arm:<6}{nm:<8}" + " ".join(f"{k}={v*100:5.1f}%" for k, v in b.items()))
    print()

print(BAR)
print("2. 底仓 sleeve 首笔买入日 = 首个买点（命题C 的择时禀赋是 n=1 的一次抽样）")
print(BAR)
for nm in LEGS:
    leg = legs[nm]
    r = simulate(leg, Cfg(sell_prod=True), keep_detail=True)
    i0 = first_tradable_idx(leg, Cfg(sell_prod=True))
    if r["buys"]:
        # 首个买点 = 第一笔成交（底仓在同一信号日/次日入场）
        n_at = len([1 for p, a, t in r["buys"]])
        first_px = r["buys"][0][0]
        win = leg.px[i0:]
        pct = float((win < first_px).mean())
        print(f"{nm:<8} 起点 {leg.dates[i0]} 首个买点价 {first_px:9.2f} "
              f"= 全窗价格分位 {pct*100:5.1f}%  （窗口低 {win.min():9.2f} 高 {win.max():9.2f}）"
              f" 全窗买笔 {n_at}")

print()
print(BAR)
print("3. 阶梯腿去掉（科创50）：同起点公平对照")
print(BAR)
leg = legs["科创50"]
for lab, cfg in (("现状(ladder, 起点=数据首日)", Cfg(sell_prod=True)),
                 ("去阶梯(B1, 起点=数据首日)", Cfg(sell_prod=True, ladder_off=True))):
    r = simulate(leg, cfg)
    print(f"{lab:<32} {r['d0']}~{r['d1']} 年化{r['ann']*100:6.2f}% 夏普{r['sharpe']:5.2f} "
          f"回撤{r['mdd']*100:6.1f}% 买{r['nb']:>3d} 均仓{r['pos']*100:3.0f}%")
i_warm = 500
for lab, cfg in (("现状(ladder, 起点=锚预热完成)", Cfg(sell_prod=True)),
                 ("去阶梯(B1, 起点=锚预热完成)", Cfg(sell_prod=True, ladder_off=True))):
    r = simulate(leg, cfg, i_warm, len(leg.px))
    print(f"{lab:<32} {r['d0']}~{r['d1']} 年化{r['ann']*100:6.2f}% 夏普{r['sharpe']:5.2f} "
          f"回撤{r['mdd']*100:6.1f}% 买{r['nb']:>3d} 均仓{r['pos']*100:3.0f}%")
r = buy_hold(leg, Cfg(sell_prod=True))
print(f"{'买入持有(起点=数据首日)':<30} 年化{r['ann']*100:6.2f}% 回撤{r['mdd']*100:6.1f}%")

print()
print(BAR)
print("4. 曲线切半（不重置资金）：全样本单跑一次，按中点切曲线算两段年化差 pp")
print(BAR)


def curve_halves(nm, cfg_arm, cfg_base):
    leg = legs[nm]
    i0 = first_tradable_idx(leg, cfg_base)
    i1 = len(leg.px)
    mid = i0 + (i1 - i0) // 2
    ra = simulate(leg, cfg_arm, keep_detail=True)
    rb = simulate(leg, cfg_base, keep_detail=True)
    out = []
    for lo, hi, lab in ((0, mid - i0, "上半"), (mid - i0, i1 - i0 - 1, "下半")):
        da = leg.dates[i0 + lo:i0 + hi + 1]
        a = ann_of(ra["curve"][lo:hi + 1], da)
        b = ann_of(rb["curve"][lo:hi + 1], da)
        out.append((lab, (a - b) * 100))
    return out


CASES = [
    ("B/init", Cfg(sell_prod=True, size="init"), Cfg(sell_prod=True)),
    ("B/ramp", Cfg(sell_prod=True, size="ramp"), Cfg(sell_prod=True)),
    ("B/cd0", Cfg(sell_prod=True, cooldown=0), Cfg(sell_prod=True)),
    ("C/base25", Cfg(sell_prod=True, base=.25), Cfg(sell_prod=True)),
    ("C/base100", Cfg(sell_prod=True, base=1.0), Cfg(sell_prod=True)),
    ("D/g2", Cfg(sell_prod=True, sell_mode="grid", grid_g=.02), Cfg(sell_prod=True)),
    ("D/g3.5", Cfg(sell_prod=True, sell_mode="grid", grid_g=.035), Cfg(sell_prod=True)),
    ("D/g5", Cfg(sell_prod=True, sell_mode="grid", grid_g=.05), Cfg(sell_prod=True)),
    ("D/g8", Cfg(sell_prod=True, sell_mode="grid", grid_g=.08), Cfg(sell_prod=True)),
]
for lab, ca, cb in CASES:
    s = f"{lab:<11}"
    for nm in LEGS:
        hs = curve_halves(nm, ca, cb)
        s += f"| {nm} " + " ".join(f"{k}{v:+6.2f}" for k, v in hs) + " "
    print(s)

print()
print(BAR)
print("5. 红利腿 网格 vs 月频：日收益差的分块 bootstrap（块长 250 交易日，2000 次）")
print(BAR)
leg = legs["红利"]
base = simulate(leg, Cfg(sell_prod=True), keep_detail=True)
for g in (.02, .035, .05, .08):
    arm = simulate(leg, Cfg(sell_prod=True, sell_mode="grid", grid_g=g), keep_detail=True)
    ra = np.diff(np.log(arm["curve"]))
    rb = np.diff(np.log(base["curve"]))
    d = ra - rb
    n, L = len(d), 250
    rng = np.random.default_rng(20260805)
    nb = n // L + 1
    sims = []
    for _ in range(2000):
        st = rng.integers(0, n - L, nb)
        x = np.concatenate([d[s:s + L] for s in st])[:n]
        sims.append(x.mean() * 250)
    sims = np.array(sims)
    obs = d.mean() * 250
    print(f"g={g:<5} 观测年化对数差 {obs*100:+5.2f}pp  "
          f"bootstrap 均值 {sims.mean()*100:+5.2f}pp  "
          f"95%CI [{np.percentile(sims,2.5)*100:+5.2f},{np.percentile(sims,97.5)*100:+5.2f}]  "
          f"p(≤0)={np.mean(sims<=0):.3f}")

print()
print(BAR)
print("6. 红利腿：不卖 / 月频 / 网格 三方对照（判断 E54-b 的收益是不是单纯'少卖'）")
print(BAR)
for lab, cfg in (("不卖", Cfg(sell_prod=True, sell_mode="none")),
                 ("月频5%", Cfg(sell_prod=True)),
                 ("网格2%", Cfg(sell_prod=True, sell_mode="grid", grid_g=.02)),
                 ("网格3.5%", Cfg(sell_prod=True, sell_mode="grid", grid_g=.035)),
                 ("网格5%", Cfg(sell_prod=True, sell_mode="grid", grid_g=.05)),
                 ("网格8%", Cfg(sell_prod=True, sell_mode="grid", grid_g=.08))):
    r = simulate(legs["红利"], cfg)
    print(f"{lab:<9} 年化{r['ann']*100:6.2f}% 夏普{r['sharpe']:5.2f} 回撤{r['mdd']*100:6.1f}% "
          f"卖{r['ns']:>3d} 均仓{r['pos']*100:3.0f}% 亏损年{r['nloss']}/{r['nyr']}")
print("同法对沪深300：")
for lab, cfg in (("不卖", Cfg(sell_prod=True, sell_mode="none")),
                 ("月频5%", Cfg(sell_prod=True)),
                 ("网格2%", Cfg(sell_prod=True, sell_mode="grid", grid_g=.02))):
    r = simulate(legs["沪深300"], cfg)
    print(f"{lab:<9} 年化{r['ann']*100:6.2f}% 夏普{r['sharpe']:5.2f} 回撤{r['mdd']*100:6.1f}% "
          f"卖{r['ns']:>3d} 均仓{r['pos']*100:3.0f}%")
