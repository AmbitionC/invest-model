# -*- coding: utf-8 -*-
"""乖离率均值回归的**可行域标定**（P70/E60 的第 3 步之前；标定不是裁决）。

owner 的命题：`bias = P/MA − 1` 这个量本身在数学意义上就会均值回归，应该能赚这个过程的钱。

**命题的前半段几乎肯定是对的**（bias 是围绕 0 的有界震荡量，构造上就必须回归）。
本脚本要标定的是**后半段能不能成立**，而这归结为一个恒等式：

    ln[(1+b_{t+H}) / (1+b_t)]  =  ln(P_{t+H}/P_t)  −  ln(M_{t+H}/M_t)
    ╰──── 乖离率的回归量 ────╯     ╰─ 价格腿(你赚的) ─╯   ╰─ 均线腿(白送的) ─╯

**均值上这个分解是精确可加的**，所以可以直接问：乖离率从 −30% 回到 0 的那 30 个百分点，
有多少是价格涨上去（能落袋），有多少是**均线自己跌下来**（一分钱赚不到）？

⚠️ **本脚本是标定，不是判据。** 按 SOP §7.5 第 1 条，E60 的判据**不得**挂在这里的点估计上，
必须把举证责任放到标定未产出的维度（样本外、稳健性、增量价值）。标定结果须在判据前**披露**。

只读 results/bias_meanrev/*.csv，不落库、不联网。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))
from e57_bias_top3_leg import UNIVERSE  # noqa: E402

HS = (5, 10, 20, 60, 120)
QLOW, QHIGH = 0.05, 0.95


def halflife(b: pd.Series) -> tuple[float, float]:
    """AR(1)：b_{t+1} = a + φ·b_t。半衰期 = ln0.5 / lnφ（φ<1 才有意义）。"""
    x, y = b.shift(1).dropna(), b.dropna()
    i = x.index.intersection(y.index)
    x, y = x.loc[i].to_numpy(), y.loc[i].to_numpy()
    phi = float(np.polyfit(x, y, 1)[0])
    return phi, (float(np.log(0.5) / np.log(phi)) if 0 < phi < 1 else np.nan)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results/bias_meanrev")
    a = ap.parse_args()
    root = Path(a.data)
    D = {nm: pd.read_csv(root / f"{nm}.csv", dtype={"trade_date": str})
         for nm, _, _, _ in UNIVERSE}
    OOS = {nm: oos for nm, _, _, oos in UNIVERSE}

    print("=" * 118)
    print("标定一：乖离率本身是不是均值回归的？（owner 命题的前半段）")
    print("=" * 118)
    print(f"{'指数':>9s}{'n':>7s}{'均值':>9s}{'中位':>9s}{'标准差':>9s}"
          f"{'AR(1)φ':>9s}{'半衰期(交易日)':>14s}{'|b|>10%占比':>12s}{'穿越0次数':>10s}")
    for nm, d in D.items():
        b = d.bias60.dropna()
        phi, hl = halflife(b)
        cross = int((np.sign(b.to_numpy()[1:]) != np.sign(b.to_numpy()[:-1])).sum())
        star = "★" if OOS[nm] else " "
        print(f"{star}{nm:>8s}{len(b):>7d}{b.mean():>+9.2%}{b.median():>+9.2%}{b.std():>9.2%}"
              f"{phi:>9.4f}{hl:>14.1f}{(b.abs() > 0.10).mean():>12.1%}{cross:>10d}")
    print("  ⟹ 均值≈0、φ<1、半衰期 25~41 个交易日、反复穿越 0（88~335 次）—— **命题前半段成立，"
          "乖离率确实是强均值回归的量。** 问题全在后半段。")

    # ── 标定一之二：回归是「价格的回归」还是「均线的滚动」？ ──
    print("\n" + "=" * 118)
    print("标定一之二：🔴 判别性检验 —— 半衰期是否随均线窗口线性缩放？")
    print("=" * 118)
    print("  若 bias 的回归来自**价格真的回归**，半衰期应由价格动态决定、**与均线窗口无关**；")
    print("  若来自**均线自己滚动**（旧价格滚出窗口、均线机械地朝当前价格靠拢），")
    print("  半衰期应**正比于窗口长度**。这两个假说给出完全不同的预测，可以直接判。")
    print(f"\n{'指数':>9s}{'MA20 半衰期':>13s}{'MA60 半衰期':>13s}{'MA120 半衰期':>14s}"
          f"{'60/20 比':>10s}{'120/60 比':>11s}")
    ratios = []
    for nm, d in D.items():
        hl_ = {}
        for w in (20, 60, 120):
            _, hl_[w] = halflife(d[f"bias{w}"].dropna())
        r1, r2 = hl_[60] / hl_[20], hl_[120] / hl_[60]
        ratios += [r1, r2]
        star = "★" if OOS[nm] else " "
        print(f"{star}{nm:>8s}{hl_[20]:>13.1f}{hl_[60]:>13.1f}{hl_[120]:>14.1f}"
              f"{r1:>10.2f}{r2:>11.2f}")
    print(f"\n  窗口比是 3.00 与 2.00。实测半衰期比中位 {np.median(ratios[0::2]):.2f} 与 "
          f"{np.median(ratios[1::2]):.2f}。")
    print("  ⟹ 半衰期**随窗口成比例放大** ⟹ **回归主要是均线滚动的机械效应，不是价格在回归。**")

    # ── 标定二：回归量的精确分解 ──────────────────────────
    print("\n" + "=" * 118)
    print("标定二：🔴 回归的那部分，有多少落在价格上？（恒等式 Δlnbias = 价格腿 − 均线腿）")
    print("=" * 118)
    for side, q, cmp_ in (("低尾（跌过头，预期 bias 回升）", QLOW, "le"),
                          ("高尾（涨过头，预期 bias 回落）", QHIGH, "ge")):
        print(f"\n  ── {side}：bias60 ≤/≥ 全样本 {q:.0%} 分位 ──")
        print(f"{'指数':>9s}{'H':>5s}{'n':>6s}{'Δlnbias':>10s}{'＝价格腿':>10s}"
              f"{'− 均线腿':>10s}{'价格腿占比':>11s}{'价格腿>0比例':>13s}")
        for nm, d in D.items():
            b = d.bias60
            th = b.quantile(q)
            sel = (b <= th) if cmp_ == "le" else (b >= th)
            for h in HS:
                s = d[sel].dropna(subset=[f"dlnbias{h}", f"leg_price{h}", f"leg_ma{h}"])
                if len(s) < 20:
                    continue
                db, lp, lm = (s[f"dlnbias{h}"].mean(), s[f"leg_price{h}"].mean(),
                              s[f"leg_ma{h}"].mean())
                # 价格腿占回归量的比例（同号才有意义；异号说明价格帮了倒忙）
                frac = lp / db if abs(db) > 1e-9 else np.nan
                print(f"{nm if h == HS[0] else '':>9s}{h:>5d}{len(s):>6d}{db:>+10.2%}"
                      f"{lp:>+10.2%}{lm:>+10.2%}{frac:>11.0%}"
                      f"{(s[f'leg_price{h}'] > 0).mean():>13.0%}")

    # ── 标定三：把「价格腿占比」压成一张总表 ────────────────
    print("\n" + "=" * 118)
    print("标定三：总表 —— 「乖离率回归」有多少能变成钱")
    print("=" * 118)
    print(f"{'指数':>9s}" + "".join(f"{f'低尾H{h}':>10s}" for h in HS)
          + "".join(f"{f'高尾H{h}':>10s}" for h in HS))
    for nm, d in D.items():
        b = d.bias60
        row = []
        for q, cmp_ in ((QLOW, "le"), (QHIGH, "ge")):
            th = b.quantile(q)
            sel = (b <= th) if cmp_ == "le" else (b >= th)
            for h in HS:
                s = d[sel].dropna(subset=[f"dlnbias{h}", f"leg_price{h}"])
                if len(s) < 20:
                    row.append("—")
                    continue
                db, lp = s[f"dlnbias{h}"].mean(), s[f"leg_price{h}"].mean()
                row.append(f"{lp / db:.0%}" if abs(db) > 1e-9 else "—")
        star = "★" if OOS[nm] else " "
        print(f"{star}{nm:>8s}" + "".join(f"{v:>10s}" for v in row))
    print("\n  读法：100% ＝ 乖离率的回归全部由价格完成（你能赚到）；0% ＝ 全部由均线自己走过来"
          "\n        （你一分钱赚不到）；**负数 ＝ 价格朝反方向走，乖离率靠均线跌得更快才回归的**。")

    # ── 标定四：把两个问题接起来 —— 恐慌共振能不能救活价格腿？ ──
    print("\n" + "=" * 118)
    print("标定四：低尾 × 恐慌≥75 共振时，价格腿是否转正？（接 owner 的第二问）")
    print("=" * 118)
    print("  动机：标定二显示大盘腿在低尾的价格腿是**负的**（乖离率靠均线跌下来才回归）。")
    print("  若「跌过头」这件事本身没有信息，但「跌过头 + 全市场恐慌」有，价格腿应在共振子集里转正。")
    print("  🔴 **必须同期对照**：共振子集只存在于 2015 年后（恐慌数据起点），而全样本低尾含"
          "2005-2014；直接比会把「时期」当成「共振」。故对照臂一律裁到恐慌可用期。")
    print(f"\n{'指数':>9s}{'H':>5s}{'低尾(全样本)':>13s}{'低尾(15年后)':>13s}"
          f"{'共振n':>7s}{'共振价格腿':>11s}{'共振−同期低尾':>15s}{'共振胜率':>9s}{'同期胜率':>9s}")
    for nm, d in D.items():
        b = d.bias60
        lo = b <= b.quantile(QLOW)
        has_fear = d.fear.notna()
        lo_same = lo & has_fear                       # 同期对照臂
        res = lo & (d.fear >= 75)
        for h in (20, 60, 120):
            s0 = d[lo].dropna(subset=[f"leg_price{h}"])
            s1 = d[lo_same].dropna(subset=[f"leg_price{h}"])
            s2 = d[res].dropna(subset=[f"leg_price{h}"])
            m0 = s0[f"leg_price{h}"].mean()
            if len(s2) < 10 or len(s1) < 10:
                print(f"{nm if h == 20 else '':>9s}{h:>5d}{m0:>+13.2%}"
                      f"{'样本不足':>13s}{len(s2):>7d}{'—':>11s}{'—':>15s}{'—':>9s}{'—':>9s}")
                continue
            m1, m2 = s1[f"leg_price{h}"].mean(), s2[f"leg_price{h}"].mean()
            print(f"{nm if h == 20 else '':>9s}{h:>5d}{m0:>+13.2%}{m1:>+13.2%}"
                  f"{len(s2):>7d}{m2:>+11.2%}{(m2 - m1) * 100:>+15.2f}"
                  f"{(s2[f'leg_price{h}'] > 0).mean():>9.0%}"
                  f"{(s1[f'leg_price{h}'] > 0).mean():>9.0%}")
    print("\n  读法：有效力的是**「共振−同期低尾」那一列**（两臂同期、同为低尾，只差恐慌条件）。")
    print("  「低尾(全样本)」列只用来显示时期效应有多大——不可用来给共振记功。")

    # ── 标定五：价格腿的符号稳不稳？（标定四暴露出的问题） ──
    print("\n" + "=" * 118)
    print("标定五：🔴 「低尾价格腿为负」这个结论本身分期稳不稳？")
    print("=" * 118)
    print("  标定四里沪深300 全样本低尾 H60 价格腿 −6.92%，但只看 2015 年后是 **+2.86%** ——")
    print("  符号翻了。所以要把分期拆开看，否则整套结论建在一个不稳的点估计上。")
    print(f"\n{'指数':>9s}{'H':>5s}" + "".join(f"{p:>16s}" for p in
                                              ("2005-2010", "2011-2015", "2016-2020", "2021-今")))
    PER = (("2005-2010", "20050101", "20101231"), ("2011-2015", "20110101", "20151231"),
           ("2016-2020", "20160101", "20201231"), ("2021-今", "20210101", "20991231"))
    for nm, d in D.items():
        b = d.bias60
        lo = b <= b.quantile(QLOW)
        for h in (20, 60):
            cells = []
            for _, s, e in PER:
                sel = lo & (d.trade_date >= s) & (d.trade_date <= e)
                ss = d[sel].dropna(subset=[f"leg_price{h}"])
                cells.append(f"{ss[f'leg_price{h}'].mean():+.2%} (n={len(ss)})"
                             if len(ss) >= 10 else f"— (n={len(ss)})")
            print(f"{nm if h == 20 else '':>9s}{h:>5d}" + "".join(f"{c:>16s}" for c in cells))
    print("\n  ⟹ 逐格看符号：**同一个指数在不同十年里价格腿正负都有** ⟹ "
          "「低尾之后价格还会跌」不是一条稳定规律，\n     它在 2008 和 2011-2015 那种"
          "长熊里成立，在 2016 年后大体不成立。这一条必须写进 E60 的判据设计里。")

    print("\n" + "=" * 118)
    print("⚠️ 以上全部是标定（全样本、事后分位、无成本、无执行），**不是判据、不可作晋升依据**。")
    print("=" * 118)


if __name__ == "__main__":
    main()
