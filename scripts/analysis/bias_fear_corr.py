# -*- coding: utf-8 -*-
"""乖离率极值 × 恐慌极值 的相关性（owner 2026-08-06 追问的第二件事）。

## 🔴 先说结论成立的前提：恐慌分**不是**独立于乖离率的信息源

`invest_model/signals/fear.py` 的恐慌分是 5 个等权分量的均值，其中：

  ① 动量   = 沪深300 距 **MA125** 的乖离率        ← **本身就是一个乖离率**
  ② 波动率 = 沪深300 **20 日已实现波动**          ← 同一条价格序列的函数
  ③ 宽度   = 全市场站上 MA20 的占比                ┐
  ④ 涨跌停 = 跌停/(涨停+跌停)                      ├ 全市场**横截面**，与指数价格不同源
  ⑤ 新高新低 = 120 日新低 vs 新高家数              ┘

⟹ 直接拿 bias60 和恐慌合成分求相关，**其中 2/5 的相关是定义带来的、不是市场告诉你的**；
对沪深300 尤其严重（分量①②用的就是它自己）。所以本脚本把恐慌分**拆开**再算：

    合成分 = (f_mom + f_vol + f_breadth + f_limit + f_hl) / 5

f_mom / f_vol 可由沪深300 收盘价**精确重建**（映射公式见 fear.py `_lin`），于是横截面三项的均值
可以从合成分**反解**：

    f_cross = (5 × score − f_mom − f_vol) / 3

`f_cross` 落在 [0,100] 内的比例是这套重建的**自检**——若重建错了，反解值会大面积越界。
**「乖离率极值 vs 恐慌极值」的真实相关性，要看 bias60 与 f_cross 的关系，不是与合成分的关系。**

样本：恐慌数据 2015-01 起（此前无回填）⟹ 2007/2008 两轮极值**不在样本内**，须据此读数。
只读 results/*.csv，不落库、不联网。
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

FEAR_HI, FEAR_XHI, FEAR_GREED = 75.0, 85.0, 25.0     # 系统既有分档
TAILS = (0.05, 0.02)                                  # 极值定义：分位
LAGS = range(-20, 21)


class stats:  # noqa: N801 —— 环境无 scipy，自带最小实现（秩相关＝秩上的 Pearson）
    class _R:
        def __init__(self, v):
            self.statistic = v

    @staticmethod
    def spearmanr(x, y):
        x = pd.Series(np.asarray(x, dtype=float))
        y = pd.Series(np.asarray(y, dtype=float))
        m = x.notna() & y.notna()
        if m.sum() < 3:
            return stats._R(np.nan)
        rx, ry = x[m].rank(), y[m].rank()
        return stats._R(float(np.corrcoef(rx, ry)[0, 1]))


def _lin(x, lo, hi):
    return np.clip((x - lo) / (hi - lo) * 100.0, 0.0, 100.0)


def rebuild_components(hs300: pd.DataFrame) -> pd.DataFrame:
    """从沪深300 收盘价重建恐慌分量①动量、②波动率（公式逐字照抄 fear.py）。"""
    c = hs300.close
    dev = c / c.rolling(125).mean() - 1.0
    vol = c.pct_change().rolling(20).std() * np.sqrt(250)
    return pd.DataFrame({
        "trade_date": hs300.trade_date,
        "f_mom": _lin(-dev, -0.10, 0.15),
        "f_vol": _lin(vol, 0.15, 0.35),
    })


def overlap(a: np.ndarray, b: np.ndarray) -> dict:
    """两个布尔事件集合的重合度。"""
    na, nb = int(a.sum()), int(b.sum())
    both = int((a & b).sum())
    return {"na": na, "nb": nb, "both": both,
            "p_b_given_a": both / na if na else np.nan,
            "p_a_given_b": both / nb if nb else np.nan,
            "jaccard": both / int((a | b).sum()) if (a | b).any() else np.nan}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results/bias_meanrev")
    a = ap.parse_args()
    root = Path(a.data)

    hs = pd.read_csv(root / "沪深300.csv", dtype={"trade_date": str})
    comp = rebuild_components(hs)

    print("=" * 112)
    print("乖离率极值 × 恐慌极值｜先把恐慌分拆成「价格派生」与「全市场横截面」两半")
    print("=" * 112)

    # ── 重建自检 ──────────────────────────────────────────
    m = hs[["trade_date", "fear"]].merge(comp, on="trade_date").dropna()
    m["f_cross"] = (5 * m.fear - m.f_mom - m.f_vol) / 3
    inrange = float(((m.f_cross >= -1) & (m.f_cross <= 101)).mean())
    print(f"\n【重建自检】反解出的横截面三项均值 f_cross 落在 [0,100] 的比例：{inrange:.1%}"
          f"（n={len(m)}）")
    print(f"  f_cross 分布：min {m.f_cross.min():.1f} / p1 {m.f_cross.quantile(.01):.1f} / "
          f"中位 {m.f_cross.median():.1f} / p99 {m.f_cross.quantile(.99):.1f} / max {m.f_cross.max():.1f}")
    if inrange < 0.95:
        print("  ⚠️ 越界比例偏高 ⟹ 重建公式与实际落库口径可能不一致，下面的拆分只能定性读。")
    else:
        print("  ✅ 重建可信：分量①②的公式与落库口径一致，横截面三项的反解成立。")
    print(f"  合成分 vs 重建的动量分量 Spearman ρ = "
          f"{stats.spearmanr(m.fear, m.f_mom).statistic:+.3f}"
          f"　｜　vs 波动率分量 {stats.spearmanr(m.fear, m.f_vol).statistic:+.3f}"
          f"　｜　vs 横截面三项 {stats.spearmanr(m.fear, m.f_cross).statistic:+.3f}")

    # ── 一、水平相关：bias60 vs 恐慌的三个切面 ──────────────
    print("\n" + "=" * 112)
    print("一、水平相关（Spearman ρ，负号＝乖离率越低恐慌越高，符合直觉）")
    print("=" * 112)
    print(f"{'指数':>9s}{'n':>6s}{'vs 合成分':>11s}{'vs ①动量':>11s}{'vs ②波动率':>12s}"
          f"{'vs ③④⑤横截面':>15s}{'机械占比':>10s}")
    S = {}
    for nm, f, col, oos in UNIVERSE:
        d = pd.read_csv(root / f"{nm}.csv", dtype={"trade_date": str})
        d = d.merge(comp, on="trade_date", how="left")
        d["f_cross"] = (5 * d.fear - d.f_mom - d.f_vol) / 3
        d = d.dropna(subset=["bias60", "fear", "f_cross"])
        S[nm] = d
        r_all = stats.spearmanr(d.bias60, d.fear).statistic
        r_mom = stats.spearmanr(d.bias60, d.f_mom).statistic
        r_vol = stats.spearmanr(d.bias60, d.f_vol).statistic
        r_x = stats.spearmanr(d.bias60, d.f_cross).statistic
        star = "★" if oos else " "
        print(f"{star}{nm:>8s}{len(d):>6d}{r_all:>+11.3f}{r_mom:>+11.3f}{r_vol:>+12.3f}"
              f"{r_x:>+15.3f}{abs(r_mom) / (abs(r_mom) + abs(r_x)):>10.0%}")
    print("  「机械占比」＝|ρ(①动量)| / (|ρ(①动量)| + |ρ(横截面)|)，粗略表示相关里有多少是定义带来的。")

    # ── 二、极值集合的重合 ────────────────────────────────
    print("\n" + "=" * 112)
    print("二、极值重合：乖离率低尾 ∩ 恐慌高尾（这才是 owner 问的「有没有相关性」）")
    print("=" * 112)
    for q in TAILS:
        print(f"\n  ── 乖离率低尾定义：全样本后 {q:.0%} 分位 ──")
        print(f"{'指数':>9s}{'低尾日':>7s}{'恐慌≥75':>8s}{'同时':>6s}"
              f"{'P(恐慌≥75|低尾)':>16s}{'P(低尾|恐慌≥75)':>16s}{'Jaccard':>9s}"
              f"{'低尾日恐慌中位':>13s}{'其余日':>8s}")
        for nm, d in S.items():
            th = d.bias60.quantile(q)
            lo = (d.bias60 <= th).to_numpy()
            fh = (d.fear >= FEAR_HI).to_numpy()
            o = overlap(lo, fh)
            print(f"{nm:>9s}{o['na']:>7d}{o['nb']:>8d}{o['both']:>6d}"
                  f"{o['p_b_given_a']:>16.0%}{o['p_a_given_b']:>16.0%}{o['jaccard']:>9.2f}"
                  f"{d.fear[lo].median():>13.1f}{d.fear[~lo].median():>8.1f}")

    print(f"\n  ── 高尾（涨过头）∩ 极贪婪（恐慌 ≤ {FEAR_GREED:.0f}）──")
    print(f"{'指数':>9s}{'高尾日':>7s}{'极贪日':>7s}{'同时':>6s}"
          f"{'P(极贪|高尾)':>14s}{'P(高尾|极贪)':>14s}{'高尾日恐慌中位':>13s}{'其余日':>8s}")
    for nm, d in S.items():
        hi = (d.bias60 >= d.bias60.quantile(0.95)).to_numpy()
        gr = (d.fear <= FEAR_GREED).to_numpy()
        o = overlap(hi, gr)
        print(f"{nm:>9s}{o['na']:>7d}{o['nb']:>7d}{o['both']:>6d}"
              f"{o['p_b_given_a']:>14.0%}{o['p_a_given_b']:>14.0%}"
              f"{d.fear[hi].median():>13.1f}{d.fear[~hi].median():>8.1f}")

    # ── 三、领先滞后 ──────────────────────────────────────
    print("\n" + "=" * 112)
    print("三、领先滞后：谁先动？（对**日度变化**求相关，避免两条高自相关水平序列的伪相关）")
    print("=" * 112)
    print("  正 lag ＝ 恐慌滞后于乖离率（乖离率先动）；负 lag ＝ 恐慌先动")
    print(f"{'指数':>9s}{'最优lag':>8s}{'该lag ρ':>10s}{'lag=0':>9s}"
          f"{'lag−5':>9s}{'lag+5':>9s}  对象")
    for nm, d in S.items():
        for tgt, lbl in (("fear", "合成分"), ("f_cross", "横截面三项")):
            db = d.bias60.diff().to_numpy()
            df_ = d[tgt].diff().to_numpy()
            rs = {}
            for L in LAGS:
                x, y = (db[:-L], df_[L:]) if L > 0 else ((db[-L:], df_[:L]) if L < 0 else (db, df_))
                msk = ~(np.isnan(x) | np.isnan(y))
                rs[L] = stats.spearmanr(x[msk], y[msk]).statistic if msk.sum() > 100 else np.nan
            best = max(rs, key=lambda k: abs(rs[k]) if rs[k] == rs[k] else -1)
            print(f"{nm if lbl == '合成分' else '':>9s}{best:>8d}{rs[best]:>+10.3f}"
                  f"{rs[0]:>+9.3f}{rs[-5]:>+9.3f}{rs[5]:>+9.3f}  {lbl}")

    # ── 四、极值 episode 的时间距离 ────────────────────────
    print("\n" + "=" * 112)
    print("四、极值 episode 的时间距离：乖离率见底那天，恐慌极值离它多远？")
    print("=" * 112)
    print(f"{'指数':>9s}{'低尾episode':>12s}{'恐慌≥75 episode':>16s}"
          f"{'距离中位(交易日)':>16s}{'±5日内':>8s}{'±20日内':>9s}")
    for nm, d in S.items():
        b = d.bias60.to_numpy()
        fe = d.fear.to_numpy()
        lo_th = np.quantile(b, 0.05)
        li = np.flatnonzero(b <= lo_th)
        fi = np.flatnonzero(fe >= FEAR_HI)
        # episode：间隔 >20 个交易日视为不同事件，取事件内最极端的一天
        def eps(idx, arr, mode):
            if len(idx) == 0:
                return []
            groups: list[list[int]] = [[int(idx[0])]]
            for j in idx[1:]:
                if int(j) - groups[-1][-1] <= 20:
                    groups[-1].append(int(j))
                else:
                    groups.append([int(j)])
            pick = min if mode == "min" else max
            return [pick(g, key=lambda i: arr[i]) for g in groups]
        le, fee = eps(li, b, "min"), eps(fi, fe, "max")
        if not le or not fee:
            print(f"{nm:>9s}{len(le):>12d}{len(fee):>16d}{'—':>16s}{'—':>8s}{'—':>9s}")
            continue
        dist = [min(abs(x - y) for y in fee) for x in le]
        print(f"{nm:>9s}{len(le):>12d}{len(fee):>16d}{np.median(dist):>16.0f}"
              f"{np.mean([x <= 5 for x in dist]):>8.0%}{np.mean([x <= 20 for x in dist]):>9.0%}")

    print("\n" + "=" * 112)
    print("⚠️ 全节样本自 2015-01（恐慌数据起点）——2007/2008 那两轮最极端的乖离率不在样本内。")
    print("=" * 112)


if __name__ == "__main__":
    main()
