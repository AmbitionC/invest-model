# -*- coding: utf-8 -*-
"""乖离率低尾的**历史极值排名**口径（owner 2026-08-05：「我要看的是历史极值排第几，
进到前五你再看看」）。

**为什么单独一个脚本**：E56 首跑用的是**分位**（X=2%/5%/10%），而博主自己的口径是
**全历史极值排名**（P39/E37 命题原文：「用全历史极值排名（不是滚动分位）度量风险」，
他数的是「创业板 27.53% 近十年排名第五，前四分别是…」）。分位和排名不是一回事——
X=2% 一档四腿还有 44~71 天，那不叫极值。这是 E56 设计时我替换掉的一个口径，须补上。

本脚本只做两件事：
  A. 把四腿乖离率（相对 MA60）的**低尾极值谱**列出来——按不重叠 episode 排名，
     给日期、读数、排名，以及当前读数排第几。这是 owner 直接要看的东西。
  B. 回答「进到前五会怎样」：前五 episode 与 B2 腿既有价格闸的共现，以及前瞻收益。

⚠️ **治理边界**：B 部分是**探索，不是判据**。E56 已按写死的判据 FAIL；换排名口径要不要
改判，必须**实测**，不能靠推理——

🔴 **我第一版在这里推错过一次，留痕**：当时写「排名前五是分位 2% 的真子集（5/500=1%<2%），
而 X=2% 档共现已实测为 0，故前五必然也是 0」。**这个推理对逐日排名成立，对 episode 排名不成立**
——episode 排名把同一轮里的连续深跌折叠成一个代表点，所以第 4、5 名的代表日**可以落在
2~5% 分位带里**（实测：创业板 20181018 因果分位 3.44%、20250407 为 2.43%，两天都过 B2 价格闸）。
**结论没变（见下实测 2/20），但当时那条理由是错的，不能用。**

本脚本因此并列两个排名口径：
  · **事后 episode 谱**（descriptive）：把全历史低尾按不重叠 episode 归并后排名。
    用来回答 owner 的问题「现在排第几」，**但它不可交易**——"谁是第 1 名"要看完全部历史才知道。
  · **因果排名**（tradable）：当日读数是否为**截至当日**见过的最低 K 个之一。
    这才是能写进规则的版本，也是 E37 高尾用的口径（"超过此前历史最大值"）。

若前瞻收益为正，那证明的是**另一个命题**：「乖离率低尾单独作短线信号」。
它需要自己的 P/E，且按双引擎判定**必须整条短线腿一起上**（进场 + 退出 + 仓位）。

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
from long_window_backtest import LEGS  # noqa: E402
from e56_bias_low_tail import bias_and_causal_pct, prep_all, with_warm, first_tradable  # noqa: E402

GAP = 60          # 不重叠 episode 的间隔（交易日），与 E56 判据③同口径
TOPN = 8          # 列出前 8，owner 关心的是前 5


def low_episodes(b: np.ndarray, dates: np.ndarray, i0: int, gap: int = GAP) -> list[dict]:
    """把低尾按不重叠 episode 归并：每段取其**最低点**作为该 episode 的代表。

    做法：按 bias 从低到高扫，若该日与已选中的任一代表相距 ≤gap 则并入既有 episode，
    否则新开一个。等价于「贪心取全局最低点、屏蔽其前后 gap 天、再取次低」。
    """
    order = np.argsort(b[i0:], kind="stable") + i0
    reps: list[int] = []
    for i in order:
        if b[i] != b[i]:
            continue
        if all(abs(i - j) > gap for j in reps):
            reps.append(int(i))
        if len(reps) >= 40:
            break
    return [{"i": i, "date": str(dates[i]), "bias": float(b[i])} for i in reps]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    ap.add_argument("--w", type=int, default=60, help="均线窗口（博主口径 60）")
    a = ap.parse_args()
    root = Path(a.data)

    print("=" * 108)
    print(f"乖离率低尾·历史极值排名（相对 MA{a.w}，博主口径：全历史极值排名，不是滚动分位）")
    print(f"episode 按不重叠 {GAP} 个交易日归并，每段取最低点为代表")
    print("=" * 108)

    all_rows = []
    for nm, f, col, trf, _fx, mode in LEGS:
        df0, _ = prep_all(root, f, col, trf)
        b, pct = bias_and_causal_pct(df0.c, a.w)
        df = with_warm(df0, 500)
        d = df.trade_date.values
        i0 = int(np.searchsorted(d, first_tradable(df, mode)))
        eps = low_episodes(b, d, i0)
        cur_b = float(b[-1])
        # 当前读数在全历史里的排名（第几低）
        valid = b[i0:][~np.isnan(b[i0:])]
        cur_rank = int((valid < cur_b).sum()) + 1
        # 当前读数在 episode 谱里的位置
        cur_ep_rank = sum(1 for e in eps if e["bias"] < cur_b) + 1

        print(f"\n【{nm}】数据 {d[i0]} ~ {d[-1]}　当前乖离率 {cur_b:+.2%}"
              f"　全历史逐日排名 第 {cur_rank} 低（共 {len(valid)} 天）"
              f"　episode 谱上排 第 {cur_ep_rank}")
        print(f"  {'排名':>4s}{'日期':>10s}{'乖离率':>9s}{'当日因果分位':>13s}"
              f"{'<近5年中位数':>13s}{'恐慌':>7s}{'后20日':>9s}{'后60日':>9s}{'后250日':>10s}")
        c = df.c.values
        r12 = df.r1250.values
        fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
        fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
        for k, e in enumerate(eps[:TOPN], 1):
            i = e["i"]
            gate = "✅是" if (r12[i] == r12[i] and c[i] < r12[i]) else "❌否"
            fv = fmap.get(str(d[i]), np.nan)
            fs = f"{fv:.0f}" if fv == fv else "—"
            fwd = []
            for h in (20, 60, 250):
                j = i + h
                fwd.append(f"{c[j] / c[i] - 1:>+9.1%}" if j < len(c) else f"{'—':>9s}")
            pv = pct[i]
            pv_s = f"{pv:.2%}" if pv == pv else "—"
            print(f"  {k:>4d}{e['date']:>10s}{e['bias']:>9.2%}{pv_s:>13s}"
                  f"{gate:>13s}{fs:>7s}{fwd[0]}{fwd[1]}{fwd[2]:>10s}")
            if k <= 5:
                all_rows.append(dict(leg=nm, rank=k, date=e["date"], bias=e["bias"],
                                     gate=(r12[i] == r12[i] and c[i] < r12[i]),
                                     fear=fv,
                                     f20=(c[i + 20] / c[i] - 1) if i + 20 < len(c) else np.nan,
                                     f60=(c[i + 60] / c[i] - 1) if i + 60 < len(c) else np.nan,
                                     f250=(c[i + 250] / c[i] - 1) if i + 250 < len(c) else np.nan))

    # ── 前五 episode 的汇总：这才是 owner 问的那一档 ────────────────────────
    R = pd.DataFrame(all_rows)
    print("\n" + "=" * 108)
    print("前五 episode 汇总（四腿合计 n=20）")
    print("=" * 108)
    print(f"  同时满足 B2 既有价格闸「收盘 < 近5年中位数」的：{int(R.gate.sum())}/{len(R)}")
    print(f"  同时恐慌 ≥75 的：{int((R.fear >= 75).sum())}/{len(R)}"
          f"（其中恐慌数据缺失 {int(R.fear.isna().sum())} 个——2015 年前无恐慌数据）")
    for h, cname in ((20, "f20"), (60, "f60"), (250, "f250")):
        v = R[cname].dropna()
        print(f"  后 {h:>3d} 日：均值 {v.mean():+.1%}　中位 {v.median():+.1%}"
              f"　为正 {(v > 0).sum()}/{len(v)}　最差 {v.min():+.1%}　最好 {v.max():+.1%}")
    print("\n  逐腿：")
    for nm, g in R.groupby("leg", sort=False):
        print(f"    {nm:>7s} 价格闸命中 {int(g.gate.sum())}/5　"
              f"后20日均值 {g.f20.mean():+.1%}　后60日 {g.f60.mean():+.1%}　"
              f"后250日 {g.f250.mean():+.1%}")

    # ── C. 因果排名口径（可交易版）+ 接进 B2 的实测 ──────────────────────
    print("\n" + "=" * 108)
    print("因果排名口径（可交易版）：当日乖离率是否为**截至当日**见过的最低 K 个之一")
    print("  这是 E37 高尾用的同一种口径（'超过此前历史最大值'），事后 episode 谱不可交易。")
    print("=" * 108)
    print(f"  {'腿':>7s}{'K':>5s}{'触发日':>8s}{'不重叠事件':>12s}"
          f"{'过B2价格闸':>12s}{'恐慌≥75':>9s}{'后20日均值':>12s}{'为正':>8s}")
    from e56_bias_low_tail import run as e56run, episodes as e56eps
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    causal_low = {}
    for nm, f, col, trf, _fx, mode in LEGS:
        df0, ret = prep_all(root, f, col, trf)
        b, _ = bias_and_causal_pct(df0.c, a.w)
        df = with_warm(df0, 500)
        d, c = df.trade_date.values, df.c.values
        i0 = int(np.searchsorted(d, first_tradable(df, mode)))
        r12 = df.r1250.values
        for K in (3, 5, 10):
            hist: list[float] = []
            hit = np.zeros(len(b), bool)
            for i in range(len(b)):
                if b[i] != b[i]:
                    continue
                if len(hist) >= 500:
                    kth = np.partition(np.asarray(hist), K - 1)[K - 1]
                    if b[i] <= kth:
                        hit[i] = True
                hist.append(b[i])
            sel = [i for i in np.where(hit)[0] if i >= i0]
            if K == 5:
                causal_low[nm] = (df, ret, mode, hit)
            gate = sum(1 for i in sel if r12[i] == r12[i] and c[i] < r12[i])
            hot = sum(1 for i in sel if fmap.get(str(d[i]), np.nan) >= 75)
            f20 = [c[i + 20] / c[i] - 1 for i in sel if i + 20 < len(c)]
            m = f"{np.mean(f20):+.1%}" if f20 else "—"
            pos = f"{sum(1 for x in f20 if x > 0)}/{len(f20)}" if f20 else "—"
            print(f"  {nm:>7s}{K:>5d}{len(sel):>8d}{e56eps(sel):>12d}"
                  f"{gate:>12d}{hot:>9d}{m:>12s}{pos:>8s}")

    print("\n  ── 把因果排名前 5 接进 B2 作追加触发器（E56 判据①的口径，仅换触发器定义）──")
    print(f"  {'腿':>7s}{'Δ年化':>10s}{'Δ回撤':>10s}{'低尾买入笔数':>14s}")
    n_eff = 0
    for nm, f, col, trf, _fx, mode in LEGS:
        df, ret, md, hit = causal_low[nm]
        d0 = first_tradable(df, md)
        base = e56run(df, ret, fmap, nm, d0, None, md)
        tr = e56run(df, ret, fmap, nm, d0, None, md, low=hit)
        da = (tr["ann"] - base["ann"]) * 100
        dm = (tr["mdd"] - base["mdd"]) * 100
        n_eff += da >= 0.50
        print(f"  {nm:>7s}{da:>+10.2f}{dm:>+10.2f}{tr['nlow']:>14d}")
    print(f"  ⟹ 达标腿 {n_eff}/4（E56 判据①要求 ≥3 且无一腿回撤恶化 >3pp）")

    print("\n" + "=" * 108)
    print("读法（治理边界，必须一起读）")
    print("=" * 108)
    print("  1. 上面的前瞻收益是**探索，不是判据**。E56 已按 2026-08-05 写死的判据 FAIL，")
    print("     换排名口径后**实测**仍不改判（见上面 C 段：达标腿远不足 3）。")
    print("     ⚠️ 留痕：我第一版在这里用「前五是 2% 分位的真子集」推过一次，**那条推理是错的**")
    print("     ——它对逐日排名成立、对 episode 排名不成立（实测创业板第 4/5 名分位 3.44%/2.43%）。")
    print("     结论未变，但当时的理由不成立，已作废。")
    print("  2. 若前瞻收益为正，那证明的是**另一个命题**：「乖离率低尾单独作短线信号」。")
    print("     它需要自己的 P/E，且按双引擎判定**必须整条短线腿一起上**（进场+退出+仓位）——")
    print("     博主自己就是这么用的（『搏反弹』+『跑路时做到心中有数』）。")
    print("  3. 前五 episode 四腿合计只有 20 个样本，且多集中在同几轮系统性调整里，")
    print("     跨腿高度相关 ⟹ **有效独立事件远少于 20**。")


if __name__ == "__main__":
    main()
