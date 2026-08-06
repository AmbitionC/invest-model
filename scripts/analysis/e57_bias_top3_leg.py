# -*- coding: utf-8 -*-
"""E57 —— 乖离率**双尾前三**作为独立中短线腿（P67）。

owner 命题：「所有指数，偏离度高低，在前三的位置的话，都可以作为比较强烈的信号，
值得中短线操作。」

判据 **2026-08-05 跑数前写死于 `docs/model_change_proposals.md` P67 段**（见 git 提交顺序），
本脚本逐条执行、一字不改，并**全部评估、不短路**——这是针对上一轮 E51-E55 集体失效机制
（AND 门第一条被判死后稳健性判据永不被评估）的直接修正。

标的 7 个指数，其中 **上证50 / 中证500 / 中证1000 从未参与过任何调参**＝真样本外。
腿的定义：因果排名前 3 触发 → 次一交易日收盘进/出 → 持有 H 日无条件平仓 → 满仓或空仓。

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

RNG = np.random.default_rng(20260805)
K = 3                      # 判据写死：前三
HS = (20, 60)              # 持有期两档并列
WINDOWS = (20, 60, 120)    # MA 窗口对照臂
WARMS = (350, 500, 650, 800)
WARM_MAIN = 500
NPERM = 2000

# (名称, 文件, 列, 是否样本外)
UNIVERSE = [
    ("沪深300", "index_dump_000300_SH.csv", "close", False),
    ("创业板", "spread_full_history.csv", "chinext", False),
    ("科创50", "index_dump_000688_SH.csv", "close", False),
    ("中证红利", "index_dump_000922_CSI.csv", "close", False),
    ("上证50", "index_dump_000016_SH.csv", "close", True),
    ("中证500", "index_dump_000905_SH.csv", "close", True),
    ("中证1000", "index_dump_000852_SH.csv", "close", True),
]


def load(root: Path, f: str, col: str) -> pd.DataFrame:
    d = pd.read_csv(root / f, dtype={"trade_date": str}).sort_values(
        "trade_date").reset_index(drop=True)
    d["c"] = pd.to_numeric(d[col])
    return d[["trade_date", "c"]].dropna().reset_index(drop=True)


def causal_topk(b: np.ndarray, k: int, side: str, warm: int) -> np.ndarray:
    """当日读数是否为**截至当日**见过的最极端 k 个之一（因果，不含未来）。"""
    hist: list[float] = []
    hit = np.zeros(len(b), bool)
    for i in range(len(b)):
        if b[i] != b[i]:
            continue
        if len(hist) >= warm:
            arr = np.asarray(hist)
            kth = np.partition(arr, k - 1)[k - 1] if side == "low" else np.partition(arr, -k)[-k]
            if (b[i] <= kth) if side == "low" else (b[i] >= kth):
                hit[i] = True
        hist.append(b[i])
    return hit


def fwd(c: np.ndarray, i: int, h: int) -> float:
    """从**次一交易日收盘**起算的 h 日收益（exec_lag=1，判据已写明）。"""
    a, b = i + 1, i + 1 + h
    return c[b] / c[a] - 1 if b < len(c) else np.nan


def eff(c: np.ndarray, idx: list[int], h: int, i0: int) -> tuple[float, int]:
    """效应量 = 触发日 h 日收益均值 − 该指数全样本 h 日收益均值。"""
    t = [fwd(c, i, h) for i in idx]
    t = [x for x in t if x == x]
    allr = [fwd(c, i, h) for i in range(i0, len(c) - h - 1)]
    allr = [x for x in allr if x == x]
    if not t or not allr:
        return np.nan, 0
    return float(np.mean(t) - np.mean(allr)), len(t)


def eps_count(idx: list[int], gap: int) -> int:
    if not idx:
        return 0
    n, prev = 1, idx[0]
    for i in idx[1:]:
        if i - prev > gap:
            n += 1
            prev = i
    return n


def leg_nav(c: np.ndarray, idx: list[int], h: int, i0: int, side: str,
            mode: str = "extend") -> tuple[float, float, float]:
    """把腿做成净值。低尾腿：触发→满仓 h 日，其余空仓。高尾腿：触发→空仓 h 日，其余满仓。

    mode='extend' 持仓期内再触发则顺延；'reset' 则重置计时（两个实现臂）。
    返回 (年化, 夏普, 最大回撤)。
    """
    n = len(c)
    inpos = np.zeros(n, bool)
    until = -1
    hits = set(idx)
    for i in range(i0, n):
        if i in hits:
            until = (max(until, i + h) if mode == "extend" else i + h)
        inpos[i] = i <= until
    hold = inpos if side == "low" else ~inpos
    r = np.zeros(n)
    r[1:] = c[1:] / c[:-1] - 1
    # 次一交易日收盘生效：持仓状态右移一位
    hold = np.concatenate([[False], hold[:-1]])
    rr = np.where(hold[i0:], r[i0:], 0.0)
    v = np.cumprod(1 + rr)
    yrs = len(rr) / 250.0
    ann = v[-1] ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    pk = np.maximum.accumulate(v)
    return ann, ((ann - 0.02) / vol if vol else np.nan), float(((v - pk) / pk).min())


def bh(c: np.ndarray, i0: int) -> tuple[float, float, float]:
    r = np.zeros(len(c))
    r[1:] = c[1:] / c[:-1] - 1
    v = np.cumprod(1 + r[i0:])
    yrs = len(v) / 250.0
    ann = v[-1] ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    pk = np.maximum.accumulate(v)
    return ann, ((ann - 0.02) / vol if vol else np.nan), float(((v - pk) / pk).min())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    a = ap.parse_args()
    root = Path(a.data)

    D = {}
    for nm, f, col, oos in UNIVERSE:
        d = load(root, f, col)
        c = d.c.to_numpy(dtype=float)
        D[nm] = dict(dates=d.trade_date.to_numpy(), c=c, oos=oos, bias={})
        for w in WINDOWS:
            D[nm]["bias"][w] = (pd.Series(c) / pd.Series(c).rolling(w).mean() - 1).to_numpy()

    print("=" * 112)
    print("E57 —— 乖离率双尾前三作为独立中短线腿（P67）｜判据 2026-08-05 跑数前写死，全部评估不短路")
    print(f"7 个指数（★＝从未参与调参的样本外对照）· K={K} · 持有 H∈{HS} · exec_lag=1")
    print("=" * 112)

    res = {}
    for side, label in (("low", "低尾腿（跌到前三 → 买入持有 H 日）"),
                        ("high", "高尾腿（涨到前三 → 清仓空仓 H 日）")):
        print(f"\n{'█' * 112}\n■ {label}\n{'█' * 112}")
        for h in HS:
            print(f"\n  ── 持有 H = {h} 个交易日 ──")
            print(f"    {'指数':>9s}{'触发日':>7s}{'episode':>9s}{'触发后均值':>11s}"
                  f"{'全样本均值':>11s}{'效应量':>9s}{'为正':>9s}{'置换p':>8s}")
            rows = []
            for nm in D:
                z = D[nm]
                c, b = z["c"], z["bias"][60]
                i0 = int(np.argmax(~np.isnan(b))) + WARM_MAIN
                if i0 >= len(c) - h - 2:
                    continue
                hit = causal_topk(b, K, side, WARM_MAIN)
                idx = [i for i in np.where(hit)[0] if i0 <= i < len(c) - h - 1]
                e, n = eff(c, idx, h, i0)
                tvals = [fwd(c, i, h) for i in idx]
                tvals = [x for x in tvals if x == x]
                allr = [fwd(c, i, h) for i in range(i0, len(c) - h - 1)]
                allr = [x for x in allr if x == x]
                pos = f"{sum(1 for x in tvals if x > 0)}/{len(tvals)}" if tvals else "—"
                # 置换检验
                if n >= 2 and allr:
                    arr = np.asarray(allr)
                    draws = RNG.choice(arr, size=(NPERM, n), replace=True).mean(axis=1) - arr.mean()
                    p = float((draws <= e).mean() if side == "high" else (draws >= e).mean())
                else:
                    p = np.nan
                star = "★" if z["oos"] else " "
                print(f"    {star}{nm:>8s}{len(idx):>7d}{eps_count(idx, h):>9d}"
                      f"{(np.mean(tvals) if tvals else np.nan):>11.2%}"
                      f"{(np.mean(allr) if allr else np.nan):>11.2%}"
                      f"{e:>+9.2%}{pos:>9s}{p:>8.3f}")
                rows.append(dict(nm=nm, oos=z["oos"], n=len(idx), eps=eps_count(idx, h),
                                 eff=e, p=p, idx=idx, i0=i0))
            res[(side, h)] = rows

            # 合并置换检验（7 指数所有触发日一起）
            pool_t, pool_a, tot = [], [], 0
            for r in rows:
                z = D[r["nm"]]
                c = z["c"]
                pool_t += [x for x in (fwd(c, i, h) for i in r["idx"]) if x == x]
                pool_a += [x for x in (fwd(c, i, h) for i in range(r["i0"], len(c)-h-1)) if x == x]
                tot += r["n"]
            if pool_t and pool_a:
                arr = np.asarray(pool_a)
                e_all = float(np.mean(pool_t) - arr.mean())
                draws = RNG.choice(arr, size=(NPERM, len(pool_t)), replace=True).mean(axis=1) - arr.mean()
                p_all = float((draws <= e_all).mean() if side == "high" else (draws >= e_all).mean())
                print(f"    {'合并':>9s}{tot:>7d}{'':>9s}{np.mean(pool_t):>11.2%}"
                      f"{arr.mean():>11.2%}{e_all:>+9.2%}{'':>9s}{p_all:>8.3f}")
                res[(side, h, "pool")] = (e_all, p_all, tot)

    # ── 判据逐条 ────────────────────────────────────────────────
    print("\n" + "=" * 112)
    print("判据逐条评估（全部评估、不短路）")
    print("=" * 112)

    verdict = {}
    for side in ("low", "high"):
        thr = 0.03 if side == "low" else -0.03
        ok1 = False
        for h in HS:
            rows = res[(side, h)]
            n_ok = sum(1 for r in rows if (r["eff"] >= thr if side == "low" else r["eff"] <= thr))
            ok1 |= n_ok >= 5
            print(f"  ① {side:>4s} H={h:>2d}：达标指数 {n_ok}/7（要求 ≥5，阈值 "
                  f"{'≥+3.0pp' if side == 'low' else '≤−3.0pp'}）")
        verdict[(side, 1)] = ok1

        thr2 = 0.015 if side == "low" else -0.015
        ok2 = False
        for h in HS:
            rows = [r for r in res[(side, h)] if r["oos"]]
            n_ok = sum(1 for r in rows if (r["eff"] >= thr2 if side == "low" else r["eff"] <= thr2))
            ok2 |= n_ok >= 2
            oo = "、".join(f"{r['nm']} {r['eff']:+.2%}" for r in rows)
            print(f"  ② {side:>4s} H={h:>2d} 样本外：达标 {n_ok}/3（要求 ≥2，阈值 "
                  f"{'≥+1.5pp' if side == 'low' else '≤−1.5pp'}）　{oo}")
        verdict[(side, 2)] = ok2

        ok3 = False
        for h in HS:
            rows = res[(side, h)]
            per = all(r["eps"] >= 3 for r in rows)
            tot = sum(r["eps"] for r in rows)
            months = set()
            for r in rows:
                dts = D[r["nm"]]["dates"]
                months |= {str(dts[i])[:6] for i in r["idx"]}
            ok3 |= (per and tot >= 25 and len(months) >= 10)
            print(f"  ③ {side:>4s} H={h:>2d}：每指数 episode≥3 {'✅' if per else '❌'}"
                  f"（最小 {min(r['eps'] for r in rows)}）· 合计 {tot}（要求 ≥25）"
                  f"· 自然月去重后有效独立事件 {len(months)}（要求 ≥10）")
        verdict[(side, 3)] = ok3

        ok4 = False
        for h in HS:
            rows = res[(side, h)]
            ps = [r["p"] for r in rows if r["p"] == r["p"]]
            e_all, p_all, _ = res.get((side, h, "pool"), (np.nan, np.nan, 0))
            n_sig = sum(1 for p in ps if p < 0.05)
            ok4 |= (p_all < 0.05)
            print(f"  ④ {side:>4s} H={h:>2d}：单指数 p<0.05 的有 {n_sig}/{len(ps)}"
                  f"· **合并 p = {p_all:.3f}**（判据要求合并 p<0.05）")
        verdict[(side, 4)] = ok4

    # ⑤ 交易层面
    print("\n  ⑤ 交易层面（腿净值 vs 买入持有；夏普 +0.10 且回撤不恶化 >5pp，≥5/7）")
    for side in ("low", "high"):
        for h in HS:
            rows = res[(side, h)]
            n_ok, det = 0, []
            for r in rows:
                z = D[r["nm"]]
                c, i0 = z["c"], r["i0"]
                la, ls, lm = leg_nav(c, r["idx"], h, i0, side)
                ba, bs, bm = bh(c, i0)
                good = (ls - bs) >= 0.10 and (lm - bm) >= -0.05
                n_ok += good
                det.append(f"{r['nm']}Δ夏普{ls-bs:+.2f}/Δ回撤{(lm-bm)*100:+.1f}pp")
            print(f"    {side:>4s} H={h:>2d}：达标 {n_ok}/7　" + " ".join(det))
            verdict[(side, 5)] = verdict.get((side, 5), False) or (n_ok >= 5)

    # ⑥ 稳健（带触发数>0 前置门）
    print("\n  ⑥ 稳健性（前置门：处理臂触发数>0 才评估）")
    for side in ("low", "high"):
        h = 20
        # MA 窗口
        wsign = []
        for w in WINDOWS:
            s_ = 0
            for nm in D:
                z = D[nm]; c, b = z["c"], z["bias"][w]
                i0 = int(np.argmax(~np.isnan(b))) + WARM_MAIN
                if i0 >= len(c) - h - 2:
                    continue
                idx = [i for i in np.where(causal_topk(b, K, side, WARM_MAIN))[0]
                       if i0 <= i < len(c) - h - 1]
                e, n = eff(c, idx, h, i0)
                if n > 0 and e == e:
                    s_ += (1 if e > 0 else -1)
            wsign.append(s_)
        # 分半
        half = []
        for nm in D:
            z = D[nm]; c, b = z["c"], z["bias"][60]
            i0 = int(np.argmax(~np.isnan(b))) + WARM_MAIN
            if i0 >= len(c) - h - 2:
                continue
            mid = (i0 + len(c)) // 2
            idx = [i for i in np.where(causal_topk(b, K, side, WARM_MAIN))[0]
                   if i0 <= i < len(c) - h - 1]
            e1, n1 = eff(c, [i for i in idx if i < mid], h, i0)
            e2, n2 = eff(c, [i for i in idx if i >= mid], h, mid)
            if n1 > 0 and n2 > 0 and e1 == e1 and e2 == e2:
                half.append(np.sign(e1) == np.sign(e2))
        # WARM
        wm = []
        for wa in WARMS:
            s_ = 0
            for nm in D:
                z = D[nm]; c, b = z["c"], z["bias"][60]
                i0 = int(np.argmax(~np.isnan(b))) + wa
                if i0 >= len(c) - h - 2:
                    continue
                idx = [i for i in np.where(causal_topk(b, K, side, wa))[0]
                       if i0 <= i < len(c) - h - 1]
                e, n = eff(c, idx, h, i0)
                if n > 0 and e == e:
                    s_ += (1 if e > 0 else -1)
            wm.append(s_)
        print(f"    {side:>4s}：MA 窗口 20/60/120 净符号 {wsign}（≥2 档同号？"
              f"{'✅' if len({np.sign(x) for x in wsign}) <= 2 and wsign.count(max(wsign, key=abs)) else '—'}）"
              f"· 分半同号 {sum(half)}/{len(half)}"
              f"· WARM 350/500/650/800 净符号 {wm}")
        verdict[(side, 6)] = (len(half) > 0 and sum(half) >= len(half) * 0.6
                              and len({int(np.sign(x)) for x in wm if x != 0}) <= 1)

    print("\n" + "=" * 112)
    print("E57 裁决")
    print("=" * 112)
    for side, lb in (("low", "低尾腿"), ("high", "高尾腿")):
        v = [verdict.get((side, i), False) for i in range(1, 7)]
        print(f"  {lb}：①{'✅' if v[0] else '❌'} ②{'✅' if v[1] else '❌'} "
              f"③{'✅' if v[2] else '❌'} ④{'✅' if v[3] else '❌'} "
              f"⑤{'✅' if v[4] else '❌'} ⑥{'✅' if v[5] else '❌'}")
        if not (v[1] and v[2] and v[3]):
            why = []
            if not v[1]:
                why.append("样本外塌")
            if not v[2]:
                why.append("样本不足")
            if not v[3]:
                why.append("不显著")
            print(f"    ⟹ **FAIL**（判据写明②③④任一不过即否）：{'、'.join(why)}")
        elif all(v):
            print("    ⟹ **PASS** → 走高置信直升评估，登记为独立中短线提示腿。")
        else:
            print("    ⟹ **①②③④过而⑤⑥不过 → 记知识库，不接入生产。**")


def warm_sensitivity(root: Path) -> None:
    """附（**非判据·披露**）：排名预热对触发数与效应量的影响。

    🔴 起因：主表里沪深300 与中证红利的**高尾触发数为 0**，我在首轮汇报里把它写成
    「它们的历史最高偏离出现在 2007 年、落在预热期内」——**这是把我自己的参数选择
    说成了数据事实**。预登记的判据⑥ WARM 网格是 {350,500,650,800}，**下界 350 本身
    就高到足以永远排除 2007 年那批峰值** ⟹ 那条稳健性判据在这个问题上没有分辨力。
    这是本系列第三处判据设计缺陷（前两处：E56 的②聚合口径未写明、④零触发时空洞通过）。

    本节把预热放宽到 {120, 250, 500}，看裁决是否为参数假象。**结果不改判**（见下），
    但理由必须换成实测，不能再用「落在预热期内」这种把参数当事实的说法。
    """
    print("\n" + "=" * 112)
    print("附（非判据·披露）：排名预热敏感性 —— 主表的 0 触发是数据事实还是参数假象？")
    print("=" * 112)
    for side in ("high", "low"):
        print(f"\n  ── {'高尾' if side == 'high' else '低尾'}（H=20）──")
        print(f"    {'指数':>9s}" + "".join(f"{'预热'+str(w):>22s}" for w in (120, 250, 500)))
        print(f"    {'':>9s}" + "".join(f"{'触发/事件':>11s}{'效应量':>11s}" for _ in range(3)))
        for nm, f, col, oos in UNIVERSE:
            d = load(root, f, col)
            c = d.c.to_numpy(dtype=float)
            b = (pd.Series(c) / pd.Series(c).rolling(60).mean() - 1).to_numpy()
            row = ""
            for w in (120, 250, 500):
                i0 = int(np.argmax(~np.isnan(b))) + w
                if i0 >= len(c) - 22:
                    row += f"{'—':>11s}{'—':>11s}"
                    continue
                idx = [i for i in np.where(causal_topk(b, K, side, w))[0]
                       if i0 <= i < len(c) - 21]
                e, n = eff(c, idx, 20, i0)
                row += f"{f'{len(idx)}/{eps_count(idx, 20)}':>11s}"
                row += (f"{e:>+11.2%}" if n else f"{'—':>11s}")
            star = "★" if oos else " "
            print(f"    {star}{nm:>8s}" + row)


if __name__ == "__main__":
    main()
    warm_sensitivity(Path("results"))
