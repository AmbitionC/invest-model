# -*- coding: utf-8 -*-
"""E58 —— 乖离率极值 + **止盈止损** 的小仓位中短线 sleeve（P68）。

owner 2026-08-05：「破极值后有回撤或补涨，为什么不拿一部分仓位做中短线？
带好止盈止损，胜率是不是也挺好？」

判据 **2026-08-05 跑数前写死于 `docs/model_change_proposals.md` P68 段**（见 git 提交顺序，
判据先单独提交、脚本后写），本脚本逐条执行、**六条全部评估不短路**。

与 E57 的区别（缺口是真的）：E57 的退出是「固定持有 H 日到期无条件平仓」，
**从没测过止盈止损**；而 E57 的头号失败机制正是 60 日反转 —— 止盈规则在定义上能拦掉它。

先验申明 **30/70（不看好）**：18 格参数网格架在一个只有约 14 个独立事件的信号上是
p-hacking 温床，故判据一律看**整片网格**的中位数与为正格数，不看任何单格最优。

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
from e57_bias_top3_leg import UNIVERSE, causal_topk, eps_count, load  # noqa: E402

K = 3
TPS = (0.05, 0.10, 0.15)
SLS = (0.03, 0.05, 0.08)
MAXHS = (20, 60)
COST = 0.0006                 # 往返 0.06%（单边 0.03% ETF 佣金，无印花税）
WARMS = (120, 250, 500)
WARM_MAIN = 500
MAS = (20, 60, 120)


def trades(c: np.ndarray, idx: list[int], tp: float, sl: float, maxh: int,
           cost: float) -> list[dict]:
    """逐笔：次一交易日收盘买入 → 收盘价触发止盈/止损/到期 → 次一交易日收盘卖出。

    盘中不可见，故一律用收盘价判定、次日收盘成交（与进场同口径，无前视）。
    持仓期内再触发不加仓（`busy_until`）。
    """
    out: list[dict] = []
    busy = -1
    for i in idx:
        if i <= busy:
            continue
        e = i + 1                                   # 进场日（次一交易日收盘）
        if e >= len(c):
            break
        px = c[e]
        exit_j, why = None, None
        for j in range(e + 1, min(e + 1 + maxh, len(c))):
            r = c[j] / px - 1
            if r >= tp:
                exit_j, why = j, "止盈"
                break
            if r <= -sl:
                exit_j, why = j, "止损"
                break
        if exit_j is None:
            exit_j = min(e + maxh, len(c) - 1)
            why = "到期"
        s = min(exit_j + 1, len(c) - 1)             # 次一交易日收盘卖出
        ret = c[s] / px - 1 - cost
        out.append(dict(entry=e, exit=s, hold=s - e, ret=ret, why=why))
        busy = s
    return out


def grid_stats(c: np.ndarray, idx: list[int], cost: float) -> pd.DataFrame:
    rows = []
    for tp in TPS:
        for sl in SLS:
            for mh in MAXHS:
                t = trades(c, idx, tp, sl, mh, cost)
                if not t:
                    rows.append(dict(tp=tp, sl=sl, maxh=mh, n=0, mean=np.nan,
                                     win=np.nan, pf=np.nan, hold=np.nan))
                    continue
                r = np.array([x["ret"] for x in t])
                w = r > 0
                gain = r[w].mean() if w.any() else 0.0
                loss = -r[~w].mean() if (~w).any() else np.nan
                rows.append(dict(tp=tp, sl=sl, maxh=mh, n=len(t), mean=float(r.mean()),
                                 win=float(w.mean()),
                                 pf=(gain / loss if loss and loss == loss else np.nan),
                                 hold=float(np.mean([x["hold"] for x in t]))))
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    a = ap.parse_args()
    root = Path(a.data)

    D = {}
    for nm, f, col, oos in UNIVERSE:
        d = load(root, f, col)
        c = d.c.to_numpy(dtype=float)
        D[nm] = dict(dates=d.trade_date.to_numpy(), c=c, oos=oos,
                     bias={w: (pd.Series(c) / pd.Series(c).rolling(w).mean() - 1).to_numpy()
                           for w in MAS})

    print("=" * 112)
    print("E58 —— 乖离率低尾前三 + 止盈止损 小仓位 sleeve（P68）｜判据 2026-08-05 跑数前写死")
    print(f"网格 TP{[f'{x:.0%}' for x in TPS]} × SL{[f'{x:.0%}' for x in SLS]} × "
          f"MAXH{MAXHS} = {len(TPS)*len(SLS)*len(MAXHS)} 格　往返成本 {COST:.2%}")
    print("★ = 从未参与调参的样本外对照")
    print("=" * 112)

    # 触发日
    TRIG = {}
    for nm in D:
        z = D[nm]
        b = z["bias"][60]
        i0 = int(np.argmax(~np.isnan(b))) + WARM_MAIN
        hit = causal_topk(b, K, "low", WARM_MAIN)
        TRIG[nm] = [i for i in np.where(hit)[0] if i >= i0]

    # ── 主表：逐指数的整片网格 ────────────────────────────────
    G = {}
    print(f"\n{'指数':>9s}{'触发日':>7s}{'成交笔数':>9s}{'每笔均值中位':>13s}{'为正格数':>9s}"
          f"{'胜率中位':>9s}{'盈亏比中位':>11s}{'平均持有':>9s}")
    for nm in D:
        g = grid_stats(D[nm]["c"], TRIG[nm], COST)
        G[nm] = g
        v = g.dropna(subset=["mean"])
        star = "★" if D[nm]["oos"] else " "
        if v.empty:
            print(f"{star}{nm:>8s}{len(TRIG[nm]):>7d}{'—':>9s}{'—':>13s}{'—':>9s}"
                  f"{'—':>9s}{'—':>11s}{'—':>9s}")
            continue
        print(f"{star}{nm:>8s}{len(TRIG[nm]):>7d}{int(v.n.median()):>9d}"
              f"{v['mean'].median():>+13.2%}{f'{(v[chr(109)+chr(101)+chr(97)+chr(110)]>0).sum()}/{len(v)}':>9s}"
              f"{v['win'].median():>9.0%}{v['pf'].median():>11.2f}{v['hold'].median():>9.0f}")

    # ── 合并口径（7 指数所有笔一起）───────────────────────────
    print(f"\n  ── 合并口径：18 格逐格（7 指数所有成交笔合并）──")
    print(f"  {'TP':>5s}{'SL':>5s}{'MAXH':>6s}{'笔数':>6s}{'每笔均值':>10s}{'胜率':>7s}"
          f"{'盈亏比':>8s}{'平均持有':>9s}{'止盈/止损/到期':>16s}")
    cells = []
    for tp in TPS:
        for sl in SLS:
            for mh in MAXHS:
                allt = []
                for nm in D:
                    allt += trades(D[nm]["c"], TRIG[nm], tp, sl, mh, COST)
                if not allt:
                    continue
                r = np.array([x["ret"] for x in allt])
                w = r > 0
                gain = r[w].mean() if w.any() else 0.0
                loss = -r[~w].mean() if (~w).any() else np.nan
                cw = {k: sum(1 for x in allt if x["why"] == k) for k in ("止盈", "止损", "到期")}
                cells.append(dict(tp=tp, sl=sl, maxh=mh, n=len(allt), mean=float(r.mean()),
                                  win=float(w.mean()),
                                  pf=(gain / loss if loss and loss == loss else np.nan)))
                print(f"  {tp:>5.0%}{sl:>5.0%}{mh:>6d}{len(allt):>6d}{r.mean():>+10.2%}"
                      f"{w.mean():>7.0%}{(gain/loss if loss and loss==loss else np.nan):>8.2f}"
                      f"{np.mean([x['hold'] for x in allt]):>9.0f}"
                      f"{f'{cw[chr(27490)+chr(30408)]}/{cw[chr(27490)+chr(25439)]}/{cw[chr(21040)+chr(26399)]}':>16s}")
    C = pd.DataFrame(cells)

    # ── 判据逐条 ───────────────────────────────────────────
    print("\n" + "=" * 112)
    print("判据逐条评估（六条全部评估、不短路）")
    print("=" * 112)

    c1 = (C["mean"].median() >= 0.02) and ((C["mean"] > 0).sum() >= 0.70 * len(C))
    print(f"  ① 整片网格为正：每笔均值中位 {C['mean'].median():+.2%}（要求 ≥+2.0%）"
          f"· 为正格数 {(C['mean'] > 0).sum()}/{len(C)}（要求 ≥{int(np.ceil(0.7*len(C)))}）"
          f" ⟹ {'✅' if c1 else '❌'}")

    oos_cells = []
    for tp in TPS:
        for sl in SLS:
            for mh in MAXHS:
                allt = []
                for nm in D:
                    if D[nm]["oos"]:
                        allt += trades(D[nm]["c"], TRIG[nm], tp, sl, mh, COST)
                if allt:
                    oos_cells.append(np.mean([x["ret"] for x in allt]))
    O = np.array(oos_cells)
    c2 = len(O) > 0 and (np.median(O) >= 0.01) and ((O > 0).sum() >= 0.70 * len(O))
    print(f"  ② 样本外不塌（上证50/中证500/中证1000）：中位 {np.median(O):+.2%}（要求 ≥+1.0%）"
          f"· 为正格数 {(O > 0).sum()}/{len(O)}（要求 ≥{int(np.ceil(0.7*len(O)))}）"
          f" ⟹ {'✅' if c2 else '❌'}")

    exp_pos = ((C["win"] * C["pf"] - (1 - C["win"])) > 0)
    c3 = (C["win"].median() >= 0.55) and bool(exp_pos.median())
    print(f"  ③ 胜率：全网格胜率中位 {C['win'].median():.0%}（要求 ≥55%）"
          f"· 盈亏比中位 {C['pf'].median():.2f}"
          f"· 期望为正的格数 {int(exp_pos.sum())}/{len(C)} ⟹ {'✅' if c3 else '❌'}")

    # ④ 相对固定持有 20 日有增量
    n_better = 0
    det = []
    for nm in D:
        c = D[nm]["c"]
        fixed = []
        busy = -1
        for i in TRIG[nm]:
            if i <= busy:
                continue
            e = i + 1
            s = min(e + 20, len(c) - 1)
            if e < len(c):
                fixed.append(c[s] / c[e] - 1 - COST)
                busy = s
        g = G[nm].dropna(subset=["mean"])
        if not fixed or g.empty:
            det.append(f"{nm}—")
            continue
        better = g["mean"].median() > np.mean(fixed)
        n_better += better
        det.append(f"{nm} {g['mean'].median():+.1%} vs 固定 {np.mean(fixed):+.1%}"
                   f"{'✅' if better else '❌'}")
    c4 = n_better >= 5
    print(f"  ④ 止盈止损相对固定持有 20 日有增量：{n_better}/7（要求 ≥5）⟹ {'✅' if c4 else '❌'}")
    print(f"     {' ｜ '.join(det)}")

    months, after2015 = set(), 0
    for nm in D:
        dts = D[nm]["dates"]
        months |= {str(dts[i])[:6] for i in TRIG[nm]}
        after2015 += eps_count([i for i in TRIG[nm] if str(dts[i]) >= "20160101"], 20)
    c5a = len(months) >= 10
    c5b = after2015 >= 3
    print(f"  ⑤ 事件充分：自然月去重后 {len(months)} 个（要求 ≥10）⟹ {'✅' if c5a else '❌'}"
          f"　｜　**2015 年后 7 指数合计触发 {after2015} 次**（<3 则只能提示、不得配资金）"
          f" ⟹ {'✅可配资金' if c5b else '❌只能提示'}")

    print("  ⑥ 稳健：")
    warm_med = []
    for w in WARMS:
        allc = []
        for tp in TPS:
            for sl in SLS:
                for mh in MAXHS:
                    allt = []
                    for nm in D:
                        b = D[nm]["bias"][60]
                        i0 = int(np.argmax(~np.isnan(b))) + w
                        idx = [i for i in np.where(causal_topk(b, K, "low", w))[0] if i >= i0]
                        allt += trades(D[nm]["c"], idx, tp, sl, mh, COST)
                    if allt:
                        allc.append(np.mean([x["ret"] for x in allt]))
        warm_med.append(np.median(allc) if allc else np.nan)
    ma_med = []
    for w in MAS:
        allc = []
        for tp in TPS:
            for sl in SLS:
                for mh in MAXHS:
                    allt = []
                    for nm in D:
                        b = D[nm]["bias"][w]
                        i0 = int(np.argmax(~np.isnan(b))) + WARM_MAIN
                        idx = [i for i in np.where(causal_topk(b, K, "low", WARM_MAIN))[0]
                               if i >= i0]
                        allt += trades(D[nm]["c"], idx, tp, sl, mh, COST)
                    if allt:
                        allc.append(np.mean([x["ret"] for x in allt]))
        ma_med.append(np.median(allc) if allc else np.nan)
    nocost = []
    for tp in TPS:
        for sl in SLS:
            for mh in MAXHS:
                allt = []
                for nm in D:
                    allt += trades(D[nm]["c"], TRIG[nm], tp, sl, mh, 0.0)
                if allt:
                    nocost.append(np.mean([x["ret"] for x in allt]))
    signs = {int(np.sign(x)) for x in warm_med if x == x}
    c6 = (len(signs) <= 1) and (len({int(np.sign(x)) for x in ma_med if x == x}) <= 2) \
        and (np.sign(np.median(nocost)) == np.sign(C["mean"].median()))
    print(f"     排名预热 {WARMS} 中位 {[f'{x:+.2%}' for x in warm_med]}（不变号？"
          f"{'✅' if len(signs) <= 1 else '❌'}）")
    print(f"     MA 窗口 {MAS} 中位 {[f'{x:+.2%}' for x in ma_med]}")
    print(f"     不含成本中位 {np.median(nocost):+.2%} vs 含成本 {C['mean'].median():+.2%}"
          f"（结论一致？{'✅' if np.sign(np.median(nocost)) == np.sign(C['mean'].median()) else '❌'}）")
    print(f"     ⟹ ⑥ {'✅' if c6 else '❌'}")

    # ── 裁决 ──────────────────────────────────────────────
    print("\n" + "=" * 112)
    print("E58 裁决")
    print("=" * 112)
    print(f"  ①{'✅' if c1 else '❌'} ②{'✅' if c2 else '❌'} ③{'✅' if c3 else '❌'} "
          f"④{'✅' if c4 else '❌'} ⑤事件{'✅' if c5a else '❌'}/近十年{'✅' if c5b else '❌'} "
          f"⑥{'✅' if c6 else '❌'}")
    if c1 and c2 and c3 and c4:
        if c5b:
            print("  ⟹ **①②③④全过且 2015 年后触发 ≥3 → 可考虑小 sleeve（S ≤ 10%），走高置信直升评估。**")
        else:
            print("  ⟹ **①②③④全过但 2015 年后触发 <3 → 只进提示行，不配资金**（判据已写死此分支）。")
    else:
        bad = [n for n, ok in (("①", c1), ("②", c2), ("③", c3), ("④", c4)) if not ok]
        print(f"  ⟹ **FAIL**：{'、'.join(bad)} 不过 ⟹ 不接入生产，入库负结果。")


if __name__ == "__main__":
    main()
