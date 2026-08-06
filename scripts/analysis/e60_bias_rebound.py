# -*- coding: utf-8 -*-
"""E60 —— 乖离率极值**短线搏反弹**腿（持有 1~10 日）·P70。

owner 2026-08-06：「我说的并不是拿 60 天，而是搏个反弹，哪怕只拿一天。」

判据 **跑数前写死于 `docs/model_change_proposals.md` P70 段**（判据先单独提交、脚本后写，
git 可查顺序），本脚本逐条执行、**六条全部评估不短路**。

腿：`z=(bias60−μ)/σ`（expanding，只用 ≤t 信息）≤ −Z_in → T+1 收盘买入 → 持有 N 日 →
收盘无条件平仓。不设止盈止损（E58 已证止盈线比信号时间尺度短会砍掉右尾）。
持仓期间不重复触发（非重叠）。

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

ZS = (1.5, 2.0, 2.5, 3.0)
NS = (1, 2, 3, 5, 10)
Z_MAIN, WARM_MAIN, MA_MAIN = 2.0, 750, 60
WARMS, MAS, COSTS = (250, 750, 1250), (20, 60, 120), (0.0005, 0.0010, 0.0015)
COST_MAIN = 0.0010          # 往返 10bp（单边 5bp）
CASH = 0.015                # 空仓现金 1.5%/年
EP_GAP = 20                 # episode 合并阈值（交易日）
HURDLE = 0.0030             # 判据①：含成本每笔 ≥ +0.30%
# 样本外分级（D2/D3 核验结论）：中证1000 有 45% 回溯段，记录但不计分
OOS_SCORED, OOS_NOTED = ("上证50", "中证500"), ("中证1000",)


def zscore(b: pd.Series, warm: int) -> np.ndarray:
    mu = b.expanding(min_periods=warm).mean()
    sd = b.expanding(min_periods=warm).std(ddof=1)
    return ((b - mu) / sd).to_numpy()


def run(c: np.ndarray, z: np.ndarray, dates: np.ndarray, zin: float, n: int,
        cost: float, extra: np.ndarray | None = None) -> dict:
    """非重叠短线腿：z≤−zin 触发 → T+1 收盘买 → 持有 n 日 → 收盘卖。"""
    N = len(c)
    trades, hold = [], np.zeros(N, bool)
    i = 0
    while i < N - n - 1:
        ok = z[i] == z[i] and z[i] <= -zin and (extra is None or bool(extra[i]))
        if not ok:
            i += 1
            continue
        e, x = i + 1, i + 1 + n                     # 买入日、卖出日
        if x >= N:
            break
        trades.append(dict(sig=str(dates[i]), entry=str(dates[e]), exit=str(dates[x]),
                           ret=c[x] / c[e] - 1.0 - cost, z=float(z[i])))
        hold[e:x + 1] = True
        i = x                                        # 非重叠：平仓后才可再触发
    if not trades:
        return dict(ntr=0, mean=np.nan, med=np.nan, win=np.nan, ann=np.nan,
                    ep=0, ep16=0, expo=0.0, trades=[])

    # 逐日净值：持仓吃指数收益，空仓吃现金
    nav = np.ones(N)
    for k in range(1, N):
        r = (c[k] / c[k - 1] - 1.0) if hold[k] else CASH / 250
        nav[k] = nav[k - 1] * (1 + r)
    for t in trades:                                 # 成本一次性扣在买入日
        nav[np.searchsorted(dates.astype(str), t["entry"]):] *= (1 - cost)
    i0 = int(np.argmax(z == z))                      # 首个 z 可算日
    yrs = (N - i0) / 250.0
    ann = (nav[-1] / nav[i0]) ** (1 / yrs) - 1 if yrs > 0 else np.nan

    sig = [int(np.searchsorted(dates.astype(str), t["sig"])) for t in trades]
    ep, last = 1, sig[0]
    ep16 = int(trades[0]["sig"] >= "20160101")
    for s, t in zip(sig[1:], trades[1:]):
        if s - last > EP_GAP:
            ep += 1
            ep16 += int(t["sig"] >= "20160101")
        last = s
    rets = np.array([t["ret"] for t in trades])
    return dict(ntr=len(trades), mean=float(rets.mean()), med=float(np.median(rets)),
                win=float((rets > 0).mean()), ann=float(ann), ep=ep, ep16=ep16,
                expo=float(hold[i0:].mean()), trades=trades)


def load_all(root: Path, ma: int = MA_MAIN, warm: int = WARM_MAIN) -> dict:
    D = {}
    for nm, _, _, _ in UNIVERSE:
        d = pd.read_csv(root / f"{nm}.csv", dtype={"trade_date": str})
        D[nm] = dict(c=d.close.to_numpy(float), dates=d.trade_date.to_numpy(),
                     z=zscore(d[f"bias{ma}"], warm), fear=d.fear.to_numpy(),
                     bias=d[f"bias{ma}"].to_numpy())
    return D


def grid(D: dict, cost: float = COST_MAIN) -> pd.DataFrame:
    rows = []
    for nm, d in D.items():
        for zin in ZS:
            for n in NS:
                r = run(d["c"], d["z"], d["dates"], zin, n, cost)
                rows.append(dict(nm=nm, zin=zin, n=n, **{k: v for k, v in r.items()
                                                         if k != "trades"}))
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results/bias_meanrev")
    a = ap.parse_args()
    root = Path(a.data)
    D = load_all(root)

    print("=" * 120)
    print("E60 —— 乖离率极值短线搏反弹（P70）｜判据 2026-08-06 跑数前写死")
    print(f"入场 z≤−{Z_MAIN}（expanding, WARM={WARM_MAIN}）· T+1 收盘买 · 持有 N 日 · "
          f"往返成本 {COST_MAIN:.2%} · 空仓现金 {CASH:.1%}/年 · 非重叠")
    print("★ = 样本外（上证50/中证500 计分；中证1000 有 45% 回溯段，记录不计分）")
    print("=" * 120)

    G = grid(D)
    M = G[G.zin == Z_MAIN]

    print(f"\n【主口径 z≤−{Z_MAIN}】含成本每笔平均收益")
    print(f"{'指数':>9s}" + "".join(f"{f'N={n}':>11s}" for n in NS) + f"{'触发笔数':>10s}")
    for nm, _, _, _ in UNIVERSE:
        s = M[M.nm == nm].set_index("n")
        star = "★" if nm in OOS_SCORED + OOS_NOTED else " "
        print(f"{star}{nm:>8s}" + "".join(
            f"{s.loc[n, 'mean']:>+11.2%}" if s.loc[n, "ntr"] > 0 else f"{'—':>11s}" for n in NS)
            + f"{int(s.loc[NS[0], 'ntr']):>10d}")

    print(f"\n【主口径】胜率 ｜ 年化贡献（该腿满仓、其余现金 {CASH:.1%}）")
    print(f"{'指数':>9s}" + "".join(f"{f'N={n}':>13s}" for n in NS)
          + f"{'episode':>9s}{'16年后':>8s}{'持仓占比':>9s}")
    for nm, _, _, _ in UNIVERSE:
        s = M[M.nm == nm].set_index("n")
        star = "★" if nm in OOS_SCORED + OOS_NOTED else " "
        print(f"{star}{nm:>8s}" + "".join(
            f"{s.loc[n, 'win']:>6.0%}/{s.loc[n, 'ann']:>6.1%}" if s.loc[n, "ntr"] > 0
            else f"{'—':>13s}" for n in NS)
            + f"{int(s.loc[NS[0], 'ep']):>9d}{int(s.loc[NS[0], 'ep16']):>8d}"
            + f"{s.loc[3, 'expo']:>9.1%}")

    # ── 判据 ──────────────────────────────────────────────
    print("\n" + "=" * 120)
    print("判据逐条评估（六条全部评估、不短路）")
    print("=" * 120)

    per_n = {n: int((M[M.n == n].set_index("nm").reindex([u[0] for u in UNIVERSE])
                     ["mean"] >= HURDLE).sum()) for n in NS}
    best_n = max(per_n, key=lambda k: per_n[k])
    c1 = per_n[best_n] >= 5
    print(f"  ① 含成本每笔 ≥ +{HURDLE:.2%} 的指数数：" +
          "　".join(f"N={n}→{v}/7" for n, v in per_n.items()) +
          f"　⟹ 最佳 N={best_n}（{per_n[best_n]}/7，要求 ≥5）{'✅' if c1 else '❌'}")

    oo = M[(M.n == best_n) & (M.nm.isin(OOS_SCORED))].set_index("nm")["mean"]
    c2 = bool((oo >= HURDLE).all())
    note = M[(M.n == best_n) & (M.nm.isin(OOS_NOTED))].set_index("nm")["mean"]
    print(f"  ② 样本外两条都达标：" + "｜".join(f"{k} {v:+.2%}" for k, v in oo.items())
          + f"　⟹ {'✅' if c2 else '❌'}"
          + "　（不计分：" + "｜".join(f"{k} {v:+.2%}" for k, v in note.items()) + "）")

    cells = G.groupby(["zin", "n"])["mean"].median()
    pos = int((cells > 0).sum())
    is_peak = bool(cells.idxmax() == (Z_MAIN, best_n))
    c3 = (pos / len(cells) >= 0.80) and not is_peak
    print(f"  ③ 网格 {pos}/{len(cells)} 格跨腿中位为正（要求 ≥80%＝{int(0.8*len(cells))} 格）"
          f"· 主口径格是否为全网格最优：{'是 ⟹ 判该条不过' if is_peak else '否'}"
          f"　⟹ {'✅' if c3 else '❌'}")

    ep = M[M.n == best_n].set_index("nm")
    c4 = bool((ep.ep >= 5).all() and (ep.ep16 >= 3).all())
    print(f"  ④ 每腿 episode ≥5（最少 {int(ep.ep.min())}）· 2016 年后每腿 ≥3"
          f"（最少 {int(ep.ep16.min())}）⟹ {'✅' if c4 else '❌'}")

    ann_ok = int((ep.ann > 0.02).sum())
    c5 = ann_ok >= 5
    print(f"  ⑤ 年化贡献 > 2.0%（货基机会成本）：{ann_ok}/7（要求 ≥5）⟹ {'✅' if c5 else '❌'}"
          f"　" + "｜".join(f"{k} {v:.1%}" for k, v in ep.ann.items()))

    print("  ⑥ 稳健：")
    sub = []
    for w in WARMS:
        gw = grid(load_all(root, MA_MAIN, w))
        v = int((gw[(gw.zin == Z_MAIN) & (gw.n == best_n)]["mean"] >= HURDLE).sum())
        sub.append(("WARM", w, v))
    for m in MAS:
        gm = grid(load_all(root, m, WARM_MAIN))
        v = int((gm[(gm.zin == Z_MAIN) & (gm.n == best_n)]["mean"] >= HURDLE).sum())
        sub.append(("MA", m, v))
    for cs in COSTS:
        gc = grid(D, cs)
        v = int((gc[(gc.zin == Z_MAIN) & (gc.n == best_n)]["mean"] >= HURDLE).sum())
        sub.append(("成本", f"{cs:.2%}", v))
    for lbl in ("WARM", "MA", "成本"):
        xs = [(k, v) for g, k, v in sub if g == lbl]
        print(f"     {lbl}：" + "　".join(f"{k}→{v}/7" for k, v in xs))
    warm_ok = all((v >= 5) == c1 for g, _, v in sub if g == "WARM")
    ma_ok = sum(1 for g, _, v in sub if g == "MA" and (v >= 5) == c1) >= 2
    cost_ok = sum(1 for g, _, v in sub if g == "成本" and (v >= 5) == c1) >= 2
    c6 = warm_ok and ma_ok and cost_ok
    print(f"     ⟹ ⑥ WARM 不变号 {'✅' if warm_ok else '❌'}·MA ≥2 档同向 "
          f"{'✅' if ma_ok else '❌'}·成本 ≥2 档同向 {'✅' if cost_ok else '❌'}"
          f" ⟹ {'✅' if c6 else '❌'}")

    # ── 恐慌臂（增量价值，同期同口径对照） ──────────────────
    print("\n" + "=" * 120)
    print(f"恐慌臂：低尾 + fear≥75 相对**同期同口径**低尾的增量（样本裁到 2015-01-05 起）")
    print("=" * 120)
    print(f"{'指数':>9s}{'同期低尾n':>10s}{'每笔':>9s}{'共振n':>7s}{'共振每笔':>10s}"
          f"{'增量':>9s}{'同期胜率':>9s}{'共振胜率':>9s}")
    for nm, d in D.items():
        hf = d["fear"] == d["fear"]
        base = run(d["c"], np.where(hf, d["z"], np.nan), d["dates"], Z_MAIN, best_n, COST_MAIN)
        res = run(d["c"], np.where(hf, d["z"], np.nan), d["dates"], Z_MAIN, best_n,
                  COST_MAIN, extra=(d["fear"] >= 75))
        if base["ntr"] == 0 or res["ntr"] == 0:
            print(f"{nm:>9s}{base['ntr']:>10d}{'—':>9s}{res['ntr']:>7d}{'—':>10s}"
                  f"{'—':>9s}{'—':>9s}{'—':>9s}")
            continue
        print(f"{nm:>9s}{base['ntr']:>10d}{base['mean']:>+9.2%}{res['ntr']:>7d}"
              f"{res['mean']:>+10.2%}{(res['mean']-base['mean'])*100:>+9.2f}"
              f"{base['win']:>9.0%}{res['win']:>9.0%}")

    # ── 探索（非判据·不可晋升） ────────────────────────────
    print("\n" + "=" * 120)
    print("⚠️ 探索：放宽入场阈值能不能补上判据④的触发频次？**非判据、不可作晋升依据**")
    print("=" * 120)
    print("  🔴 判据④是唯一没过的一条，而「等不到就放宽条件」正是 E58 判据⑤警告的参数漂移。")
    print("     本节只用来说明**为什么**④过不了，不是用来找一个能过的参数。")
    print(f"\n{'Z 阈值':>8s}{'总笔数':>8s}{'每笔中位':>10s}{'达标腿':>8s}"
          f"{'最少episode':>12s}{'16年后最少':>11s}{'④能否过':>9s}")
    for zin in ZS:
        g = G[(G.zin == zin) & (G.n == best_n)]
        ok4 = bool((g.ep >= 5).all() and (g.ep16 >= 3).all())
        print(f"{zin:>8.1f}{int(g.ntr.sum()):>8d}{g['mean'].median():>+10.2%}"
              f"{int((g['mean'] >= HURDLE).sum()):>8d}"
              f"{int(g.ep.min()):>12d}{int(g.ep16.min()):>11d}"
              f"{('过' if ok4 else '不过'):>9s}")
    print("\n  读法：阈值放宽会同时**抬高频次、压低每笔幅度**——这正是 E58 记过的"
          "「没有既有样本又有幅度的中间档」。")

    print("\n" + "=" * 120)
    print("E60 裁决")
    print("=" * 120)
    cs_ = [c1, c2, c3, c4, c5, c6]
    print("  " + " ".join(f"{i}{'✅' if v else '❌'}" for i, v in
                          zip("①②③④⑤⑥", cs_)))
    if not c4:
        print("  ⟹ **FAIL**：④样本不足或十年不响（判据写死：④不过即 FAIL）。")
    elif not c1:
        print("  ⟹ **FAIL**：①含成本每笔跑不赢交易摩擦（判据写死）。")
    elif c1 and c2 and c4 and c5:
        print("  ⟹ **PASS** → 走高置信直升评估（提示-only、owner 手动、零自动交易）。"
              if c3 and c6 else "  ⟹ **①②④⑤过而③/⑥不过 → 记知识库，不接入生产。**")
    else:
        print("  ⟹ **FAIL**：②/⑤ 未过。")


if __name__ == "__main__":
    main()
