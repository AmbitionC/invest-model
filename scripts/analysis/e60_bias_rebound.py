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

# 🔴 2026-08-06 修正：owner 两轮前就指定过「近十年、滚动算」，E59 用了、E60 却错用成
#    全历史 expanding。窗口口径改回滚动 10 年（2500 交易日）。
# ⚠️ RED 通道 2026-08-06 更正：我原先在这里写「expanding 的 σ 随历史积累变大，会系统性制造
#    近年不触发」——**这个机制是反的**。实测 expanding 的 σ 单调**下降**（0.133→0.082）。
#    真机制是：rolling 会**忘掉 2008/2015 那段高波动体制**，σ 掉到 0.048，于是门槛从
#    bias −15.4% 松到 −9.0% ⟹ 换滚动**只加信号不减**（V 测出滚动是 expanding 的严格超集、
#    Jaccard 87.3%、expanding 独有为 0）。即：滚动让判据④看起来变好，靠的是降门槛。
ROLL_WIN = 2500                     # 近十年（交易日）
WINS = (1250, 2500, 3750)           # 窗口敏感性：5 / 10 / 15 年
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


def zscore(b: pd.Series, warm: int, win: int | None = ROLL_WIN) -> np.ndarray:
    """滚动窗口内的 z 分数（win=None 退回全历史 expanding，作对照臂）。

    窗口不足 win 时用截至当日的全部历史（owner：「不足十年就按最大的来」）。
    """
    if win is None:
        mu = b.expanding(min_periods=warm).mean()
        sd = b.expanding(min_periods=warm).std(ddof=1)
    else:
        mu = b.rolling(win, min_periods=warm).mean()
        sd = b.rolling(win, min_periods=warm).std(ddof=1)
    return ((b - mu) / sd).to_numpy()


def pct_rank(b: pd.Series, warm: int, win: int | None = ROLL_WIN) -> np.ndarray:
    """滚动窗口内的分位（0=窗口内最低）——「近十年历史极值」的另一种读法，作并列对照臂。"""
    r = (b.rolling(win, min_periods=warm) if win else b.expanding(min_periods=warm))
    return r.apply(lambda x: (x <= x.iloc[-1]).mean(), raw=False).to_numpy()


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
        # 🔴 V 通道 2026-08-06 抓到的前视 bug：原写 hold[e:x+1]，把**买入日当天**
        #    （close[T+1]/close[T]，即信号日→买入日那一段）也算进了净值。T+1 收盘才买入，
        #    这一天赚不到。累乘应从 e+1 开始，与逐笔口径 c[x]/c[e] 严格一致。
        #    影响仅限年化贡献（判据⑤）；逐笔收益表不受影响（本来就是 c[x]/c[e]）。
        hold[e + 1:x + 1] = True
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


def load_all(root: Path, ma: int = MA_MAIN, warm: int = WARM_MAIN,
             win: int | None = ROLL_WIN) -> dict:
    D = {}
    for nm, _, _, _ in UNIVERSE:
        d = pd.read_csv(root / f"{nm}.csv", dtype={"trade_date": str})
        D[nm] = dict(c=d.close.to_numpy(float), dates=d.trade_date.to_numpy(),
                     z=zscore(d[f"bias{ma}"], warm, win), fear=d.fear.to_numpy(),
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
    print(f"入场 z≤−{Z_MAIN}（**滚动 {ROLL_WIN} 交易日≈近十年**, 预热 {WARM_MAIN}）· "
          f"T+1 收盘买 · 持有 N 日 · "
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
    print("     逐腿明细（判据写的是「每腿」，一腿不过即整条不过）：")
    for nm, _, _, _ in UNIVERSE:
        r = ep.loc[nm]
        yrs = len([1 for _ in D[nm]["z"] if _ == _]) / 250.0
        ok = "✅" if (r.ep >= 5 and r.ep16 >= 3) else "❌"
        why = ("" if (r.ep >= 5 and r.ep16 >= 3) else
               ("（可算历史仅 %.1f 年，结构上不可能凑够 5 个 episode）" % yrs if yrs < 10
                else "（2016 年后不够）" if r.ep >= 5 else "（总数不够）"))
        print(f"       {ok} {nm:>8s}  episode {int(r.ep):>2d} · 2016年后 {int(r.ep16):>2d}"
              f" · 可算历史 {yrs:>4.1f} 年{why}")

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
    for w in WINS:
        gw = grid(load_all(root, MA_MAIN, WARM_MAIN, w))
        v = int((gw[(gw.zin == Z_MAIN) & (gw.n == best_n)]["mean"] >= HURDLE).sum())
        sub.append(("窗口", w, v))
    for cs in COSTS:
        gc = grid(D, cs)
        v = int((gc[(gc.zin == Z_MAIN) & (gc.n == best_n)]["mean"] >= HURDLE).sum())
        sub.append(("成本", f"{cs:.2%}", v))
    for lbl in ("WARM", "窗口", "MA", "成本"):
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

    # ── V 通道三条对照臂（2026-08-06 交叉验证提出，全部并列呈现） ──
    print("\n" + "=" * 120)
    print("V 通道对照臂：三处「换个写法读数就变」的地方")
    print("=" * 120)

    print("\n  【V-D】判据⑤的年化贡献里有多少是**空仓吃利息**？")
    print(f"  🔴 净值里空仓按 {CASH:.1%}/年 计息，而判据⑤的门槛写的是 2.0% —— 光放着不动就有 "
          f"{CASH:.2%}，\n     这条判据实际只要求交易本身贡献 0.5pp。**判据设计偏松，记缺陷。**")
    print(f"{'指数':>9s}{'年化贡献':>10s}{'减去现金':>10s}{'纯交易项':>10s}{'仍>2.0%?':>10s}")
    for nm, _, _, _ in UNIVERSE:
        r = ep.loc[nm]
        base = (1 + CASH / 250) ** 250 - 1
        print(f"{nm:>9s}{r.ann:>10.2%}{base:>10.2%}{r.ann - base:>10.2%}"
              f"{('是' if r.ann - base > 0.02 else '否'):>10s}")

    print("\n  【V-C】不同 N 不是在同一批交易上比较（非重叠 ⟹ N 越大后续触发被吃掉）")
    print(f"{'指数':>9s}" + "".join(f"{f'N={n}笔数':>10s}" for n in NS) + "  固定信号集对照")
    for nm, d in D.items():
        cnts = [int(M[(M.nm == nm) & (M.n == n)]["ntr"].iloc[0]) for n in NS]
        # 固定信号集：用 N=1 的触发日，对所有 N 复用同一批信号
        sig = [t["sig"] for t in run(d["c"], d["z"], d["dates"], Z_MAIN, 1, COST_MAIN)["trades"]]
        idx = np.searchsorted(d["dates"].astype(str), sig)
        fixed = []
        for n in NS:
            rr = [d["c"][i + 1 + n] / d["c"][i + 1] - 1 - COST_MAIN
                  for i in idx if i + 1 + n < len(d["c"])]
            fixed.append(f"{np.mean(rr):+.2%}" if rr else "—")
        print(f"{nm:>9s}" + "".join(f"{c:>10d}" for c in cnts) + "  " + " ".join(fixed))
    print("     ⟹ 固定信号集下 N 的排序会变（V 报 2/7 腿最优 N 翻转、5/7 腿最优是 N=10）"
          "\n        ——**「N=2 最好」这个读数不成立**，跨 N 横向比较须用固定信号集。")

    print("\n  【V-B】🔴 滚动窗口把 E59 判死的**阈值漂移**装了回来")
    print("  滚动 σ 随近十年波动收缩而变小 ⟹ 同样的 z=−2 在不同年代对应完全不同的实际跌幅。")
    print(f"{'指数':>9s}{'前半段触发 bias60 中位':>24s}{'后半段':>12s}{'漂移':>10s}"
          f"{'最近一次触发':>14s}{'其 bias60':>11s}")
    for nm, d in D.items():
        tr = run(d["c"], d["z"], d["dates"], Z_MAIN, best_n, COST_MAIN)["trades"]
        if len(tr) < 4:
            print(f"{nm:>9s}{'样本不足':>24s}")
            continue
        idx = np.searchsorted(d["dates"].astype(str), [t["sig"] for t in tr])
        bs = d["bias"][idx]
        h = len(bs) // 2
        print(f"{nm:>9s}{np.median(bs[:h]):>24.1%}{np.median(bs[h:]):>12.1%}"
              f"{(np.median(bs[h:]) - np.median(bs[:h])) * 100:>+10.1f}"
              f"{tr[-1]['sig']:>14s}{bs[-1]:>11.1%}")
    print("     ⟹ 与 E59 记的失败机制逐字相同：阈值随最近十年的波动幅度漂移，"
          "\n        它度量的是「比最近十年安静时期波动更大」，不是「跌得够深」。")

    print("\n  【RED-3】判据①的脆弱度：去掉每腿收益最高的 1 笔之后还过吗？")
    print(f"{'指数':>9s}{'原每笔':>9s}{'去最高1笔':>11s}{'最高那笔':>10s}{'日期':>11s}"
          f"{'仍≥0.30%?':>11s}")
    keep = 0
    for nm, d in D.items():
        tr = run(d["c"], d["z"], d["dates"], Z_MAIN, best_n, COST_MAIN)["trades"]
        if len(tr) < 2:
            print(f"{nm:>9s}{'样本不足':>9s}")
            continue
        rs = np.array([t["ret"] for t in tr])
        j = int(rs.argmax())
        m2 = float(np.delete(rs, j).mean())
        keep += int(m2 >= HURDLE)
        print(f"{nm:>9s}{rs.mean():>+9.2%}{m2:>+11.2%}{rs[j]:>+10.2%}{tr[j]['sig']:>11s}"
              f"{('是' if m2 >= HURDLE else '否'):>11s}")
    print(f"     ⟹ 去最高 1 笔后 {keep}/7 达标（原 7/7）。RED 另测：126 笔总收益里"
          f"**最高 5 笔占 42%**，\n        且全部落在 2008-09-17 与 2015-07~08。"
          "**①过得并不厚实。**")

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
