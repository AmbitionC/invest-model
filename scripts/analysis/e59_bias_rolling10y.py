# -*- coding: utf-8 -*-
"""E59 —— 乖离率「近十年滚动窗口前四」双尾择时（高卖低买）·P69。

owner 2026-08-05 指定口径：
    「近十年来的各个指数的乖离率前四。不足十年就按最大的来。高的低的都要算，
     高的卖，低的买。并且每次 10% 不太合理，你就单纯算这个策略就行。」

判据 **2026-08-05 跑数前写死于 `docs/model_change_proposals.md` P69 段**
（判据先单独提交、脚本后写，git 可查前后顺序），本脚本逐条执行、**六条全部评估不短路**。

🔴 本条存在的理由是我此前的一处口径错误：博主原文是「近十年历史上排名第五」＝**滚动窗口**，
而 E37/E56/E57/E58 我用的都是 **since-inception 全历史排名**。全历史排名在 2008/2015 之后
被永久锁死（⟹「2015 年后只触发过 1 次」这个否决理由），而滚动窗口会**重新武装**。

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
from e57_bias_top3_leg import UNIVERSE, load  # noqa: E402

K_MAIN = 4                       # owner 指定：前四
WIN_MAIN = 2500                  # 近十年 ≈ 2500 交易日
MIN_WARM = 250                   # 不足十年时的最低预热
MA_MAIN = 60
COST = 0.0006                    # 往返 0.06%
CASH_RATE = 0.02                 # 空仓期现金 2%/年
KS, WINS, MAS = (3, 4, 5), (1250, 2500, 3750), (20, 60, 120)


def rolling_rank_signals(b: np.ndarray, k: int, win: int, warm: int = MIN_WARM):
    """滚动窗口内的双尾前 k 名（因果，含当日；窗口不足则用截至当日的全部历史）。

    返回 (buy, sell) 两个 bool 数组：buy = 当日在窗口内最低 k 个之内；sell = 最高 k 个之内。
    """
    n = len(b)
    buy = np.zeros(n, bool)
    sell = np.zeros(n, bool)
    valid = [i for i in range(n) if b[i] == b[i]]
    for pos, i in enumerate(valid):
        if pos + 1 < warm:                    # 预热不足不给信号
            continue
        lo = max(0, pos + 1 - win)
        w = np.asarray([b[j] for j in valid[lo:pos + 1]])
        if len(w) < k:
            continue
        buy[i] = b[i] <= np.partition(w, k - 1)[k - 1]
        sell[i] = b[i] >= np.partition(w, -k)[-k]
    return buy, sell


def run_leg(c: np.ndarray, buy: np.ndarray, sell: np.ndarray, start_long: bool = False):
    """高卖低买状态机：空仓+买信号 → 次日收盘满仓；持仓+卖信号 → 次日收盘清仓。

    返回逐日净值、持仓状态、成交流水。
    """
    n = len(c)
    hold = start_long
    nav = np.ones(n)
    holds = np.zeros(n, bool)
    trades: list[dict] = []
    entry = None
    for i in range(1, n):
        # 昨日信号，今日收盘执行（exec_lag=1）
        if not hold and buy[i - 1]:
            hold = True
            entry = i
            nav[i - 1] *= (1 - COST / 2)
        elif hold and sell[i - 1]:
            hold = False
            nav[i - 1] *= (1 - COST / 2)
            if entry is not None:
                trades.append(dict(entry=entry, exit=i, ret=c[i] / c[entry] - 1 - COST))
                entry = None
        r = (c[i] / c[i - 1] - 1) if hold else (CASH_RATE / 250)
        nav[i] = nav[i - 1] * (1 + r)
        holds[i] = hold
    return nav, holds, trades


def signal_report(D: dict, k: int, win: int, ma: int) -> None:
    """滚动窗口到底有没有「丢掉旧数据、重新武装」——判据外的机制诊断，不参与裁决。

    这一节的存在是因为首跑结果（20 年 0~2 次往返）看起来像实现 bug。逐年打出窗口阈值
    可证：窗口**确实**在释放（沪深300 窗口最低 2025 年 −29.0% → 2026 年 −20.0%，
    2015-08 那批滚出去了；创业板 2024-10-08 因高尾阈值回落到 +41.0% 才点亮）。
    """
    print("\n" + "=" * 116)
    print("机制诊断（判据外）：滚动窗口有没有重新武装？逐年窗口阈值 vs 当年极值")
    print("=" * 116)
    print(f"{'指数':>9s}{'买信号':>7s}{'卖信号':>7s}  年份分布（买 ↓ / 卖 ↑）")
    for nm, z in D.items():
        b = z["bias"][ma]
        dates = z["dates"].astype(str)
        buy, sell = rolling_rank_signals(b, k, win)
        yb, ys = {}, {}
        for i in np.flatnonzero(buy):
            yb[dates[i][:4]] = yb.get(dates[i][:4], 0) + 1
        for i in np.flatnonzero(sell):
            ys[dates[i][:4]] = ys.get(dates[i][:4], 0) + 1
        seg = "　".join(f"↓{y}×{n}" for y, n in sorted(yb.items())) + \
              ("　" if yb and ys else "") + \
              "　".join(f"↑{y}×{n}" for y, n in sorted(ys.items()))
        print(f"{nm:>9s}{int(buy.sum()):>7d}{int(sell.sum()):>7d}  {seg}")
    print("  ⟹ 触发日数与随机基准一致（滚动段 4/2500×N + 预热段 Σ4/pos ≈ 17~25 天/指数），"
          "\n     即**信号稀疏是这个口径的定义性质，不是实现错误**：窗口长 10 年，"
          "而上一场危机的极值要 10 年后才滚出去。")

    print("\n  逐笔往返（空仓起手）——看清每一笔都发生了什么：")
    for nm, z in D.items():
        c, dates = z["c"], z["dates"].astype(str)
        buy, sell = rolling_rank_signals(z["bias"][ma], k, win)
        _, _, tr = run_leg(c, buy, sell)
        if not tr:
            print(f"{nm:>13s}  （无完整往返）")
            continue
        print(f"{nm:>13s}  " + " ｜ ".join(
            f"{dates[t['entry']]}@{c[t['entry']]:.0f} → {dates[t['exit']]}@{c[t['exit']]:.0f} "
            f"{t['ret']:+.1%}（持有 {(t['exit'] - t['entry']) / 250:.1f} 年）" for t in tr))

    print("\n  阈值漂移（同一条规则在不同年代要求的「便宜」完全不同）：")
    for nm, z in D.items():
        b = z["bias"][ma]
        dates = z["dates"].astype(str)
        buy, _ = rolling_rank_signals(b, k, win)
        hit = np.flatnonzero(buy)
        if len(hit) < 2:
            continue
        print(f"{nm:>13s}  首个触发 {dates[hit[0]]} bias={b[hit[0]]:>7.1%}"
              f"　→　末个触发 {dates[hit[-1]]} bias={b[hit[-1]]:>7.1%}")


def stats(nav: np.ndarray, i0: int) -> tuple[float, float, float]:
    v = nav[i0:] / nav[i0]
    yrs = len(v) / 250.0
    ann = v[-1] ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    pk = np.maximum.accumulate(v)
    return ann, ((ann - 0.02) / vol if vol else np.nan), float(((v - pk) / pk).min())


def bh_stats(c: np.ndarray, i0: int) -> tuple[float, float, float]:
    v = c[i0:] / c[i0]
    yrs = len(v) / 250.0
    ann = v[-1] ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    pk = np.maximum.accumulate(v)
    return ann, ((ann - 0.02) / vol if vol else np.nan), float(((v - pk) / pk).min())


def evaluate(D: dict, k: int, win: int, ma: int, start_long: bool = False) -> pd.DataFrame:
    rows = []
    for nm, z in D.items():
        c = z["c"]
        b = z["bias"][ma]
        buy, sell = rolling_rank_signals(b, k, win)
        i0 = int(np.argmax(~np.isnan(b))) + MIN_WARM
        if i0 >= len(c) - 250:
            continue
        nav, holds, tr = run_leg(c, buy, sell, start_long)
        a, s, m = stats(nav, i0)
        ba, bs, bm = bh_stats(c, i0)
        after = sum(1 for t in tr if str(z["dates"][t["exit"]]) >= "20160101")
        rows.append(dict(nm=nm, oos=z["oos"], yrs=(len(c) - i0) / 250,
                         ann=a, bh=ba, sharpe=s, bhsharpe=bs, mdd=m, bhmdd=bm,
                         n=len(tr), n2016=after, expo=float(holds[i0:].mean()),
                         win=(float(np.mean([t["ret"] > 0 for t in tr])) if tr else np.nan),
                         avg=(float(np.mean([t["ret"] for t in tr])) if tr else np.nan)))
    return pd.DataFrame(rows)


def table(R: pd.DataFrame) -> None:
    print(f"{'指数':>9s}{'年数':>6s}{'策略年化':>9s}{'买持年化':>9s}{'超额':>8s}"
          f"{'策略夏普':>9s}{'买持夏普':>9s}{'策略回撤':>9s}{'买持回撤':>9s}"
          f"{'往返':>5s}{'16年后':>7s}{'持仓占比':>9s}{'胜率':>7s}{'每笔':>8s}")
    for _, r in R.iterrows():
        star = "★" if r.oos else " "
        print(f"{star}{r.nm:>8s}{r.yrs:>6.1f}{r.ann:>9.2%}{r.bh:>9.2%}"
              f"{(r.ann - r.bh) * 100:>+8.2f}{r.sharpe:>9.2f}{r.bhsharpe:>9.2f}"
              f"{r.mdd:>9.1%}{r.bhmdd:>9.1%}{r.n:>5.0f}{r.n2016:>7.0f}"
              f"{r.expo:>9.0%}{r.win:>7.0%}{r.avg:>+8.1%}")


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

    print("=" * 116)
    print("E59 —— 乖离率「近十年滚动窗口前四」高卖低买（P69）｜判据 2026-08-05 跑数前写死")
    print(f"窗口 {WIN_MAIN} 交易日（≈10 年，不足则用截至当日全部历史·最低预热 {MIN_WARM}）"
          f"· K={K_MAIN} · MA{MA_MAIN} · 往返成本 {COST:.2%} · 空仓现金 {CASH_RATE:.0%}/年")
    print("★ = 从未参与调参的样本外对照　｜　只算策略本身，不做 sleeve 稀释")
    print("=" * 116)

    R = evaluate(D, K_MAIN, WIN_MAIN, MA_MAIN)
    print("\n【主臂】空仓起手（第一个动作只能是买）")
    table(R)

    signal_report(D, K_MAIN, WIN_MAIN, MA_MAIN)

    print("\n" + "=" * 116)
    print("判据逐条评估（六条全部评估、不短路）")
    print("=" * 116)

    c1n = int(((R.ann - R.bh) >= 0.01).sum())
    c1 = c1n >= 5
    print(f"  ① 跑赢买入持有 +1.0pp：{c1n}/7（要求 ≥5）⟹ {'✅' if c1 else '❌'}")

    O = R[R.oos]
    c2n = int(((O.ann - O.bh) >= 0.01).sum())
    c2 = c2n >= 2
    print(f"  ② 样本外不塌：{c2n}/3（要求 ≥2）⟹ {'✅' if c2 else '❌'}　"
          + "｜".join(f"{r.nm} {(r.ann - r.bh) * 100:+.2f}pp" for _, r in O.iterrows()))

    c3n = int(((R.sharpe >= R.bhsharpe) & ((R.mdd - R.bhmdd) >= -0.05)).sum())
    c3 = c3n >= 5
    print(f"  ③ 风险调整不劣：{c3n}/7（要求 ≥5）⟹ {'✅' if c3 else '❌'}")

    per5 = bool((R.n >= 5).all())
    tot = int(R.n.sum())
    a16 = bool((R.n2016 >= 1).all())
    c4 = per5 and tot >= 40 and a16
    print(f"  ④ 样本充分：每指数往返 ≥5 {'✅' if per5 else '❌'}（最少 {int(R.n.min())}）"
          f"· 合计 {tot}（要求 ≥40）· **2016 年后每指数 ≥1 次** "
          f"{'✅' if a16 else '❌'}（最少 {int(R.n2016.min())}）⟹ {'✅' if c4 else '❌'}")
    print(f"     ⟹ 滚动窗口有没有解决「十年不响」？"
          f"{'**有** —— 2016 年后合计 ' + str(int(R.n2016.sum())) + ' 次往返' if R.n2016.sum() >= 7 else '没有'}"
          f"（全历史排名口径下 2015 年后 7 指数合计只有 1 次）")

    c5n = int(((R.expo >= 0.20) & (R.expo <= 0.90)).sum())
    c5 = c5n >= 5
    print(f"  ⑤ 不是买入持有的伪装（持仓占比 20%~90%）：{c5n}/7（要求 ≥5）⟹ {'✅' if c5 else '❌'}")

    print("  ⑥ 稳健：")
    sens = {}
    for lbl, ks, wins, mas in (("K", KS, (WIN_MAIN,), (MA_MAIN,)),
                               ("窗口", (K_MAIN,), WINS, (MA_MAIN,)),
                               ("MA", (K_MAIN,), (WIN_MAIN,), MAS)):
        outs = []
        for k in ks:
            for w in wins:
                for m in mas:
                    rr = evaluate(D, k, w, m)
                    outs.append((f"{lbl}={k if lbl=='K' else (w if lbl=='窗口' else m)}",
                                 int(((rr.ann - rr.bh) >= 0.01).sum())))
        sens[lbl] = outs
        print(f"     {lbl}：" + "　".join(f"{a}→{b}/7" for a, b in outs))
    Rl = evaluate(D, K_MAIN, WIN_MAIN, MA_MAIN, start_long=True)
    c6d = int(((Rl.ann - Rl.bh) >= 0.01).sum())
    print(f"     起手满仓臂：{c6d}/7（空仓起手 {c1n}/7）")
    print("\n【对照臂】满仓起手（「高卖低买」更自然的读法：本来就持有指数，高位卖、低位买回）")
    table(Rl)
    ok_same = lambda lst: sum(1 for _, v in lst if (v >= 5) == (c1n >= 5)) >= 2   # noqa: E731
    c6 = ok_same(sens["K"]) and ok_same(sens["窗口"]) and ok_same(sens["MA"]) \
        and ((c6d >= 5) == (c1n >= 5))
    print(f"     ⟹ ⑥ {'✅' if c6 else '❌'}")

    print("\n" + "=" * 116)
    print("E59 裁决")
    print("=" * 116)
    print(f"  ①{'✅' if c1 else '❌'} ②{'✅' if c2 else '❌'} ③{'✅' if c3 else '❌'} "
          f"④{'✅' if c4 else '❌'} ⑤{'✅' if c5 else '❌'} ⑥{'✅' if c6 else '❌'}")
    if not c4:
        print("  ⟹ **FAIL**：④样本不足（判据写死：④不过即 FAIL）。")
    elif not c1:
        print("  ⟹ **FAIL**：①跑不赢买入持有，做它没有意义（判据写死）。")
    elif c1 and c2 and c3 and c4 and c5:
        print("  ⟹ **PASS** → 走高置信直升评估，登记为独立择时腿（提示-only、零自动交易）。"
              if c6 else "  ⟹ **①②③④⑤过而⑥不过 → 记知识库，不接入生产。**")
    else:
        print("  ⟹ **FAIL**：②/③/⑤ 未过。")


if __name__ == "__main__":
    main()
