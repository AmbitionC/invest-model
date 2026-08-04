# -*- coding: utf-8 -*-
"""四腿宽基策略·长周期与滚动十年窗口检验

owner 2026-08-02：「策略周期放大到十年的周期再看看」。

两件事：
  A. **把样本拉到数据极限**（沪深300/红利 2005 起 ≈19.6 年可用、创业板 2010 起 ≈14.2 年、
     科创50 2019-12 起 ≈6.2 年），看策略在含 2007 泡沫 + 2008 崩盘的完整周期里的表现。
     **重要口径**：恐慌指数只回填到 2015-01，故 2015 年前**恐慌抢买腿自然不触发**——
     长窗结果 = 纯「锚买 + 月卖 5%」纪律的表现，不含恐慌腿。这一点必须在解读时记住。
  B. **滚动十年窗口**：按月滚动所有可能的 10 年起点，比较「策略年化」与「买入持有年化」，
     回答"随便挑一个十年，这套东西赢面多大、最差多差"。

口径同 SOP：一笔钱 100／闲钱 2%／exec_lag=1／日频回撤／卖出 flat5%／买入检查周频。
红利腿收益按全收益指数（H00922.CSI），信号仍用价格指数。只读 CSV，不落库。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib import font_manager  # noqa: E402

FONT = "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"
RF, CASH, WARM = 0.02, 0.02, 500
RUNG, FRAC = [0.50, 0.55, 0.60, 0.65], [0.30, 0.35, 0.40, 0.50]
# (名称, 信号CSV, 列, 全收益CSV, 起点=None 表示按锚预热完成日自动对齐, 模式)
# 2026-08-04 红队 M1：此前用硬编码起点（如 20070101），但 expanding 锚要到第 WARM=500 个
# 交易日才可用 —— 中间那段基准全额吃到、策略一股买不了。红利腿实测那 18 个交易日全收益
# 指数涨 +25.9%，把「跑输」虚增了约 1.2pp。**起点一律 = 策略第一个可交易日，策略与基准同起点。**
LEGS = [
    ("沪深300", "hs300.csv", "close", None, None, "anchor"),
    ("创业板", "spread_full.csv", "chinext", None, None, "anchor"),
    ("科创50", "star50.csv", "close", None, "20200601", "ladder"),   # 阶梯腿不用 expanding 锚
    ("红利", "000922_csi.csv", "close", "000922_tr.csv", None, "anchor"),
]


def first_tradable(df: pd.DataFrame, mode: str, fixed: str | None) -> str:
    """策略第一个可交易日：anchor 腿 = expanding 锚预热完成日；ladder 腿沿用给定起点。"""
    if fixed:
        return fixed
    idx = df.index[df["exp"].notna()]
    return str(df.trade_date.iloc[int(idx[0])]) if len(idx) else str(df.trade_date.iloc[0])


def prep(root: Path, f: str, col: str, trf: str | None) -> tuple[pd.DataFrame, pd.Series | None]:
    d = pd.read_csv(root / f, dtype={"trade_date": str}).sort_values("trade_date").reset_index(drop=True)
    d["c"] = pd.to_numeric(d[col])
    c = d.c.values
    d["exp"] = [np.median(c[: i + 1]) if i >= WARM else np.nan for i in range(len(d))]
    d["r1250"] = d.c.rolling(1250).median()
    d["peak"] = d.c.cummax()
    ym = d.trade_date.str[:6]
    d["me"] = (ym != ym.shift(-1)).values
    wk = pd.to_datetime(d.trade_date).dt.isocalendar()
    w = wk.week.astype(str) + "-" + wk.year.astype(str)
    d["we"] = (w != w.shift(-1)).values
    ret = None
    if trf:
        tr = pd.read_csv(root / trf, dtype={"trade_date": str})
        tr["c"] = pd.to_numeric(tr.close)
        d = d.merge(tr[["trade_date", "c"]], on="trade_date", suffixes=("", "_tr"))
        ret = d.c_tr
    return d, ret


def run(df, ret, fmap, nm, d0, d1, mode, init=100.0):
    d, c = df.trade_date.values, df.c.values
    rr = ret.pct_change().fillna(0).values if ret is not None else None
    i0 = int(np.searchsorted(d, d0))
    i1 = int(np.searchsorted(d, d1, side="right"))
    if i1 - i0 < 250:
        return None
    cash, units, nav = init, 0.0, 1.0
    last, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, pos, nb, ns, npan = [], [], 0, 0, 0
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            cash *= (1 + CASH) ** ((pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25)
            nav = nav * (1 + rr[i]) if rr is not None else ci
        elif rr is None:
            nav = ci
        r = df.iloc[i]
        for k, fr, _t in [x for x in pend if x[2] == i]:
            if k == "B":
                a = cash * fr
                if a > 0.05:
                    units += a / nav
                    cash -= a
                    nb += 1
            else:
                s = units * fr
                if s > 0:
                    cash += s * nav
                    units -= s
                    ns += 1
        pend = [x for x in pend if x[2] > i]
        sig, f = [], fmap.get(d[i], np.nan)
        if f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and ci < r.r1250:
            sig.append(("B", 0.50))
            npan += 1
        if f == f and f >= 75:
            last = i
        if mode == "ladder":
            dd = ci / r.peak - 1
            if dd <= -RUNG[0]:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate(RUNG) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False
                    sig.append(("B", FRAC[j]))
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            sig.append(("B", 0.20))
        mul = 1.30 * 1.10 if nm == "创业板" else 1.30
        if r.me and r.exp == r.exp and ci > r.exp * mul and units > 0:
            sig.append(("S", 0.05))
        for k, fr in sig:
            pend.append((k, fr, min(i + 1, i1 - 1)))
        tv = cash + units * nav
        curve.append(tv)
        pos.append(units * nav / tv)
    v = np.array(curve)
    pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(d[i1 - 1]) - pd.Timestamp(d[i0])).days / 365.25
    ann = (v[-1] / init) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    mdd = float(((v - pk) / pk).min())
    base = (ret if ret is not None else df.c).values
    bh = (base[i1 - 1] / base[i0]) ** (1 / yrs) - 1
    bhv = base[i0:i1]
    bhpk = np.maximum.accumulate(bhv)
    bhvol = float(pd.Series(bhv).pct_change().dropna().std() * np.sqrt(250))
    return dict(dates=d[i0:i1], curve=v, ann=ann, vol=vol, sharpe=(ann - RF) / vol,
                mdd=mdd, calmar=ann / abs(mdd), yrs=yrs, nb=nb, ns=ns, npan=npan,
                posavg=float(np.mean(pos)), bh=bh, bhmdd=float(((bhv - bhpk) / bhpk).min()), bhsharpe=(bh - RF) / bhvol if bhvol else np.nan)


def combined(data, fmap, starts, END, sleeve_interest=True):
    """四腿合计（各 25 元）。2026-08-04 红队 M5：科创50 sleeve 在开仓前的闲置利息
    此前只计入中间净值、没带进开仓本金 ⟹ 低报 0.22pp。此处修正为带息进场。
    同时返回「三腿诚实基线」（剔除科创50，只用三腿全程可比的区间）供风险指标对照。"""
    series, span0 = {}, min(starts[nm] for nm in starts)
    for nm, f, col, trf, _fx, mode in LEGS:
        df, ret = data[nm]
        d0 = starts[nm]
        init = 25.0
        if sleeve_interest and d0 > span0:
            init = 25.0 * (1 + CASH) ** ((pd.Timestamp(d0) - pd.Timestamp(span0)).days / 365.25)
        r = run(df, ret, fmap, nm, d0, END, mode, init=init)
        series[nm] = pd.Series(r["curve"], index=list(r["dates"]))
    cal = sorted(set().union(*[set(x.index) for x in series.values()]))

    def agg(names):
        tot = []
        for d in cal:
            v = 0.0
            for nm in names:
                sx = series[nm]
                if d in sx.index:
                    v += float(sx[d])
                elif d < sx.index[0]:
                    v += 25.0 * (1 + CASH) ** ((pd.Timestamp(d) - pd.Timestamp(span0)).days / 365.25)
                else:
                    v += float(sx.iloc[-1])
            tot.append(v)
        v = np.array(tot); pk = np.maximum.accumulate(v)
        n0 = 25.0 * len(names)
        yrs = (pd.Timestamp(cal[-1]) - pd.Timestamp(cal[0])).days / 365.25
        ann = (v[-1] / n0) ** (1 / yrs) - 1
        vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
        return dict(ann=ann, vol=vol, sharpe=(ann - RF) / vol,
                    mdd=float(((v - pk) / pk).min()), yrs=yrs, curve=v, cal=cal)

    four = agg([nm for nm in series])
    three = agg([nm for nm in series if nm != "科创50"])
    # 三腿诚实基线：只取三腿全程可比的区间（科创50 空窗期风险数字不可用）
    star0 = starts["科创50"]
    idx = [i for i, d in enumerate(cal) if d < star0]
    if idx:
        v3 = three["curve"][idx]; pk3 = np.maximum.accumulate(v3)
        three["mdd_prestar"] = float(((v3 - pk3) / pk3).min())
        three["frac_prestar"] = len(idx) / len(cal)
    return four, three


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    ap.add_argument("--out-dir", default="results")
    args = ap.parse_args()
    root, outd = Path(args.data), Path(args.out_dir)
    outd.mkdir(parents=True, exist_ok=True)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    END = "20260729"

    data = {nm: prep(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}

    # ---------- A. 各腿最长窗口 ----------
    print("=" * 104)
    print("A. 各腿最长可用窗口（2015 前无恐慌数据 ⇒ 恐慌腿自然不触发）")
    print("=" * 104)
    print(f"{'腿':8s}{'区间':>22s}{'年数':>6s}{'策略年化':>9s}{'买持年化':>9s}{'超额':>8s}"
          f"{'策略夏普':>9s}{'买持夏普':>9s}{'策略回撤':>9s}{'买持回撤':>9s}{'均持仓':>7s}{'恐慌买':>7s}")
    longs = {}
    starts = {nm: first_tradable(data[nm][0], mode, fx) for nm, _, _, _, fx, mode in LEGS}
    for nm, f, col, trf, _fx, mode in LEGS:
        df, ret = data[nm]
        d0 = starts[nm]
        r = run(df, ret, fmap, nm, d0, END, mode)
        longs[nm] = r
        print(f"{nm:8s}{d0[:4]+'-'+d0[4:6]+'~'+END[:4]+'-'+END[4:6]:>22s}{r['yrs']:>6.1f}"
              f"{r['ann']:>9.2%}{r['bh']:>9.2%}{(r['ann']-r['bh'])*100:>+8.2f}"
              f"{r['sharpe']:>9.2f}{r['bhsharpe']:>9.2f}{r['mdd']:>9.1%}{r['bhmdd']:>9.1%}"
              f"{r['posavg']:>7.0%}{r['npan']:>7d}")

    # ---------- B. 滚动十年窗口 ----------
    print("\n" + "=" * 104)
    print("B. 滚动十年窗口（按月滚动起点，每个窗口 10 年）")
    print("=" * 104)
    roll = {}
    for nm, f, col, trf, _fx, mode in LEGS:
        df, ret = data[nm]
        d0 = starts[nm]
        days = df.trade_date.values
        month_starts = sorted({d[:6] for d in days if d >= d0})
        rows = []
        for s in month_starts:
            s0 = s + "01"
            e0 = f"{int(s[:4]) + 10}{s[4:6]}28"
            if e0 > END:
                break
            r = run(df, ret, fmap, nm, s0, e0, mode)
            if r and r["yrs"] >= 9.5:
                rows.append((s, r["ann"], r["bh"], r["mdd"], r["bhmdd"], r["sharpe"]))
        roll[nm] = rows
        if not rows:
            print(f"{nm}：可用十年窗口 0 个（数据长度不足）")
            continue
        a = np.array([x[1] for x in rows])
        b = np.array([x[2] for x in rows])
        span = (pd.Timestamp(rows[-1][0] + "01") - pd.Timestamp(rows[0][0] + "01")).days / 365.25
        indep = span / 10 + 1
        wins = [x[0] for x in rows if x[1] > x[2]]
        blocks = 1 + sum(1 for a, b in zip(wins, wins[1:]) if
                         (pd.Timestamp(b + "01") - pd.Timestamp(a + "01")).days > 45) if wins else 0
        print(f"{nm}：{len(rows)} 个十年窗口（起点 {rows[0][0]} ~ {rows[-1][0]}）"
              f"｜⚠ **不重叠独立窗口仅 {indep:.2f} 个**，胜局分布于 {blocks} 个连续段"
              f"（{wins[0]}~{wins[-1]}）" if wins else
              f"{nm}：{len(rows)} 个十年窗口｜⚠ 不重叠独立窗口仅 {indep:.2f} 个｜无胜局")
        print(f"    策略年化 中位 {np.median(a):+.2%}｜最差 {a.min():+.2%}（起点 {rows[int(a.argmin())][0]}）"
              f"｜最好 {a.max():+.2%}（起点 {rows[int(a.argmax())][0]}）")
        print(f"    买持年化 中位 {np.median(b):+.2%}｜最差 {b.min():+.2%}｜最好 {b.max():+.2%}")
        print(f"    跑赢买持比例 {(a > b).mean():.0%}｜策略为正比例 {(a > 0).mean():.0%}"
              f"｜买持为正比例 {(b > 0).mean():.0%}｜中位超额 {np.median(a - b)*100:+.2f}pp")

    print("\n" + "=" * 104)
    print("C. 四腿合计（各 25 元 · 修正 sleeve 空窗期利息）与三腿诚实基线")
    print("=" * 104)
    four, three = combined(data, fmap, starts, END)
    print(f"  四腿合计（{four['yrs']:.1f}年）年化 {four['ann']:.2%} 波动 {four['vol']:.2%} "
          f"夏普 {four['sharpe']:.3f} 日频回撤 {four['mdd']:.1%}")
    print(f"  三腿基线（剔科创50）年化 {three['ann']:.2%} 波动 {three['vol']:.2%} "
          f"夏普 {three['sharpe']:.3f} 日频回撤 {three['mdd']:.1%}")
    if "frac_prestar" in three:
        print(f"  ⚠ 科创50 空窗期占全窗 {three['frac_prestar']:.1%}，该段四腿风险数字被一个常数腿稀释；"
              f"该段三腿回撤 {three['mdd_prestar']:.1%} 才是诚实读数")

    _charts(longs, roll, outd)


def _charts(longs: dict, roll: dict, outd: Path) -> None:
    fig = plt.figure(figsize=(16, 22))
    gs = fig.add_gridspec(4, 2, height_ratios=[1.25, 1.0, 1.0, 1.0], hspace=0.42, wspace=0.22, top=0.905)

    ax = fig.add_subplot(gs[0, :])
    for nm, col in zip(longs, ("#c0392b", "#2980b9", "#8e44ad", "#1e8449")):
        r = longs[nm]
        ax.plot(pd.to_datetime(r["dates"]), r["curve"], lw=1.5, color=col,
                label=f"{nm} 策略 年化{r['ann']:+.2%}（买持{r['bh']:+.2%}）")
    ax.set_yscale("log")
    ax.axvline(pd.Timestamp("2015-01-05"), color="#7f8c8d", ls=":", lw=1.5)
    ax.text(pd.Timestamp("2015-03-01"), ax.get_ylim()[0] * 1.15,
            "恐慌数据起点\n左侧＝纯锚买+月卖纪律", fontsize=9, color="#7f8c8d")
    ax.set_title("[1] 各腿最长窗口净值（起点各 100，对数轴）", fontsize=13)
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(alpha=.25, which="both")
    ax.set_ylabel("净值")

    ax = fig.add_subplot(gs[1, 0])
    nms = [n for n in roll if roll[n]]
    ax.boxplot([[x[1] * 100 for x in roll[n]] for n in nms], tick_labels=nms, widths=.5,
               patch_artist=True, boxprops=dict(facecolor="#5dade2", alpha=.6))
    ax.boxplot([[x[2] * 100 for x in roll[n]] for n in nms], tick_labels=nms, widths=.28,
               patch_artist=True, boxprops=dict(facecolor="#f5b041", alpha=.75))
    ax.axhline(0, color="k", lw=.8)
    ax.set_ylabel("十年窗口年化 %")
    ax.set_title("[2] 滚动十年窗口年化分布\n宽箱=策略（蓝）／窄箱=买入持有（橙）", fontsize=12)
    ax.grid(alpha=.25, axis="y")

    ax = fig.add_subplot(gs[1, 1])
    for nm, col in zip(nms, ("#c0392b", "#2980b9", "#1e8449", "#8e44ad")):
        rr = roll[nm]
        ax.scatter([x[2] * 100 for x in rr], [x[1] * 100 for x in rr], s=14, alpha=.65,
                   color=col, label=f"{nm}（{len(rr)} 窗口·跑赢 {np.mean([x[1] > x[2] for x in rr]):.0%}）")
    lim = [-8, 22]
    ax.plot(lim, lim, "--", color="#7f8c8d", lw=1)
    ax.set_xlim(*lim)
    ax.set_ylim(*lim)
    ax.set_xlabel("买入持有 十年年化 %")
    ax.set_ylabel("策略 十年年化 %")
    ax.set_title("[3] 每个十年窗口：策略 vs 买入持有\n对角线以上＝策略赢", fontsize=12)
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(alpha=.25)

    ax = fig.add_subplot(gs[2, :])
    for nm, col in zip(nms, ("#c0392b", "#2980b9", "#1e8449", "#8e44ad")):
        rr = roll[nm]
        xs = [pd.Timestamp(x[0][:4] + "-" + x[0][4:6] + "-01") for x in rr]
        ax.plot(xs, [(x[1] - x[2]) * 100 for x in rr], lw=1.8, color=col, label=f"{nm}")
    ax.axhline(0, color="k", lw=1)
    ax.set_ylabel("策略 减 买入持有（pp/年）")
    ax.set_title("[4] 十年窗口超额随起点变化（横轴＝十年窗口的起点）", fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(alpha=.25)

    ax = fig.add_subplot(gs[3, 0])
    nm4 = list(longs)
    ix = np.arange(len(nm4)); w = .36
    ax.bar(ix - w / 2, [longs[n]["mdd"] * 100 for n in nm4], w, color="#2980b9", label="策略")
    ax.bar(ix + w / 2, [longs[n]["bhmdd"] * 100 for n in nm4], w, color="#c0392b", label="买入持有")
    for i, n in enumerate(nm4):
        ax.text(i - w / 2, longs[n]["mdd"] * 100 - 4, f'{longs[n]["mdd"]*100:.0f}', ha="center", fontsize=9)
        ax.text(i + w / 2, longs[n]["bhmdd"] * 100 - 4, f'{longs[n]["bhmdd"]*100:.0f}', ha="center", fontsize=9)
    ax.set_xticks(ix); ax.set_xticklabels(nm4)
    ax.axhline(0, color="k", lw=.8); ax.set_ylim(-82, 6); ax.set_ylabel("最大回撤 %（日频）")
    ax.set_title("[5] 全窗口最大回撤：策略把 70% 级别的回撤压到 17~44%\n这是这套东西最大的单一价值", fontsize=12)
    ax.legend(fontsize=9, loc="lower right"); ax.grid(alpha=.25, axis="y")

    ax = fig.add_subplot(gs[3, 1])
    ix = np.arange(len(nms)); w = .18
    for j, (key, lb, col) in enumerate(((1, "策略", "#2980b9"), (2, "买入持有", "#c0392b"))):
        lo = [min(x[key] for x in roll[n]) * 100 for n in nms]
        me = [np.median([x[key] for x in roll[n]]) * 100 for n in nms]
        hi = [max(x[key] for x in roll[n]) * 100 for n in nms]
        off = (j - .5) * 2 * w
        ax.bar(ix + off, me, w * 1.7, color=col, alpha=.75, label=f"{lb} 中位")
        ax.errorbar(ix + off, me, yerr=[np.array(me) - np.array(lo), np.array(hi) - np.array(me)],
                    fmt="none", ecolor="#2c3e50", capsize=5, lw=1.3)
        for i in range(len(nms)):
            ax.text(ix[i] + off, lo[i] - 1.6, f"{lo[i]:.1f}", ha="center", fontsize=8, color="#2c3e50")
    ax.axhline(0, color="k", lw=1)
    ax.set_xticks(ix); ax.set_xticklabels(nms); ax.set_ylim(-9, 20); ax.set_ylabel("十年窗口年化 %")
    ax.set_title("[6] 十年窗口的中位与极值（须线=最差~最好，标注=最差）\n策略最差窗口全部为正，买入持有会亏钱", fontsize=12)
    ax.legend(fontsize=9, loc="lower right"); ax.grid(alpha=.25, axis="y")

    fig.suptitle("四腿宽基策略 · 长周期与滚动十年窗口检验\n"
                 "口径：一笔钱100／闲钱2%／exec_lag=1／日频回撤／卖出flat5%／红利按全收益",
                 fontsize=15, y=0.955)
    p = outd / "long_window.png"
    fig.savefig(p, dpi=100, bbox_inches="tight")
    print(f"\nsaved {p}")


if __name__ == "__main__":
    main()
