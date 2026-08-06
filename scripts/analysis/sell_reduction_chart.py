# -*- coding: utf-8 -*-
"""降低卖出力度 → 对单腿与**组合整体**收益的影响（owner 2026-08-04 追问）

owner：「还是要交易。看看降低卖出策略后，对整体收益有影响吗，图重新画一份。」

E49 首跑只报了单腿 Δ，这里补上 owner 真正问的「整体」——组合层面：
  · 两腿全窗（沪深300+红利，19.5 年，全程真实可持有）
  · 四腿共同在场（2019-12 起，6.6 年）
两个口径都是交叉验证后保留的合法口径（19.5 年四腿拼接已退役）。

产出 results/sell_reduction.png（四格）+ stdout 明细表。只读 CSV，不落库、不联网。
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

from e48_e49_sell_variants import run_v  # noqa: E402
from long_window_backtest import CASH, FONT, LEGS, RF, first_tradable, prep  # noqa: E402

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}
# 卖出力度从强到弱排列——横轴就是"卖得越来越少"
CAND = [
    ("现状 1.30/月/5%", dict()),
    ("闸1.40", dict(sell_mul=1.40)),
    ("闸1.50", dict(sell_mul=1.50)),
    ("季频15%", dict(sell_every_month=3, sell_frac=0.15)),
    ("季频5%", dict(sell_every_month=3)),
    ("冷却3月", dict(sell_cooldown=3)),
    ("完全不卖", dict(no_sell=True)),
]


def combo(series: dict[str, pd.Series], names: list[str], d_start: str | None = None):
    """组合口径：各腿等额起手，按共同日历相加。d_start=None 用最早腿起点。"""
    cal = sorted(set().union(*[set(series[n].index) for n in names]))
    if d_start:
        cal = [d for d in cal if d >= d_start]
    if len(cal) < 250:
        return None
    tot = []
    for d in cal:
        v = 0.0
        for n in names:
            s = series[n]
            if d in s.index:
                v += float(s[d])
            elif d < s.index[0]:                       # 尚未开腿：现金按 2%/年计息
                v += 100.0 * (1 + CASH) ** ((pd.Timestamp(d) - pd.Timestamp(cal[0])).days / 365.25)
            else:
                v += float(s.iloc[-1])
        tot.append(v)
    v = np.array(tot); pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(cal[-1]) - pd.Timestamp(cal[0])).days / 365.25
    ann = (v[-1] / v[0]) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    return dict(ann=ann, vol=vol, sharpe=(ann - RF) / vol,
                mdd=float(((v - pk) / pk).min()), yrs=yrs, curve=v, cal=cal)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    ap.add_argument("--out-dir", default="results")
    a = ap.parse_args()
    root, outd = Path(a.data), Path(a.out_dir)
    outd.mkdir(parents=True, exist_ok=True)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False

    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    data = {nm: prep(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}
    starts = {nm: first_tradable(data[nm][0], mode, fx) for nm, _, _, _, fx, mode in LEGS}
    ends = {nm: str(data[nm][0].trade_date.iloc[-1]) for nm in data}
    modes = {nm: mode for nm, _, _, _, _, mode in LEGS}

    R = {}
    for label, kw in CAND:
        legs, ser = {}, {}
        for nm in data:
            df, ret = data[nm]
            r = run_v(df, ret, fmap, nm, starts[nm], ends[nm], modes[nm], **kw)
            legs[nm] = r
            ser[nm] = pd.Series(r["curve"], index=list(r["dates"]))
        R[label] = dict(legs=legs, ser=ser,
                        two=combo(ser, ["沪深300", "红利"]),
                        four=combo(ser, list(data), d_start=max(starts.values())))

    base = R["现状 1.30/月/5%"]

    print("=" * 108)
    print("一、单腿：卖出力度从强到弱（每腿 100 元起）")
    print("=" * 108)
    for nm in data:
        print(f"\n  {nm}（{starts[nm][:6]}~{ends[nm][:6]}，买入持有 {base['legs'][nm]['bh']:.2%}）")
        print(f"    {'方案':16s}{'年化':>9s}{'Δ年化':>9s}{'夏普':>8s}{'回撤':>9s}"
              f"{'均仓':>7s}{'卖笔数':>8s}{'Δ笔数':>8s}")
        for label, _ in CAND:
            r, b = R[label]["legs"][nm], base["legs"][nm]
            print(f"    {label:16s}{r['ann']:>9.2%}{(r['ann'] - b['ann']) * 100:>+9.2f}"
                  f"{r['sharpe']:>8.2f}{r['mdd']:>9.1%}{r['posavg']:>7.0%}"
                  f"{r['ns']:>8d}{r['ns'] - b['ns']:>+8d}")

    print("\n" + "=" * 108)
    print("二、**整体（组合口径）**——owner 问的就是这个")
    print("=" * 108)
    for key, title in (("two", "两腿全窗（沪深300+红利，19.5 年，全程真实可持有）"),
                       ("four", "四腿共同在场（2019-12 起，6.6 年）")):
        print(f"\n  {title}")
        print(f"    {'方案':16s}{'年化':>9s}{'Δ年化':>9s}{'夏普':>8s}{'Δ夏普':>8s}"
              f"{'日频回撤':>10s}{'Δ回撤':>9s}{'四腿卖笔数':>11s}")
        for label, _ in CAND:
            c, bc = R[label][key], base[key]
            ns = sum(R[label]["legs"][n]["ns"] for n in data)
            print(f"    {label:16s}{c['ann']:>9.2%}{(c['ann'] - bc['ann']) * 100:>+9.2f}"
                  f"{c['sharpe']:>8.3f}{c['sharpe'] - bc['sharpe']:>+8.3f}"
                  f"{c['mdd']:>10.1%}{(c['mdd'] - bc['mdd']) * 100:>+9.1f}{ns:>11d}")

    _chart(R, data, base, outd)


def _chart(R, data, base, outd: Path) -> None:
    labels = [l for l, _ in CAND]
    fig = plt.figure(figsize=(17, 13))
    gs = fig.add_gridspec(2, 2, hspace=0.34, wspace=0.22, top=0.905)
    fig.suptitle("降低卖出力度 → 对单腿与组合整体的影响（卖得越来越少 →）",
                 fontsize=18, weight="bold", y=0.958)

    # ① 组合整体：年化 + 卖出笔数（双轴）
    ax = fig.add_subplot(gs[0, 0])
    x = np.arange(len(labels))
    two = [R[l]["two"]["ann"] * 100 for l in labels]
    four = [R[l]["four"]["ann"] * 100 for l in labels]
    ax.plot(x, two, "o-", lw=2.4, color="#c0392b", label="两腿全窗 19.5 年")
    ax.plot(x, four, "s-", lw=2.4, color="#8e44ad", label="四腿共同在场 6.6 年")
    for i, (p, q) in enumerate(zip(two, four)):
        ax.annotate(f"{p:.2f}", (i, p), textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=9, color="#c0392b")
        ax.annotate(f"{q:.2f}", (i, q), textcoords="offset points", xytext=(0, -14),
                    ha="center", fontsize=9, color="#8e44ad")
    ax2 = ax.twinx()
    ns = [sum(R[l]["legs"][n]["ns"] for n in data) for l in labels]
    ax2.bar(x, ns, .5, color="#bdc3c7", alpha=.45, zorder=0)
    ax2.set_ylabel("四腿卖出总笔数（灰柱）", fontsize=10)
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=9.5, rotation=18)
    ax.set_ylabel("组合年化（%）"); ax.legend(fontsize=10, loc="lower left")
    ax.grid(alpha=.25); ax.set_zorder(2); ax.patch.set_visible(False)
    ax.set_title("① 整体收益 vs 卖出笔数——这是 owner 问的那张图",
                 fontsize=13, weight="bold")

    # ② 组合整体：夏普与回撤
    ax = fig.add_subplot(gs[0, 1])
    sh = [R[l]["two"]["sharpe"] for l in labels]
    md = [abs(R[l]["two"]["mdd"]) * 100 for l in labels]
    ax.plot(x, sh, "o-", lw=2.4, color="#1e8449", label="两腿夏普（左轴）")
    ax.set_ylabel("夏普"); ax.set_ylim(min(sh) * .85, max(sh) * 1.12)
    ax3 = ax.twinx()
    ax3.plot(x, md, "^--", lw=2.2, color="#e67e22", label="两腿日频最大回撤（右轴）")
    ax3.set_ylabel("最大回撤（%，越低越好）")
    for i, (s_, m_) in enumerate(zip(sh, md)):
        ax.annotate(f"{s_:.3f}", (i, s_), textcoords="offset points", xytext=(0, 8),
                    ha="center", fontsize=9, color="#1e8449")
        ax3.annotate(f"{m_:.0f}%", (i, m_), textcoords="offset points", xytext=(0, -14),
                     ha="center", fontsize=9, color="#e67e22")
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=9.5, rotation=18)
    ax.grid(alpha=.25)
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax3.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, fontsize=9.5, loc="lower left")
    ax.set_title("② 少卖的代价：收益微涨，风险明显变差", fontsize=13, weight="bold")

    # ③ 单腿 Δ年化
    ax = fig.add_subplot(gs[1, 0])
    w = 0.2
    for j, nm in enumerate(data):
        d = [(R[l]["legs"][nm]["ann"] - base["legs"][nm]["ann"]) * 100 for l in labels]
        ax.bar(x + (j - 1.5) * w, d, w, color=COL[nm], label=nm)
    ax.axhline(0, color="#333", lw=1)
    ax.axhline(-0.30, color="#c0392b", ls=":", lw=1.3)
    ax.text(len(labels) - .5, -0.30, " E49 判据线 −0.30pp", color="#c0392b",
            fontsize=9, va="bottom", ha="right")
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=9.5, rotation=18)
    ax.set_ylabel("Δ年化 vs 现状（pp）"); ax.legend(fontsize=9.5, ncol=2)
    ax.grid(alpha=.25, axis="y")
    ax.set_title("③ 单腿：谁受不了少卖——沪深300/创业板 是瓶颈",
                 fontsize=13, weight="bold")

    # ④ 净值曲线：两腿组合
    ax = fig.add_subplot(gs[1, 1])
    for l, c, lw in (("现状 1.30/月/5%", "#c0392b", 2.4), ("闸1.40", "#2980b9", 1.9),
                     ("闸1.50", "#8e44ad", 1.6), ("完全不卖", "#7f8c8d", 1.6)):
        cc = R[l]["two"]
        ax.plot(pd.to_datetime(cc["cal"]), cc["curve"] / cc["curve"][0], lw=lw, color=c,
                label=f"{l}  {cc['ann']:.2%}/yr  回撤{cc['mdd']:.0%}")
    ax.set_yscale("log")
    ax.legend(fontsize=9.5, loc="upper left"); ax.grid(alpha=.25)
    ax.set_title("④ 两腿组合净值（对数轴）：曲线几乎重叠，差别在跌的时候",
                 fontsize=13, weight="bold")

    out = outd / "sell_reduction.png"
    fig.savefig(out, dpi=115, bbox_inches="tight")
    print(f"\nsaved {out}")


if __name__ == "__main__":
    main()
