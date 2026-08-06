# -*- coding: utf-8 -*-
"""四腿宽基·全窗历史交易与仓位变化（owner 2026-08-05：「画历史交易曲线，包括历史仓位变化，
时间跨度放大一点」）。

每条腿两行：
  上：价格（对数轴，19.5 年跨度不用对数看不清 2008）+ 中位线锚 + 卖出线，
      逐笔买卖打点（点面积∝成交金额占该腿总资金的比例，一眼看出「重手买的是哪几天」）
  下：仓位曲线（面积图）——这套东西的形状全在这条线上：崩盘时冲到满仓，慢牛里被月卖磨到低仓

最后一格：四腿净值 vs 各自买入持有。

口径：闸位取 `invest_model/broad_gates.py`（P58 唯一真源，2026-08-05 起 1.30/创业板 1.43）；
引擎直接复用 `long_window_backtest.run`，图与表不会再出现两套数。
只读 results/*.csv，不落库、不联网。产出 results/broad_history.png。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib import font_manager  # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from invest_model.broad_gates import BUY_MUL, SELL_MUL  # noqa: E402
from long_window_backtest import FONT, LEGS, first_tradable, prep, run  # noqa: E402

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}
ETF = {"沪深300": "510300", "创业板": "159915", "科创50": "588000", "红利": "515080"}
BUYC, SELLC, GREY = "#1e8449", "#c0392b", "#7f8c8d"
WHY = {"锚买": "锚买", "恐慌": "恐慌抢买", "阶梯": "深回撤阶梯", "月卖": "月度减仓"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
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
    mode = {nm: m for nm, _, _, _, _, m in LEGS}
    XLO = min(pd.to_datetime(df.trade_date).iloc[0] for df, _ in data.values())
    XHI = max(pd.to_datetime(df.trade_date).iloc[-1] for df, _ in data.values())
    res = {}
    for nm, (df, ret) in data.items():
        st = first_tradable(df, mode[nm], None)
        res[nm] = run(df, ret, fmap, nm, st, None, mode[nm])

    fig = plt.figure(figsize=(19, 26))
    # 每条腿占「价格 + 仓位」两行，组间插一行空白（gridspec 的 hspace 是全局的，
    # 想让组内贴紧、组间留白只能靠空白行）
    gs = fig.add_gridspec(13, 1,
                          height_ratios=[1.55, .62, .38] * 4 + [1.5],
                          hspace=0.0, top=0.941, bottom=0.038, left=0.062, right=0.975)
    fig.suptitle("四条宽基腿·全窗历史交易与仓位变化", fontsize=25, weight="bold", y=0.977)
    fig.text(0.5, 0.9645,
             "卖出闸 = 中位线 × 1.30（创业板 × 1.43），2026-08-05 起生产与回测统一　｜　"
             "点的大小 ∝ 单笔金额占该腿资金的比例　｜　价格为对数轴",
             ha="center", fontsize=12.5, color=GREY)

    for k, nm in enumerate(data):
        r = res[nm]
        df, _ = data[nm]
        t_all = pd.to_datetime(df.trade_date)
        dts = pd.to_datetime(r["dates"])

        # ── 上：价格 + 闸线 + 逐笔成交 ──────────────────────
        ax = fig.add_subplot(gs[k * 3, 0])
        ax.plot(t_all, df.c, lw=1.15, color=COL[nm], zorder=3)
        if mode[nm] != "ladder":
            ax.plot(t_all, df.exp, lw=1.15, color="#34495e", ls=":", zorder=4, label="中位线锚")
            ax.plot(t_all, df.exp * SELL_MUL[nm], lw=1.3, color=SELLC, ls="--", alpha=.75,
                    zorder=4, label=f"卖出线（锚×{SELL_MUL[nm]:.2f}）")
            if BUY_MUL[nm] != 1.0:
                ax.plot(t_all, df.exp * BUY_MUL[nm], lw=1.3, color=BUYC, ls="--", alpha=.75,
                        zorder=4, label=f"买入线（锚×{BUY_MUL[nm]:.2f}）")
        else:
            ax.plot(t_all, df.peak * 0.50, lw=1.3, color=BUYC, ls="--", alpha=.75,
                    zorder=4, label="阶梯第一档（距峰 -50%）")
            ax.plot(t_all, df.exp * SELL_MUL[nm], lw=1.3, color=SELLC, ls="--", alpha=.75,
                    zorder=4, label=f"卖出线（锚×{SELL_MUL[nm]:.2f}）")

        for side, colr, mk in (("买", BUYC, "^"), ("卖", SELLC, "v")):
            sel = [x for x in r["trades"] if x["side"] == side]
            if not sel:
                continue
            xs = pd.to_datetime([x["date"] for x in sel])
            ys = [x["price"] for x in sel]
            sz = [max(14, min(320, x["amount"] / 100.0 * 900)) for x in sel]
            ax.scatter(xs, ys, s=sz, marker=mk, color=colr, alpha=.55,
                       edgecolors="white", linewidths=.5, zorder=6,
                       label=f"{side}出" if side == "卖" else "买入")
        ax.set_yscale("log")
        ax.set_ylabel("点位（对数）", fontsize=10.5)
        cum_s = r["curve"][-1] / r["curve"][0]
        cum_b = (1 + r["bh"]) ** r["yrs"]
        ax.set_title(f"{nm} · {ETF[nm]}　{r['dates'][0][:4]}-{r['dates'][0][4:6]} ~ "
                     f"{r['dates'][-1][:4]}-{r['dates'][-1][4:6]}（{r['yrs']:.1f} 年）　"
                     f"买 {r['nb']} 笔 · 卖 {r['ns']} 笔　｜　"
                     f"累计 {cum_s - 1:+.0%}（{cum_s:.2f} 倍） vs 买入持有 {cum_b - 1:+.0%}（{cum_b:.2f} 倍）　"
                     f"年化 {r['ann']:.2%} vs {r['bh']:.2%}　回撤 {r['mdd']:.1%} vs {r['bhmdd']:.1%}",
                     fontsize=12.5, weight="bold", color=COL[nm], pad=7)
        ax.grid(alpha=.22, which="both"); ax.tick_params(labelbottom=False, labelsize=9)
        ax.legend(fontsize=9, ncol=4, loc="upper left", framealpha=.9)
        ax.set_xlim(XLO, XHI)

        # ── 下：仓位 ──────────────────────────────────────
        axp = fig.add_subplot(gs[k * 3 + 1, 0])
        pos = r["pos_series"] * 100
        axp.fill_between(dts, 0, pos, color=COL[nm], alpha=.30, zorder=2)
        axp.plot(dts, pos, lw=.9, color=COL[nm], zorder=3)
        axp.axhline(r["posavg"] * 100, color="#34495e", ls=":", lw=1.2, zorder=4)
        axp.text(dts.iloc[0] if hasattr(dts, "iloc") else dts[0], r["posavg"] * 100 + 4,
                 f" 均仓 {r['posavg']:.0%}", fontsize=9.5, color="#34495e", va="bottom")
        axp.set_ylim(0, 105); axp.set_ylabel("仓位 %", fontsize=10)
        axp.grid(alpha=.22); axp.tick_params(labelsize=9)
        axp.set_xlim(XLO, XHI)
        axp.xaxis.set_major_locator(mdates.YearLocator(2))
        axp.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # ── 末格：净值 vs 买入持有 ──────────────────────────────
    ax = fig.add_subplot(gs[12, 0])
    for nm in data:
        r = res[nm]
        dts = pd.to_datetime(r["dates"])
        cs = r["curve"] / r["curve"][0]
        ax.plot(dts, cs, lw=2.0, color=COL[nm],
                label=f"{nm} 策略 {cs[-1]:.2f}倍（年化 {r['ann']:.2%}）")
        ax.annotate(f"{cs[-1]:.2f}倍", (dts[-1], cs[-1]), textcoords="offset points",
                    xytext=(6, 0), fontsize=10, weight="bold", color=COL[nm], va="center")
        df, ret = data[nm]
        s = (ret if ret is not None else df.c).ffill()
        i0 = int(np.searchsorted(df.trade_date.values, r["dates"][0]))
        bh = s.iloc[i0:i0 + len(dts)].to_numpy(dtype=float)
        cb = bh / bh[0]
        ax.plot(dts, cb, lw=1.1, color=COL[nm], ls=":", alpha=.65,
                label=f"{nm} 买入持有 {cb[-1]:.2f}倍（年化 {r['bh']:.2%}）")
    ax.set_yscale("log"); ax.set_ylabel("净值倍数（对数·各腿从 1 起）", fontsize=10.5)
    ax.grid(alpha=.25, which="both"); ax.tick_params(labelsize=9.5)
    ax.legend(fontsize=9.5, ncol=4, loc="upper left", framealpha=.9)
    ax.set_xlim(XLO, XHI)
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_title("四条腿累计净值 vs 各自买入持有（实线=策略，虚线=买入持有；各腿起点不同、窗口不等长，只在同色之间比较，不可横向比倍数）", fontsize=13.5, weight="bold", pad=7)

    out = outd / "broad_history.png"
    fig.savefig(out, dpi=104)
    print(f"saved {out}\n")

    print("=" * 104)
    print("成交构成（全窗，各腿一笔钱 100）")
    print("=" * 104)
    print(f"{'腿':>8s}{'区间':>18s}{'年数':>6s}{'策略累计':>10s}{'买持累计':>10s}"
          f"{'策略年化':>9s}{'买持年化':>9s}{'买笔':>5s}{'卖笔':>5s}{'均仓':>6s}")
    for nm in data:
        r = res[nm]
        cum_s = r["curve"][-1] / r["curve"][0]
        cum_b = (1 + r["bh"]) ** r["yrs"]
        print(f"{nm:>8s}{r['dates'][0]+'~'+r['dates'][-1]:>18s}{r['yrs']:>6.1f}"
              f"{cum_s:>9.2f}倍{cum_b:>9.2f}倍{r['ann']:>9.2%}{r['bh']:>9.2%}"
              f"{r['nb']:>5d}{r['ns']:>5d}{r['posavg']:>6.0%}")


if __name__ == "__main__":
    main()
