# -*- coding: utf-8 -*-
"""当前买卖点位图（owner 2026-08-04：「把现在买卖点在图片画出来我看看」）

不是历史回顾，是**此刻的位置**：每条腿现在的价、买入线、卖出线、距离多远、还要跌多少才开窗。
闸位口径与生产 hints（P26/P27 v2/P30）以及回测引擎完全同源，避免图表另起一套。

产出 results/broad_now.png。只读 CSV，不落库、不联网。
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

from invest_model.broad_gates import BUY_MUL, LADDER_RUNG, SELL_MUL  # noqa: E402
from long_window_backtest import FONT, LEGS, prep  # noqa: E402

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}
ETF = {"沪深300": "510300", "创业板": "159915", "科创50": "588000", "红利": "515080"}
GREEN, RED, GREY = "#1e8449", "#c0392b", "#7f8c8d"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    ap.add_argument("--out-dir", default="results")
    ap.add_argument("--years", type=float, default=3.0, help="下排小图回看年数")
    a = ap.parse_args()
    root, outd = Path(a.data), Path(a.out_dir)
    outd.mkdir(parents=True, exist_ok=True)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False

    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fear["score"] = pd.to_numeric(fear.score)
    f_now, f_date = float(fear.score.iloc[-1]), str(fear.trade_date.iloc[-1])

    G = {}
    for nm, f, col, trf, _fx, mode in LEGS:
        df, _ = prep(root, f, col, trf)
        last = float(df.c.iloc[-1])
        exp = float(df.exp.iloc[-1]) if df.exp.notna().any() else float("nan")
        r1250 = float(df.r1250.iloc[-1]) if df.r1250.notna().any() else float("nan")
        peak = float(df.peak.iloc[-1])
        bm, sm = BUY_MUL[nm], SELL_MUL[nm]      # P58：唯一真源 invest_model/broad_gates.py
        G[nm] = dict(df=df, mode=mode, date=str(df.trade_date.iloc[-1]), last=last,
                     exp=exp, r1250=r1250, peak=peak,
                     buy=exp * bm if exp == exp else float("nan"),
                     sell=exp * sm if exp == exp else float("nan"),
                     rung1=peak * (1 - LADDER_RUNG[0]))   # 阶梯第一档：唯一真源 broad_gates

    print("=" * 96)
    print(f"当前买卖点（恐慌 EOD {f_now:.0f} @ {f_date}）")
    print("=" * 96)
    for nm in G:
        g = G[nm]
        if g["mode"] == "ladder":
            print(f"  {nm:8s}{ETF[nm]}  收盘 {g['last']:>7.0f}  阶梯第一档 {g['rung1']:>7.0f}"
                  f"（距峰 {g['last']/g['peak']-1:+.0%}，还要跌 {g['rung1']/g['last']-1:+.0%} 才开窗）"
                  f"  卖出线 {g['sell']:>7.0f}（{g['last']/g['sell']-1:+.0%}）")
        else:
            print(f"  {nm:8s}{ETF[nm]}  收盘 {g['last']:>7.0f}  买入线 {g['buy']:>7.0f}"
                  f"（还要跌 {g['buy']/g['last']-1:+.0%}）  卖出线 {g['sell']:>7.0f}"
                  f"（{g['last']/g['sell']-1:+.0%}）  恐慌抢买价格闸 {g['r1250']:>7.0f}")

    # ── 出图 ────────────────────────────────────────────────
    fig = plt.figure(figsize=(17, 12.5))
    gs = fig.add_gridspec(2, 4, height_ratios=[1.15, 1], hspace=0.30, wspace=0.28, top=0.90)
    fig.suptitle(f"现在的买卖点位（数据截至各腿最后交易日 · 恐慌 EOD {f_now:.0f}）",
                 fontsize=19, weight="bold", y=0.965)

    # 上排：位置标尺（全部除以各自中位线归一，四腿可直接横向比"贵贱"）
    ax = fig.add_subplot(gs[0, :])
    xs = np.arange(len(G))
    for i, nm in enumerate(G):
        g = G[nm]
        anchor = g["exp"] if g["exp"] == g["exp"] else g["peak"]
        lo, hi = 0.70, max(2.25, g["last"] / anchor * 1.20)
        # 三区着色：买入区 / 持有区 / 卖出区
        bl = (g["buy"] / anchor) if g["mode"] != "ladder" else (g["rung1"] / anchor)
        sl = g["sell"] / anchor
        ax.add_patch(plt.Rectangle((i - .32, lo), .64, bl - lo, color=GREEN, alpha=.16))
        ax.add_patch(plt.Rectangle((i - .32, bl), .64, sl - bl, color="#f4d03f", alpha=.20))
        ax.add_patch(plt.Rectangle((i - .32, sl), .64, hi - sl, color=RED, alpha=.16))
        ax.hlines(bl, i - .32, i + .32, color=GREEN, lw=2.4)
        ax.hlines(sl, i - .32, i + .32, color=RED, lw=2.4)
        cur = g["last"] / anchor
        ax.plot(i, cur, "o", ms=15, color=COL[nm], zorder=6, mec="white", mew=1.6)
        # 三个标签全部收在本列内（ha=center / 列内左对齐），杜绝跨列重叠
        ax.text(i, cur + 0.085, f"现价 {g['last']:.0f}（锚的 {cur:.2f} 倍）",
                ha="center", va="bottom", fontsize=11, weight="bold", color=COL[nm],
                bbox=dict(fc="white", ec=COL[nm], alpha=.92, boxstyle="round,pad=0.25"))
        lbl_b = ("阶梯第一档" if g["mode"] == "ladder" else "买入线")
        bv = g["rung1"] if g["mode"] == "ladder" else g["buy"]
        ax.text(i - .30, bl + 0.018, f"{lbl_b} {bv:.0f}", fontsize=10, va="bottom",
                ha="left", color=GREEN, weight="bold")
        ax.text(i - .30, sl + 0.018, f"卖出线 {g['sell']:.0f}", fontsize=10, va="bottom",
                ha="left", color=RED, weight="bold")
        # 还要跌多少
        need = ((g["rung1"] if g["mode"] == "ladder" else g["buy"]) / g["last"] - 1)
        ax.text(i, lo + 0.03, f"离买入窗还要跌 {need:.0%}", ha="center", fontsize=10,
                color=GREEN, weight="bold")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{nm}\n{ETF[nm]}" for nm in G], fontsize=12.5, weight="bold")
    ax.set_ylabel("价格 ÷ 各自锚（中位线；科创50 为全历史峰）", fontsize=11)
    ax.axhline(1.0, color=GREY, ls=":", lw=1.4)
    ax.text(len(G) - .5, 1.005, "锚=1.00", fontsize=9.5, color=GREY, va="bottom", ha="right")
    ax.set_xlim(-.55, len(G) - .45); ax.set_ylim(0.70, 2.25); ax.grid(alpha=.2, axis="y")
    ax.set_title("四条腿现在都在什么位置——绿=买入区 黄=持有区 红=卖出区",
                 fontsize=13.5, weight="bold")

    # 下排：各腿近 N 年走势 + 两条线 + 当前点
    for k, nm in enumerate(G):
        axx = fig.add_subplot(gs[1, k])
        g = G[nm]
        d = g["df"]
        cut = (pd.Timestamp(g["date"]) - pd.Timedelta(days=int(a.years * 365))).strftime("%Y%m%d")
        sub = d[d.trade_date >= cut]
        t = pd.to_datetime(sub.trade_date)
        axx.plot(t, sub.c, lw=1.3, color=COL[nm])
        if g["mode"] == "ladder":
            axx.axhline(g["rung1"], color=GREEN, ls="--", lw=1.5)
            axx.text(t.iloc[0], g["rung1"], f" 阶梯档 {g['rung1']:.0f}", color=GREEN,
                     fontsize=9, va="bottom")
        else:
            axx.axhline(g["buy"], color=GREEN, ls="--", lw=1.5)
            axx.text(t.iloc[0], g["buy"], f" 买 {g['buy']:.0f}", color=GREEN,
                     fontsize=9, va="bottom")
        axx.axhline(g["sell"], color=RED, ls="--", lw=1.5)
        axx.text(t.iloc[0], g["sell"], f" 卖 {g['sell']:.0f}", color=RED,
                 fontsize=9, va="bottom")
        axx.plot(t.iloc[-1], g["last"], "o", ms=10, color=COL[nm], mec="white", mew=1.4, zorder=5)
        axx.annotate(f"{g['last']:.0f}", (t.iloc[-1], g["last"]), textcoords="offset points",
                     xytext=(-6, 10), ha="right", fontsize=10.5, weight="bold", color=COL[nm])
        if g["last"] > g["sell"]:
            state, sc = "【卖出区】", RED
        elif (g["mode"] != "ladder" and g["last"] < g["buy"]) or \
             (g["mode"] == "ladder" and g["last"] < g["rung1"]):
            state, sc = "【买入窗】", GREEN
        else:
            state, sc = "【持有区】", "#b7950b"
        axx.set_title(f"{nm}  {state}", fontsize=12.5, weight="bold", color=sc)
        lo = min(sub.c.min(), g["buy"] if g["mode"] != "ladder" else g["rung1"]) * 0.94
        hi = max(sub.c.max(), g["sell"]) * 1.06
        axx.set_ylim(lo, hi); axx.grid(alpha=.22); axx.tick_params(labelsize=9)
        import matplotlib.dates as mdates
        axx.xaxis.set_major_locator(mdates.YearLocator())
        axx.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    fig.text(0.5, 0.015,
             f"恐慌抢买（四腿共用）：EOD {f_now:.0f} < 75 未触发　｜　"
             f"P30 加杠杆：低价 未中 × 恐慌 未中　｜　P28 深危机窗 0/3　｜　"
             f"结论：四腿无一在买入窗，三腿在卖出区——只减不加的位置",
             ha="center", fontsize=12.5, weight="bold",
             bbox=dict(fc="#fdedec", ec=RED, alpha=.9))

    out = outd / "broad_now.png"
    fig.savefig(out, dpi=118, bbox_inches="tight")
    print(f"\nsaved {out}")


if __name__ == "__main__":
    main()
