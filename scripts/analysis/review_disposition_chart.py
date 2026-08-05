# -*- coding: utf-8 -*-
"""三方评审处置·SOP 第六步：图表与必报指标。

五张图，一一对应 docs/model_change_proposals.md「三方评审的处置方案」里的五条命题：
  ① 卖出闸不一致（P58）——生产提示线 vs 回测验证线，画在真实价格上
  ② 卖出闸的真实权衡（命题A）——收益 vs 回撤，两个极点都被支配
  ③ 加码斜率（命题B/P59）——钱到底投在了哪个价位
  ④ 底仓 sleeve（命题C/P60）——按他自己的目标函数检验也不改善
  ⑤ 量能地量（命题E/P62）——名义阈值漂移 vs 滚动分位口径的前瞻梯度

数据全部来自可行域标定脚本 review_disposition_calib.py / e55_volume_calib.py 的同一套引擎。
只读 CSV，不落库、不联网。产出 results/review_disposition.png。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib import font_manager  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from long_window_backtest import FONT, prep  # noqa: E402
import review_disposition_calib as K  # noqa: E402

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}
GREEN, RED, GREY, AMBER = "#1e8449", "#c0392b", "#7f8c8d", "#b7950b"
LEGS = list(K.data)
# 生产提示闸（_BROAD_LEGS）vs 回测引擎闸（long_window_backtest）
PROD = {"沪深300": 1.00, "创业板": 1.10, "科创50": 1.30, "红利": 1.00}
BACK = {"沪深300": 1.30, "创业板": 1.43, "科创50": 1.30, "红利": 1.30}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="results")
    a = ap.parse_args()
    outd = Path(a.out_dir); outd.mkdir(parents=True, exist_ok=True)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False

    fig = plt.figure(figsize=(19, 21))
    gs = fig.add_gridspec(4, 4, height_ratios=[1.15, 1, 1, 1], hspace=0.50, wspace=0.30,
                          top=0.915, bottom=0.075, left=0.055, right=0.975)
    fig.suptitle("三方独立评审的处置：五条命题的数据裁决", fontsize=23, weight="bold", y=0.975)
    fig.text(0.5, 0.948, "判据已于跑定裁数之前写死并提交（P58~P63 / E51~E55）｜ 本轮不改任何生产代码",
             ha="center", fontsize=12.5, color=GREY)

    # ── ① 卖出闸不一致：画在真实价格上 ────────────────────────────
    for k, nm in enumerate(LEGS):
        ax = fig.add_subplot(gs[0, k])
        df, _ = K.data[nm]
        d = df[df.trade_date >= "20210101"]
        t = pd.to_datetime(d.trade_date)
        last = float(df.c.iloc[-1]); med = float(df.exp.iloc[-1])
        ax.plot(t, d.c, lw=1.2, color=COL[nm])
        ax.plot(t, d.exp, lw=1.0, color=GREY, ls=":")
        p, b = med * PROD[nm], med * BACK[nm]
        ax.axhline(p, color=RED, ls="-", lw=2.0)
        ax.axhline(b, color=AMBER, ls="--", lw=2.0)
        ax.fill_between(t, p, b, color=AMBER, alpha=.13)
        ax.plot(t.iloc[-1], last, "o", ms=9, color=COL[nm], mec="white", mew=1.4, zorder=5)
        gap = b / p - 1
        ax.set_title(f"{nm}　生产 {p:.0f} vs 回测 {b:.0f}"
                     + ("　【一致】" if gap < 1e-6 else f"　差 {gap:+.0%}"),
                     fontsize=12, weight="bold", color=GREY if gap < 1e-6 else RED)
        ax.set_ylim(min(d.c.min(), p) * .92, max(d.c.max(), b) * 1.08)
        ax.grid(alpha=.22); ax.tick_params(labelsize=8.5)
        import matplotlib.dates as mdates
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%y"))
        if k == 0:
            ax.text(0.02, 0.97, "红实线=每天提示给你的卖出线\n黄虚线=所有收益数字实际用的线\n黄区=两者之间的差",
                    transform=ax.transAxes, va="top", fontsize=9, color="#5d4037",
                    bbox=dict(fc="#fff8e1", ec=AMBER, alpha=.95, boxstyle="round,pad=0.3"))

    # ── ② 卖出闸的真实权衡 ────────────────────────────────────
    ax = fig.add_subplot(gs[1, :2])
    GATES = [(None, "不卖"), (1.00, "×1.00\n生产"), (1.15, "×1.15"), (1.30, "×1.30\n回测"), (1.50, "×1.50")]
    for nm in LEGS:
        xs, ys, lb = [], [], []
        for mu, lab in GATES:
            kw = dict(no_sell=True) if mu is None else dict(sell_mul=mu * (1.10 if nm == "创业板" else 1.0))
            r = K.R(nm, **kw)
            xs.append(abs(r["mdd"])); ys.append(r["ann"]); lb.append(lab)
        ax.plot(xs, ys, "-o", color=COL[nm], lw=1.8, ms=6, label=nm)
        for x, y, s in zip(xs, ys, lb):
            if "生产" in s:
                ax.annotate("×1.00 生产", (x, y), textcoords="offset points", xytext=(-4, -15),
                            ha="right", fontsize=8.5, color=COL[nm], weight="bold")
            elif "回测" in s:
                ax.annotate("×1.30 回测", (x, y), textcoords="offset points", xytext=(4, 8),
                            ha="left", fontsize=8.5, color=COL[nm], weight="bold")
    ax.set_xlabel("最大回撤（绝对值·越左越好）", fontsize=11)
    ax.set_ylabel("年化", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.set_xlim(0.10, 0.75)
    ax.grid(alpha=.25); ax.legend(fontsize=10, ncol=2, loc="lower right")
    ax.set_title("① 卖出闸是真权衡，不是对错题——闸越高越赚也越痛，四腿夏普全在噪声内",
                 fontsize=13, weight="bold")

    # ── ③ 加码斜率：钱投在哪个价位 ──────────────────────────────
    ax = fig.add_subplot(gs[1, 2:])
    SZ = [("cur", "当前现金×比例\n（现状）", "#c0392b"), ("init", "起始资金×比例\n（金额恒定）", "#2980b9"),
          ("ramp", "越深越大\n（金额递增）", "#1e8449")]
    w, xs = 0.26, np.arange(len(LEGS))
    for j, (sz, lab, c) in enumerate(SZ):
        vals = [K.R(nm, size=sz)["deep_share"] for nm in LEGS]
        bars = ax.bar(xs + (j - 1) * w, vals, w * .92, color=c, alpha=.85, label=lab)
        for x, v in zip(bars, vals):
            ax.text(x.get_x() + x.get_width() / 2, v + .006, f"{v:.0%}",
                    ha="center", fontsize=9, weight="bold", color=c)
    ax.set_xticks(xs); ax.set_xticklabels(LEGS, fontsize=11.5, weight="bold")
    ax.set_ylabel("投在「最低价 +5% 档」内的资金占比", fontsize=10.5)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25, axis="y"); ax.legend(fontsize=9.5, loc="upper left")
    ax.set_title("② 现状把钱投在了错的价位——创业板 0.0%、红利 0.4% 投在最低档",
                 fontsize=13, weight="bold")

    # ── ④ 底仓 sleeve ────────────────────────────────────────
    BASES = [0.0, .25, .50, .75, 1.0]
    ax = fig.add_subplot(gs[2, :2])
    for nm in LEGS:
        rs = [K.R(nm, base=b) for b in BASES]
        ax.plot([b * 100 for b in BASES], [r["ann"] for r in rs], "-o",
                color=COL[nm], lw=1.9, ms=6, label=nm)
        bh = K.bh(nm)
        ax.plot(100, bh["ann"], "*", ms=15, color=COL[nm], mec="white", mew=1.0)
    ax.set_xlabel("底仓比例（%）　★=纯买入持有", fontsize=11)
    ax.set_ylabel("年化", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25); ax.legend(fontsize=10, ncol=2)
    ax.set_title("③ 加底仓：3/4 条腿单调变差（红利是唯一例外）", fontsize=13, weight="bold")

    ax = fig.add_subplot(gs[2, 2:])
    for nm in LEGS:
        rs = [K.R(nm, base=b) for b in BASES]
        ax.plot([b * 100 for b in BASES], [r["nloss"] / r["nyr"] for r in rs], "-o",
                color=COL[nm], lw=1.9, ms=6, label=f"{nm}（{rs[0]['nyr']}年）")
        bh = K.bh(nm)
        ax.plot(100, bh["nloss"] / bh["nyr"], "*", ms=15, color=COL[nm], mec="white", mew=1.0)
    ax.set_xlabel("底仓比例（%）　★=纯买入持有", fontsize=11)
    ax.set_ylabel("自然年亏损年数占比", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25); ax.legend(fontsize=9.5, ncol=2)
    ax.set_title("④ 换成他自己的目标函数（「没有任何一年会亏损」）检验：同样不改善",
                 fontsize=13, weight="bold")

    # ── ⑤ 量能地量 ───────────────────────────────────────────
    root = Path("results")
    cw = pd.read_csv(root / "crowding_daily.csv", dtype={"trade_date": str}).sort_values("trade_date")
    cw["amt"] = pd.to_numeric(cw.total_amt_yi)
    hs, _ = prep(root, "index_dump_000300_SH.csv", "close", None)
    m = cw.merge(hs[["trade_date", "c"]], on="trade_date", how="inner").reset_index(drop=True)
    m["pct3y"] = m.amt.rolling(750, min_periods=250).rank(pct=True)
    t = pd.to_datetime(m.trade_date)

    ax = fig.add_subplot(gs[3, :2])
    ax.plot(t, m.amt / 10000, lw=1.0, color="#34495e")
    for th, c, lab in ((1.0, RED, "1.0 万亿"), (1.5, AMBER, "1.5 万亿"), (2.0, GREEN, "2.0 万亿")):
        ax.axhline(th, color=c, ls="--", lw=1.5)
        ax.text(t.iloc[3], th, f" 他说的地量线 {lab}", color=c, fontsize=9, va="bottom", weight="bold")
    ax.set_ylabel("全A单日成交额（万亿）", fontsize=11)
    ax.grid(alpha=.25); ax.tick_params(labelsize=9)
    ax.set_title("⑤ 名义阈值随制度漂移：2016~18 天天满足「≤1万亿」，2026 至今 0 天",
                 fontsize=13, weight="bold")

    ax = fig.add_subplot(gs[3, 2:])
    c = m.c.values
    HZ = [20, 60, 120, 250]

    def fwd(mask):
        idx = np.where(pd.Series(mask).fillna(False).values)[0]
        return [float(np.mean([c[i + h] / c[i] - 1 for i in idx if i + h < len(c)])) for h in HZ], len(idx)

    base, nb = fwd(pd.Series(True, index=m.index))
    ax.plot(HZ, base, "-o", color=GREY, lw=2.2, ms=7, label=f"全样本基准（n={nb}）")
    for q, c2 in ((.05, "#7b1fa2"), (.10, "#c0392b"), (.20, "#e67e22"), (.30, "#16a085")):
        v, n = fwd(m.pct3y <= q)
        ax.plot(HZ, v, "-o", color=c2, lw=1.9, ms=6, label=f"成交额≤3年{int(q*100)}分位（n={n}）")
    ax.set_xlabel("前瞻交易日", fontsize=11); ax.set_ylabel("沪深300 平均涨幅", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.set_xticks(HZ); ax.grid(alpha=.25); ax.legend(fontsize=9.5)
    ax.set_title("⑥ 但换成滚动分位口径，梯度单调且很陡（与现有两腿仅重叠 5~10%）",
                 fontsize=13, weight="bold")

    fig.text(0.5, 0.028,
             "本图只呈现可行域标定（SOP 第一步）的边界，不是裁决。"
             "定裁须过已写死的 E51~E55 判据：分半不翻转 · WARM 四档不变号 · 起点敏感 ≥80% 同号 · "
             "事件级独立样本 · 多触发器竞争口径并报。",
             ha="center", fontsize=12, color="#5d4037",
             bbox=dict(fc="#fff8e1", ec=AMBER, alpha=.95, boxstyle="round,pad=0.45"))

    out = outd / "review_disposition.png"
    fig.savefig(out, dpi=110, bbox_inches="tight")
    print(f"saved {out}")


if __name__ == "__main__":
    main()
