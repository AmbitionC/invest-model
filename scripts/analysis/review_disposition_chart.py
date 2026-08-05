# -*- coding: utf-8 -*-
"""三方评审处置·交叉校准后的裁决图（2026-08-05 重制）。

**这一版替换了 08-05 早先那张「可行域标定图」**——那张图里的命题 B/C/D 三格
在交叉校准（V3 独立复算 / V4 事件级统计 / RED 红队）中被推翻，逐条更正见
docs/model_change_proposals.md §7。本图只画经过校准的读数。

七格：
  ① 两个卖出闸画在真实价格上（科创50 本来就一致，其余三腿差 30%）
  ② 卖出闸的收益-回撤权衡（真·生产混合口径单列）
  ③ 加码斜率：换成跨臂可比的尺子后，「换基数能把钱送到更低价位」不成立
  ④ 底仓（生产闸口径）：2/4 恶化 2/4 改善，不是原先写的 3/4 恶化
  ⑤ 棘轮复位 vs 不复位：一个实现开关翻转了「腿间翻转」这个头条结论
  ⑥ 量能：日级读数 vs 事件级读数（E55 的真否决点）
  ⑦ 裁决汇总

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

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from long_window_backtest import FONT, prep  # noqa: E402

import contextlib, io  # noqa: E402
with contextlib.redirect_stdout(io.StringIO()):   # 标定脚本 import 时会打表，这里只要它的函数
    import review_disposition_calib as K  # noqa: E402

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}
GREEN, RED, GREY, AMBER = "#1e8449", "#c0392b", "#7f8c8d", "#b7950b"
LEGS = list(K.data)
PROD = {"沪深300": 1.00, "创业板": 1.10, "科创50": 1.30, "红利": 1.00}   # 08-05 统一前的旧生产值（本图记录的是当时的事故状态）
BACK = {"沪深300": 1.30, "创业板": 1.43, "科创50": 1.30, "红利": 1.30}   # 回测引擎默认值
_SRC = (HERE / "review_disposition_calib.py").read_text()
_CUT = 'BAR = "="'


def _ns(extra: str = "", repl: tuple[str, str] | None = None) -> dict:
    src = _SRC
    if repl:
        src = src.replace(*repl)
    src = extra + src
    ns: dict = {}
    exec(compile(src.split(_CUT)[0], "calib_variant", "exec"), ns)
    return ns


def run_reset(nm: str, g: float, reset: bool) -> dict:
    """棘轮复位对照臂——复位＝价格跌回卖出闸下方时清空 nxt，下一轮上涨重新起算。"""
    ns = _ns(repl=("                if nxt is not None and ci >= nxt:",
                   "                if RESET and nxt is not None and ci < lvl:\n"
                   "                    nxt = None\n"
                   "                if nxt is not None and ci >= nxt:"))
    ns2 = _ns(extra="RESET = %r\n" % reset,
              repl=("                if nxt is not None and ci >= nxt:",
                    "                if RESET and nxt is not None and ci < lvl:\n"
                    "                    nxt = None\n"
                    "                if nxt is not None and ci >= nxt:"))
    del ns
    df, ret = ns2["data"][nm]
    return ns2["run"](df, ret, nm, ns2["ST"][nm], ns2["EN"][nm], ns2["MODE"][nm], grid=g)


def deep_bucket(nm: str, size: str) -> float:
    """跨臂可比的尺子：投在「全窗价格最低 5% 分位桶」内的资金占比。
    （原先用「本臂自己的最低买入价 +5%」，分母逐臂不同、不可比 —— 已撤回，见 §7.1①）"""
    ns = _ns(repl=("return dict(ann=ann", "return dict(buys=buys, ann=ann"))
    df, ret = ns["data"][nm]
    i0 = int(np.searchsorted(df.trade_date.values, ns["ST"][nm]))
    q05 = float(np.quantile(df.c.values[i0:], 0.05))
    b = ns["run"](df, ret, nm, ns["ST"][nm], ns["EN"][nm], ns["MODE"][nm], size=size)["buys"]
    w = sum(a for _, a in b)
    return sum(a for p, a in b if p <= q05) / w


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="results")
    a = ap.parse_args()
    outd = Path(a.out_dir); outd.mkdir(parents=True, exist_ok=True)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False

    fig = plt.figure(figsize=(19, 25))
    gs = fig.add_gridspec(5, 4, height_ratios=[1.05, 1, 1, 1, 0.78], hspace=0.55, wspace=0.30,
                          top=0.905, bottom=0.045, left=0.055, right=0.975)
    fig.suptitle("三方评审的处置：交叉校准后的裁决", fontsize=24, weight="bold", y=0.965)
    fig.text(0.5, 0.940,
             "V3 独立复算（292 格零差异）· V4 事件级统计 · RED 红队　｜　"
             "本图替换 08-05 早先那张标定图：命题 B/C/D 三格的原读数被推翻，此处只画校准后的数",
             ha="center", fontsize=12, color=GREY)

    # ── ① 两个卖出闸画在真实价格上 ──────────────────────────────
    for k, nm in enumerate(LEGS):
        ax = fig.add_subplot(gs[0, k])
        df, _ = K.data[nm]
        d = df[df.trade_date >= "20210101"]
        t = pd.to_datetime(d.trade_date)
        last, med = float(df.c.iloc[-1]), float(df.exp.iloc[-1])
        ax.plot(t, d.c, lw=1.2, color=COL[nm])
        ax.plot(t, d.exp, lw=1.0, color=GREY, ls=":")
        p, b = med * PROD[nm], med * BACK[nm]
        ax.axhline(p, color=RED, lw=2.0)
        ax.axhline(b, color=AMBER, ls="--", lw=2.0)
        ax.fill_between(t, p, b, color=AMBER, alpha=.13)
        ax.plot(t.iloc[-1], last, "o", ms=9, color=COL[nm], mec="white", mew=1.4, zorder=5)
        gap = b / p - 1
        ok = gap < 1e-6
        ax.set_title(f"{nm}　生产 {p:.0f} vs 回测 {b:.0f}"
                     + ("　【本来就一致】" if ok else f"　差 {gap:+.0%}"),
                     fontsize=11.5, weight="bold", color=GREEN if ok else RED)
        ax.set_ylim(min(d.c.min(), p) * .92, max(d.c.max(), b) * 1.08)
        ax.grid(alpha=.22); ax.tick_params(labelsize=8.5)
        import matplotlib.dates as mdates
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%y"))
        if k == 0:
            ax.text(0.02, 0.97, "红实线=每天提示给你的线\n黄虚线=收益数字用的线",
                    transform=ax.transAxes, va="top", fontsize=9, color="#5d4037",
                    bbox=dict(fc="#fff8e1", ec=AMBER, alpha=.95, boxstyle="round,pad=0.3"))
    fig.text(0.5, 0.742,
             "① 科创50 本来就是 ×1.30 —— 所以「统一到 ×1.00 不改生产行为」是假的："
             "那会用一个漏改的残留覆盖 08-02 一次有据可查的决定。闸位收敛＝真实的风险偏好选择，交 owner。",
             ha="center", fontsize=12.5, weight="bold", color=RED)

    # ── ② 卖出闸权衡 ─────────────────────────────────────────
    ax = fig.add_subplot(gs[1, :2])
    GATES = [None, 1.00, 1.15, 1.30, 1.50]
    for nm in LEGS:
        xs, ys = [], []
        for mu in GATES:
            kw = dict(no_sell=True) if mu is None else dict(sell_mul=mu * (1.10 if nm == "创业板" else 1.0))
            r = K.R(nm, **kw)
            xs.append(abs(r["mdd"])); ys.append(r["ann"])
        ax.plot(xs, ys, "-o", color=COL[nm], lw=1.8, ms=6, label=nm)
        rp = K.R(nm, sell_mul=1.10 if nm == "创业板" else PROD[nm])
        ax.plot(abs(rp["mdd"]), rp["ann"], "s", ms=12, mfc="none", mec=COL[nm], mew=2.4, zorder=6)
    ax.plot([], [], "s", ms=11, mfc="none", mec="k", mew=2.0, label="□ = 真·生产口径")
    ax.set_xlim(0.10, 0.75)
    ax.set_xlabel("最大回撤（绝对值·越左越好）", fontsize=11); ax.set_ylabel("年化", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25); ax.legend(fontsize=9.5, ncol=2, loc="lower right")
    ax.set_title("② 卖出闸：两个闸位下夏普几乎无差别，差别在收益/回撤/换手——风险偏好，不是对错题",
                 fontsize=12.5, weight="bold")

    # ── ③ 加码斜率：换可比尺子 ─────────────────────────────────
    ax = fig.add_subplot(gs[1, 2:])
    SZ = [("cur", "当前现金×比例（现状）", "#c0392b"), ("init", "起始资金×比例", "#2980b9"),
          ("ramp", "越深越大", "#1e8449")]
    w, xs = 0.26, np.arange(len(LEGS))
    for j, (sz, lab, c) in enumerate(SZ):
        vals = [deep_bucket(nm, sz) for nm in LEGS]
        bars = ax.bar(xs + (j - 1) * w, vals, w * .92, color=c, alpha=.85, label=lab)
        for x, v in zip(bars, vals):
            ax.text(x.get_x() + x.get_width() / 2, v + .005, f"{v:.0%}",
                    ha="center", fontsize=9, weight="bold", color=c)
    ax.set_xticks(xs); ax.set_xticklabels(LEGS, fontsize=11.5, weight="bold")
    ax.set_ylabel("投在「全窗价格最低 5% 分位桶」的资金占比", fontsize=10)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25, axis="y"); ax.legend(fontsize=9.5, loc="upper left")
    ax.set_title("③ 换成跨臂可比的尺子后：红利三臂全 0%，「换基数能把钱送到更低价位」不成立",
                 fontsize=12.5, weight="bold")

    # ── ④ 底仓（生产闸口径）─────────────────────────────────────
    BASES = [0.0, .25, .50, .75, 1.0]
    ax = fig.add_subplot(gs[2, :2])
    for nm in LEGS:
        mu = 1.10 if nm == "创业板" else PROD[nm]
        ys = [K.R(nm, base=b, sell_mul=mu)["ann"] for b in BASES]
        ax.plot([b * 100 for b in BASES], [(y - ys[0]) * 100 for y in ys], "-o",
                color=COL[nm], lw=1.9, ms=6, label=f"{nm}（现状 {ys[0]:.2%}）")
    ax.axhline(0, color=GREY, ls=":", lw=1.4)
    ax.set_xlabel("底仓比例（%）", fontsize=11)
    ax.set_ylabel("年化相对现状的变化（pp）", fontsize=11)
    ax.grid(alpha=.25); ax.legend(fontsize=9.5, ncol=2, loc="lower left")
    ax.set_title("④ 底仓（改用真·生产闸口径）：2 腿恶化 2 腿改善——「3/4 单调恶化」已撤回",
                 fontsize=12.5, weight="bold")
    ax.text(0.98, 0.97,
            "更要紧的是：底仓与增强仓是两本不交互的账，\n"
            "净值 = X×买入持有 + (1-X)×现状策略（实测相对差 <1e-3）\n"
            "=> 这张表是恒等式，回答不了「底仓是容错来源」这个指控",
            transform=ax.transAxes, fontsize=9.5, color="#5d4037", va="top", ha="right",
            bbox=dict(fc="#fff8e1", ec=AMBER, alpha=.95, boxstyle="round,pad=0.35"))

    # ── ⑤ 棘轮复位 vs 不复位 ────────────────────────────────────
    ax = fig.add_subplot(gs[2, 2:])
    now = {nm: K.R(nm) for nm in LEGS}
    noR = {nm: run_reset(nm, .02, False) for nm in LEGS}
    yesR = {nm: run_reset(nm, .02, True) for nm in LEGS}
    w, xs = 0.26, np.arange(len(LEGS))
    for j, (dd, lab, c) in enumerate(((now, "月末5%（现状）", "#7f8c8d"),
                                      (noR, "网格·不复位（原实现）", "#c0392b"),
                                      (yesR, "网格·复位", "#1e8449"))):
        vals = [abs(dd[nm]["mdd"]) for nm in LEGS]
        bars = ax.bar(xs + (j - 1) * w, vals, w * .92, color=c, alpha=.85, label=lab)
        for x, v in zip(bars, vals):
            ax.text(x.get_x() + x.get_width() / 2, v + .006, f"{v:.0%}",
                    ha="center", fontsize=8.5, weight="bold", color=c)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{nm}\n夏普 {now[nm]['sharpe']:.2f} → {noR[nm]['sharpe']:.2f} → {yesR[nm]['sharpe']:.2f}"
                        for nm in LEGS], fontsize=9.5, weight="bold")
    ax.set_ylabel("最大回撤（绝对值）", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25, axis="y"); ax.legend(fontsize=9.5)
    ax.set_title("⑤ 棘轮「跌回闸下就复位」这一个开关：创业板回撤 -56.8%→-27.5%，四腿夏普全改善",
                 fontsize=12.5, weight="bold")

    # ── ⑥ 量能：日级 vs 事件级 ──────────────────────────────────
    root = Path("results")
    cw = pd.read_csv(root / "crowding_daily.csv", dtype={"trade_date": str}).sort_values("trade_date")
    cw["amt"] = pd.to_numeric(cw.total_amt_yi)
    hs, _ = prep(root, "index_dump_000300_SH.csv", "close", None)
    m = cw.merge(hs[["trade_date", "c"]], on="trade_date", how="inner").reset_index(drop=True)
    m["pct3y"] = m.amt.rolling(750, min_periods=250).rank(pct=True)
    c = m.c.values
    idx = np.where((m.pct3y <= 0.10).fillna(False).values)[0]
    eps, cur = [], [idx[0]]
    for i in idx[1:]:
        if i - cur[-1] <= 60:
            cur.append(i)
        else:
            eps.append(cur); cur = [i]
    eps.append(cur)

    ax = fig.add_subplot(gs[3, :2])
    names, first, mid, lastd = [], [], [], []
    for e in eps:
        if e[0] + 250 >= len(c):
            continue
        names.append(m.trade_date[e[0]][:6])
        first.append(c[e[0] + 250] / c[e[0]] - 1)
        mid.append(float(np.mean([c[i + 250] / c[i] - 1 for i in e if i + 250 < len(c)])))
        j2 = min(e[-1], len(c) - 251)
        lastd.append(c[j2 + 250] / c[j2] - 1)
    xs2 = np.arange(len(names)); w = 0.26
    for j, (v, lab, col2) in enumerate(((first, "从 episode 首日算", "#c0392b"),
                                        (mid, "episode 内逐日平均", "#e67e22"),
                                        (lastd, "从 episode 末日算", "#1e8449"))):
        ax.bar(xs2 + (j - 1) * w, v, w * .92, color=col2, alpha=.85, label=lab)
    ax.axhline(0, color="k", lw=1.0)
    ax.set_xticks(xs2); ax.set_xticklabels(names, fontsize=10.5, weight="bold")
    ax.set_ylabel("此后 250 交易日沪深300 涨幅", fontsize=10.5)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.grid(alpha=.25, axis="y"); ax.legend(fontsize=9.5)
    ax.set_title("⑥ 量能：日级 +14.66% 是重叠窗口加权；按事件算，读法一变结论就变号",
                 fontsize=12.5, weight="bold")

    ax = fig.add_subplot(gs[3, 2:])
    HZ = [20, 60, 120, 250]

    def fwd(mask):
        ii = np.where(pd.Series(mask).fillna(False).values)[0]
        return [float(np.mean([c[i + h] / c[i] - 1 for i in ii if i + h < len(c)])) for h in HZ], len(ii)

    base, nb = fwd(pd.Series(True, index=m.index))
    ax.plot(HZ, base, "-o", color=GREY, lw=2.2, ms=7, label=f"全样本基准（n={nb} 日）")
    for q, c2 in ((.05, "#7b1fa2"), (.10, "#c0392b"), (.20, "#e67e22"), (.30, "#16a085")):
        v, n = fwd(m.pct3y <= q)
        ax.plot(HZ, v, "-o", color=c2, lw=1.9, ms=6, label=f"成交额≤3年{int(q*100)}分位（n={n} 日）")
    ax.set_xlabel("前瞻交易日", fontsize=11); ax.set_ylabel("沪深300 平均涨幅", fontsize=11)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.set_xticks(HZ); ax.grid(alpha=.25); ax.legend(fontsize=9.5)
    ax.set_title(f"⑥b 日级梯度确实单调——但 {len(idx)} 个触发日只合成 {len(eps)} 个 episode，"
                 "有效独立事件 3 个", fontsize=12.5, weight="bold")

    # ── ⑦ 裁决汇总 ──────────────────────────────────────────
    ax = fig.add_subplot(gs[4, :]); ax.axis("off")
    rows = [
        ("生产代码", "零改动", "三条路线无一支持任何生产变更", GREEN),
        ("P58 卖出闸", "呈 owner 决策（治理自动裁定已撤回）", "科创50 生产本就 ×1.30 =>「不改行为」是事实错误", AMBER),
        ("E52 加码斜率", "本轮无效，重测", "判据指标分母逐臂不同 + 臂集缺「现金比例随折价放大」族", AMBER),
        ("E53 底仓", "不加底仓（方向成立）· 论证撤回 · P27 的 25% 不动", "实现是两本不交互的账＝恒等式；25% 档 Δ-0.11pp、CI 跨零", AMBER),
        ("E54 网格", "本轮无效，重测（棘轮复位作显式对照臂）", "复位后「腿间翻转」消失、四腿夏普全部改善", AMBER),
        ("E55 量能", "FAIL，不接入　★本轮唯一有效裁决", "250 日为正仅 2/5、p=0.08~0.20、有效独立事件 3 个", RED),
        ("P63 杠杆退出腿", "维持登记", "执行纪律层，未被本轮评审触及", GREY),
        ("SOP", "新增 9 条修订", "判据不得挂在已被标定判死的第一条后面（本轮最重要的产出）", GREEN),
    ]
    ax.text(0.5, 1.03, "⑦ 本轮最终处置", ha="center", fontsize=15, weight="bold", transform=ax.transAxes)
    for i, (k2, v, why, c2) in enumerate(rows):
        y = 0.90 - i * 0.115
        ax.text(0.005, y, k2, fontsize=11.5, weight="bold", transform=ax.transAxes, va="center")
        ax.text(0.150, y, v, fontsize=11.5, color=c2, weight="bold", transform=ax.transAxes, va="center")
        ax.text(0.560, y, why, fontsize=10.5, color="#4a4a4a", transform=ax.transAxes, va="center")

    fig.text(0.5, 0.012,
             "本轮所有错误都在口径、指标定义与实现选择上——三条路线对引擎算术的复算是 292 格零差异。"
             "「先标定再写判据」在标定与定裁共用同一套指标时，会系统性生产出只能复述标定结论的判据。",
             ha="center", fontsize=12, color="#5d4037",
             bbox=dict(fc="#fff8e1", ec=AMBER, alpha=.95, boxstyle="round,pad=0.45"))

    out = outd / "review_disposition.png"
    fig.savefig(out, dpi=108)
    print(f"saved {out}")


if __name__ == "__main__":
    main()
