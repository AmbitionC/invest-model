# -*- coding: utf-8 -*-
"""陈老师宽基体系内化 · 结论图（2026-08-04）

四张图：
  [1] 内化过账（A 已内化 / B 已测未过 / C 本次新内化 / D 待跑 / E 框架级）
  [2] 基线修正前后对照（红队三条致命发现）
  [3] 回撤的诚实读数（全窗 vs 策略有仓子区间）
  [4] P51 首笔容错上限曲线（与他实盘口算 37% 的对照）
只读 chen_strategy/ 下 CSV，不落库、不联网。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager

FONT = "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"


def first_lot_cap(last, med):
    if last <= med:
        return 1.0
    d = 0.90 * med
    den = 1.0 / last - 1.0 / d
    return 0.0 if den >= 0 else max(0.0, min(1.0, (1.0 / med - 1.0 / d) / den))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results/chen_internalization.png")
    a = ap.parse_args()
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False
    # WenQuanYi 无 U+2212，全文一律用 ASCII 连字符

    fig = plt.figure(figsize=(16, 13))
    gs = fig.add_gridspec(2, 2, hspace=0.38, wspace=0.26, top=0.90)
    fig.suptitle("陈老师宽基指数体系 → invest-model 内化过账（2026-08-04）",
                 fontsize=17, weight="bold", y=0.965)

    # [1] 内化过账
    ax = fig.add_subplot(gs[0, 0])
    cats = ["E 框架级\n（指导设计）", "D 已登记待跑", "C 本次新内化\n（执行纪律层）",
            "B 已测未过\n（入库负结果）", "A 已在生产"]
    vals = [5, 5, 3, 13, 8]
    cols = ["#7f8c8d", "#d4ac0d", "#1e8449", "#c0392b", "#2980b9"]
    b = ax.barh(cats, vals, color=cols)
    ax.bar_label(b, fmt="%d 项", padding=4, fontsize=11)
    ax.set_xlim(0, 16)
    ax.set_title("① 全库宽基方法论过账：34 条", fontsize=13, weight="bold")
    ax.set_xlabel("条目数")
    ax.text(0.98, 0.06, "B 类之所以这么厚 ＝ 此前拿收益判据去卡执行纪律\n（判据错配）",
            transform=ax.transAxes, ha="right", fontsize=9.5, color="#c0392b",
            bbox=dict(fc="#fdedec", ec="#c0392b", alpha=.9))

    # [2] 基线修正前后
    ax = fig.add_subplot(gs[0, 1])
    lbl = ["科创50 超额\n(pp)", "组合年化\n(19.5y, %)", "沪深300 回撤\n对比缺口(pp)"]
    old = [8.77, 8.54, 39.3]
    new = [4.37, 6.45, 13.7]
    x = np.arange(3)
    ax.bar(x - .19, old, .38, color="#e59866", label="修正前（作废）")
    ax.bar(x + .19, new, .38, color="#1e8449", label="修正后（对外口径）")
    for i, (o, n) in enumerate(zip(old, new)):
        ax.text(i - .19, o + .6, f"{o:.2f}", ha="center", fontsize=10, color="#a04000")
        ax.text(i + .19, n + .6, f"{n:.2f}", ha="center", fontsize=10, weight="bold")
    ax.set_xticks(x); ax.set_xticklabels(lbl, fontsize=10)
    ax.legend(fontsize=10); ax.set_ylim(0, 46)
    ax.set_title("② 红队三条致命发现的修正幅度", fontsize=13, weight="bold")
    ax.text(0.5, 0.80, "F1 起点硬编码 ｜ F2 不可实现组合 ｜ F3 空仓期伪影",
            transform=ax.transAxes, ha="center", fontsize=9.5, color="#7f8c8d")

    # [3] 回撤诚实读数
    ax = fig.add_subplot(gs[1, 0])
    legs = ["沪深300", "创业板", "科创50", "红利"]
    s_all = [33.0, 43.8, 20.1, 33.7]
    b_all = [72.3, 69.7, 62.7, 71.6]
    b_pos = [46.7, 69.7, 28.1, 45.7]
    x = np.arange(4)
    ax.bar(x - .26, s_all, .26, color="#2980b9", label="策略 mdd")
    ax.bar(x, b_all, .26, color="#f1948a", label="买持 mdd（全窗·含空仓期伪影）")
    ax.bar(x + .26, b_pos, .26, color="#c0392b", label="买持 mdd（策略有仓子区间·诚实读数）")
    ax.set_xticks(x); ax.set_xticklabels(legs, fontsize=11)
    ax.set_ylabel("最大回撤（%，日频采样）")
    ax.legend(fontsize=9, loc="upper left", bbox_to_anchor=(0.0, 0.90))
    ax.set_title("③「70% 压到 17~44%」作废：诚实读数是 -33% vs -47%",
                 fontsize=13, weight="bold")
    ax.set_ylim(0, 100)
    ax.text(0.5, 0.965, "沪深300/红利的买持 -72% 全发生在 2008，而策略当时持仓 0~4%＝还没开始买，不是风控",
            transform=ax.transAxes, ha="center", fontsize=9.5, color="#c0392b",
            bbox=dict(fc="#fdedec", ec="#c0392b", alpha=.9))

    # [4] P51 首笔上限
    ax = fig.add_subplot(gs[1, 1])
    med = 1.0
    r = np.linspace(0.75, 1.60, 400)
    f = [first_lot_cap(v, med) for v in r]
    ax.plot(r, np.array(f) * 100, lw=2.6, color="#1e8449")
    ax.axvline(1.0, color="#2980b9", ls="--", lw=1.4)
    ax.text(1.005, 92, "P26 中位线（安全线）", fontsize=9.5, color="#2980b9")
    ax.axvline(1.30, color="#c0392b", ls="--", lw=1.4)
    ax.text(1.305, 92, "卖出闸 1.30×", fontsize=9.5, color="#c0392b")
    ax.axvline(0.90, color="#8e44ad", ls=":", lw=1.4)
    ax.text(0.905, 60, "P28 深危机口径\n0.90×（极端落点）", fontsize=9, color="#8e44ad")
    ax.scatter([1.30], [32.5], s=80, color="#c0392b", zorder=5)
    ax.annotate("系统解 32.5%\n他实盘口算 37%\n（同一约束的两个解）",
                xy=(1.30, 32.5), xytext=(1.36, 55), fontsize=10,
                arrowprops=dict(arrowstyle="->", color="#c0392b"),
                bbox=dict(fc="#fdedec", ec="#c0392b", alpha=.9))
    ax.set_xlabel("收盘 / expanding 中位线")
    ax.set_ylabel("首笔最多可动用资金（%）")
    ax.set_ylim(0, 105); ax.grid(alpha=.3)
    ax.set_title("④ P51 容错自检：首笔上限 f <= (1/m-1/d)/(1/p-1/d)",
                 fontsize=13, weight="bold")

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=125, bbox_inches="tight")
    print(f"saved {out}")


if __name__ == "__main__":
    main()
