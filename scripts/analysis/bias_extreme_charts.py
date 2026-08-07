# -*- coding: utf-8 -*-
"""把七个指数的**乖离率极值点标注在股价图上**，一个指数一张图。

owner 2026-08-06：「先把所有指数、所有出现乖离率极值的位置，在股价图里标注出来。分别画。」

极值口径按 owner 此前指定的**近十年滚动窗口**（2500 交易日，预热 750）：
    z = (bias60 − 滚动均值) / 滚动标准差      低尾 z ≤ −2 · 高尾 z ≥ +2
这与 E60 的入场口径同源，所以图上的绿点就是 E60 实际会买的那些天。

⚠️ 图只是把读数画出来，**不构成信号**：E60 已判 FAIL（六条判据过五条，卡在触发频次），
乖离率至此六个口径全部判死。红点尤其不可作卖出依据——E37/E57 已实证涨到极值之后是继续涨。

产物 `results/bias_charts/<指数>.png`。只读 results/bias_meanrev/*.csv，不落库、不联网。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates          # noqa: E402
import matplotlib.pyplot as plt            # noqa: E402
import numpy as np                         # noqa: E402
import pandas as pd                        # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))
from e57_bias_top3_leg import UNIVERSE      # noqa: E402

plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
plt.rcParams["axes.unicode_minus"] = False

WIN, WARM, ZCUT = 2500, 750, 2.0
C_LOW, C_HIGH, C_PRICE, C_MA = "#1F9C6B", "#CE4A4A", "#2B3A55", "#9AA5B1"
LABEL_TOP = 3          # 每尾标注最极端的前 N 个 episode
EP_GAP = 20            # 相邻 ≤20 交易日算同一次事件


def episodes(idx: np.ndarray, val: np.ndarray, mode: str) -> list[int]:
    """把连续成簇的触发日合并，取簇内最极端的一天作代表。"""
    if len(idx) == 0:
        return []
    groups: list[list[int]] = [[int(idx[0])]]
    for j in idx[1:]:
        (groups[-1].append(int(j)) if int(j) - groups[-1][-1] <= EP_GAP
         else groups.append([int(j)]))
    pick = min if mode == "low" else max
    return [pick(g, key=lambda i: val[i]) for g in groups]


def draw(nm: str, d: pd.DataFrame, out: Path) -> dict:
    dt = pd.to_datetime(d.trade_date, format="%Y%m%d")
    c, b = d.close.to_numpy(float), d.bias60.to_numpy(float)
    bs = pd.Series(b)
    mu = bs.rolling(WIN, min_periods=WARM).mean()
    sd = bs.rolling(WIN, min_periods=WARM).std(ddof=1)
    z = ((bs - mu) / sd).to_numpy()

    lo = np.flatnonzero(z <= -ZCUT)
    hi = np.flatnonzero(z >= ZCUT)
    lo_ep, hi_ep = episodes(lo, b, "low"), episodes(hi, b, "high")

    fig, (ax, ax2) = plt.subplots(
        2, 1, figsize=(15, 9), sharex=True, height_ratios=[2.6, 1],
        gridspec_kw=dict(hspace=0.07))

    # ── 上：股价 + 极值点 ──
    ax.plot(dt, c, lw=1.0, color=C_PRICE, label="收盘价")
    ax.plot(dt, d.ma60, lw=0.8, color=C_MA, alpha=.9, label="60 日均线")
    ax.set_yscale("log")
    ax.scatter(dt.iloc[lo], c[lo], s=26, marker="^", color=C_LOW, zorder=5,
               edgecolors="white", linewidths=.4,
               label=f"乖离率低尾极值 z<=-{ZCUT:.0f}（{len(lo)} 天 / {len(lo_ep)} 次）")
    ax.scatter(dt.iloc[hi], c[hi], s=26, marker="v", color=C_HIGH, zorder=5,
               edgecolors="white", linewidths=.4,
               label=f"乖离率高尾极值 z>=+{ZCUT:.0f}（{len(hi)} 天 / {len(hi_ep)} 次）")

    # 每尾标注最极端的前 N 次
    for eps, col, mode, dy in ((lo_ep, C_LOW, "low", -34), (hi_ep, C_HIGH, "high", 26)):
        top = sorted(eps, key=lambda i: b[i], reverse=(mode == "high"))[:LABEL_TOP]
        for i in top:
            ax.annotate(f"{dt.iloc[i]:%Y-%m-%d}\n{b[i]:+.1%}",
                        xy=(dt.iloc[i], c[i]), xytext=(0, dy), textcoords="offset points",
                        ha="center", fontsize=8, color=col, fontweight="bold",
                        arrowprops=dict(arrowstyle="-", color=col, lw=.7, alpha=.6))

    ax.set_title(f"{nm} —— 乖离率极值点标注（近十年滚动窗口 |z|>={ZCUT:.0f}）\n"
                 f"* 只是读数，不是信号：E60 已判 FAIL；红点尤其不可作卖出依据"
                 f"（E37/E57 实证涨到极值后是继续涨）",
                 fontsize=12, pad=12, loc="left")
    ax.set_ylabel("指数点位（对数轴）")
    ax.legend(loc="upper left", fontsize=9, framealpha=.9)
    ax.grid(alpha=.18, which="both")

    # ── 下：乖离率 + ±2σ 带 ──
    ax2.plot(dt, b, lw=.8, color=C_PRICE, label="乖离率 = 收盘 / MA60 - 1")
    ax2.fill_between(dt, mu - ZCUT * sd, mu + ZCUT * sd, color="#7C8BA1", alpha=.16,
                     label=f"+/-{ZCUT:.0f}σ 带（近十年滚动）")
    ax2.axhline(0, color="#556", lw=.7, ls="--", alpha=.7)
    ax2.scatter(dt.iloc[lo], b[lo], s=16, marker="^", color=C_LOW, zorder=5)
    ax2.scatter(dt.iloc[hi], b[hi], s=16, marker="v", color=C_HIGH, zorder=5)
    ax2.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax2.set_ylabel("乖离率")
    ax2.legend(loc="lower left", fontsize=8, framealpha=.9, ncol=2)
    ax2.grid(alpha=.18)
    ax2.xaxis.set_major_locator(mdates.YearLocator(2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    fig.text(0.995, 0.012,
             f"{d.trade_date.iloc[0]}~{d.trade_date.iloc[-1]}｜价格指数不复权｜"
             f"σ 带前 {WARM} 个交易日预热期内无定义故不画",
             ha="right", fontsize=7.5, color="#6B7280")
    fig.tight_layout()
    p = out / f"{nm}.png"
    fig.savefig(p, dpi=135, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return dict(nm=nm, path=p, n_lo=len(lo), n_hi=len(hi),
                ep_lo=len(lo_ep), ep_hi=len(hi_ep),
                last_lo=(str(d.trade_date.iloc[lo[-1]]) if len(lo) else "—"),
                last_hi=(str(d.trade_date.iloc[hi[-1]]) if len(hi) else "—"),
                deepest=(f"{d.trade_date.iloc[int(np.nanargmin(b))]} {np.nanmin(b):+.1%}"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results/bias_meanrev")
    ap.add_argument("--out", default="results/bias_charts")
    a = ap.parse_args()
    root, out = Path(a.data), Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"{'指数':>9s}{'低尾天数':>9s}{'次数':>6s}{'高尾天数':>9s}{'次数':>6s}"
          f"{'最近低尾':>11s}{'最近高尾':>11s}{'历史最深':>22s}")
    for nm, _, _, _ in UNIVERSE:
        d = pd.read_csv(root / f"{nm}.csv", dtype={"trade_date": str})
        r = draw(nm, d, out)
        print(f"{nm:>9s}{r['n_lo']:>9d}{r['ep_lo']:>6d}{r['n_hi']:>9d}{r['ep_hi']:>6d}"
              f"{r['last_lo']:>11s}{r['last_hi']:>11s}{r['deepest']:>22s}")
    print(f"\n图 → {out}/  （7 张）")


if __name__ == "__main__":
    main()
