# -*- coding: utf-8 -*-
"""四腿宽基策略·历史买卖全图（生产配置口径）

2026-08-02 重制，修掉上一版的制图缺陷：
  - 标题写指数名（ETF 只作执行标的注明），纵轴不再是"挂 ETF 代码画指数点位"
  - 点大小跨面板统一比例尺，并给出尺寸图例
  - 四个面板共用同一 x 轴范围（科创50 2020-06 前无数据即留空，不改范围）
  - 纵轴改对数刻度（跨 11 年 3~4 倍涨幅，线性会把早期压扁）
  - **补画恐慌买所用的价格闸线（滚动5年中位线）**——上一版只画了锚线，
    导致"恐慌买其实买在历史高位"这个问题肉眼看不出来
  - 每腿加持仓占比子图（收益里多少来自股票、多少来自货基，一眼可见）
  - 不用 U+2212，避免字体缺字形

配置口径：科创50＝深回撤阶梯 L50 + 恐慌买 + 月减5%（E31 验证配置，2026-08-02 已回滚）；
红利＝信号用价格指数、收益按全收益 H00922.CSI（E34 数据缺陷 D2 修复）。
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
RF, CASH = 0.02, 0.02
D0, D1 = "20150601", "20260729"
RUNG, FRAC = [0.50, 0.55, 0.60, 0.65], [0.30, 0.35, 0.40, 0.50]
SIZE_K = 4.0          # 全图统一：点面积 = 30 + 金额 × SIZE_K
LEGS = [
    ("沪深300", "hs300.csv", "close", None, "510300", D0, "anchor"),
    ("创业板", "spread_full.csv", "chinext", None, "159915", D0, "anchor"),
    ("科创50", "star50.csv", "close", None, "588000", "20200601", "ladder"),
    ("红利", "000922_csi.csv", "close", "000922_tr.csv", "515080", D0, "anchor"),
]


def prep(root: Path, f: str, col: str) -> pd.DataFrame:
    d = pd.read_csv(root / f, dtype={"trade_date": str}).sort_values("trade_date").reset_index(drop=True)
    d["c"] = pd.to_numeric(d[col])
    c = d.c.values
    d["exp"] = [np.median(c[: i + 1]) if i >= 500 else np.nan for i in range(len(d))]
    d["r1250"] = d.c.rolling(1250).median()
    d["peak"] = d.c.cummax()
    ym = d.trade_date.str[:6]
    d["me"] = (ym != ym.shift(-1)).values
    wk = pd.to_datetime(d.trade_date).dt.isocalendar()
    w = wk.week.astype(str) + "-" + wk.year.astype(str)
    d["we"] = (w != w.shift(-1)).values
    return d


def run(df: pd.DataFrame, ret: pd.Series | None, fmap: dict, nm: str, d0: str, mode: str, init=100.0):
    d = df.trade_date.values
    c = df.c.values
    rr = (ret.pct_change().fillna(0).values if ret is not None else None)
    i0, i1 = int(np.searchsorted(d, d0)), len(df)
    cash, units, nav = init, 0.0, 1.0
    last, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, pos, buys, sells = [], [], [], []
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            dt = (pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25
            cash *= (1 + CASH) ** dt
            nav = nav * (1 + rr[i]) if rr is not None else ci
        elif rr is None:
            nav = ci
        r = df.iloc[i]
        for k, fr, _t, tg in [x for x in pend if x[2] == i]:
            if k == "B":
                a = cash * fr
                if a > 0.05:
                    units += a / nav
                    cash -= a
                    buys.append((d[i], ci, a, tg))
            else:
                s = units * fr
                if s > 0:
                    cash += s * nav
                    units -= s
                    sells.append((d[i], ci, s * nav))
        pend = [x for x in pend if x[2] > i]
        sig, f = [], fmap.get(d[i], np.nan)
        panic = f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and ci < r.r1250
        if panic:
            sig.append(("B", 0.50, "panic"))
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
                    sig.append(("B", FRAC[j], "ladder"))
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            sig.append(("B", 0.20, "anchor"))
        mul = 1.30 * 1.10 if nm == "创业板" else 1.30
        if r.me and r.exp == r.exp and ci > r.exp * mul and units > 0:
            sig.append(("S", 0.05, "sell"))
        for k, fr, tag in sig:
            pend.append((k, fr, min(i + 1, i1 - 1), tag))
        tv = cash + units * nav
        curve.append(tv)
        pos.append(units * nav / tv)
    v = np.array(curve)
    pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(d[i1 - 1]) - pd.Timestamp(d[i0])).days / 365.25
    ann = (v[-1] / init) ** (1 / yrs) - 1
    rets = pd.Series(v).pct_change().dropna()
    vol = float(rets.std() * np.sqrt(250))
    mdd = float(((v - pk) / pk).min())
    return dict(dates=d[i0:i1], px=c[i0:i1], sub=df.iloc[i0:i1], ann=ann, vol=vol,
                sharpe=(ann - RF) / vol, mdd=mdd, calmar=ann / abs(mdd) if mdd else np.nan,
                cum=v[-1] / init - 1, curve=v, buys=buys, sells=sells, pos=np.array(pos),
                posavg=float(np.mean(pos)), posend=float(pos[-1]))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    ap.add_argument("--out", default="results/broad_trades.png")
    args = ap.parse_args()
    root = Path(args.data)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False

    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    x0, x1 = pd.Timestamp("2015-06-01"), pd.Timestamp("2026-08-15")

    fig = plt.figure(figsize=(16, 22))
    gs = fig.add_gridspec(8, 1, height_ratios=[3.2, 1.0] * 4, hspace=0.30, top=0.895)
    for k, (nm, f, col, trf, etf, d0, mode) in enumerate(LEGS):
        df = prep(root, f, col)
        ret = None
        if trf:
            tr = pd.read_csv(root / trf, dtype={"trade_date": str})
            tr["c"] = pd.to_numeric(tr.close)
            m = df.merge(tr[["trade_date", "c"]], on="trade_date", suffixes=("", "_tr"))
            df, ret = m, m.c_tr
        r = run(df, ret, fmap, nm, d0, mode)
        xs = pd.to_datetime(r["dates"])
        a = fig.add_subplot(gs[2 * k])
        a.plot(xs, r["px"], lw=0.9, color="#2f3640", label="指数收盘（信号基准）", zorder=2)
        s = r["sub"]
        if mode == "anchor":
            a.plot(xs, s.exp * (0.90 if nm == "创业板" else 1.0), "--", lw=1.2, color="#1e8449", label="锚买线（全历史中位线）")
            a.plot(xs, s.exp * (1.30 * 1.10 if nm == "创业板" else 1.30), "--", lw=1.2, color="#c0392b", label="卖出线（中位线×1.3）")
        else:
            for th in RUNG:
                a.plot(xs, s.peak * (1 - th), ":", lw=0.9, color="#e67e22")
            a.plot([], [], ":", lw=0.9, color="#e67e22", label="阶梯档（距峰 50/55/60/65%）")
            a.plot(xs, s.exp * 1.30, "--", lw=1.2, color="#c0392b", label="卖出线（中位线×1.3）")
        a.plot(xs, s.r1250, "-.", lw=1.3, color="#8e44ad", label="恐慌买价格闸（滚动5年中位线）")
        for tag, col2, mk, lb in (("panic", "#e74c3c", "*", "恐慌买"), ("ladder", "#8e44ad", "D", "阶梯买"),
                                  ("anchor", "#1e8449", "^", "锚买")):
            pts = [b for b in r["buys"] if b[3] == tag]
            if pts:
                a.scatter(pd.to_datetime([p[0] for p in pts]), [p[1] for p in pts],
                          s=[30 + p[2] * SIZE_K for p in pts], marker=mk, color=col2,
                          edgecolors="k", linewidths=.4, zorder=5, label=f"{lb} {len(pts)} 次")
        if r["sells"]:
            a.scatter(pd.to_datetime([p[0] for p in r["sells"]]), [p[1] for p in r["sells"]],
                      s=[30 + p[2] * SIZE_K for p in r["sells"]], marker="v", color="#e74c3c",
                      alpha=.45, edgecolors="none", zorder=4, label=f"卖出 {len(r['sells'])} 次")
        a.set_yscale("log")
        a.set_xlim(x0, x1)
        a.grid(alpha=.22, which="both")
        a.set_title(f"{nm}指数（执行标的 {etf}） 累计 {r['cum']:+.1%} ／ 年化 {r['ann']:+.2%} ／ 夏普 {r['sharpe']:.2f}"
                    f" ／ 日频回撤 {r['mdd']*100:.1f}% ／ 卡玛 {r['calmar']:.2f}"
                    + ("  ※收益按全收益指数（含股息）" if trf else ""), fontsize=12.5)
        a.legend(fontsize=8, ncol=3, loc="upper left")
        b = fig.add_subplot(gs[2 * k + 1])
        b.fill_between(xs, 0, r["pos"] * 100, color="#2980b9", alpha=.35)
        b.axhline(r["posavg"] * 100, color="#c0392b", ls="--", lw=1)
        b.text(x0, r["posavg"] * 100 + 3, f"平均持仓 {r['posavg']:.0%}（其余在货基吃 2%）", fontsize=9, color="#c0392b")
        b.set_xlim(x0, x1)
        b.set_ylim(0, 105)
        b.set_ylabel("持仓%", fontsize=9)
        b.grid(alpha=.22)
    fig.suptitle(f"四腿宽基策略 · 历史买卖全图（生产配置 · 一笔钱100元 · 闲钱2% · exec_lag=1）"
                 f"\n点面积统一 = 30 + 金额×{SIZE_K:.0f}（跨面板可比）；纵轴对数刻度", fontsize=15, y=0.945)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=100, bbox_inches="tight")
    print(f"saved {args.out}")


if __name__ == "__main__":
    main()
