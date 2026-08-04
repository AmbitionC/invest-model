# -*- coding: utf-8 -*-
"""期初 N 万 · 任意回看窗口的实盘化损益（owner 2026-08-04，--months 可调）

与回测脚本的区别——这里**尽量贴实盘**：
  · 三腿用**真实 ETF 收盘价**（510300 / 159915 / 588000，来自 results/fund_close_dump.csv）
    并按 **A 股最小 100 份（1 手）** 取整；红利 515080 无价格数据，用指数近似（见报告注记）。
  · 闲置现金按货基 1.5%/年（V2 实测近三年真实档位，比回测常用的 2% 保守）。
  · 信号次日收盘成交（exec_lag=1），与生产口径一致。

两个情景必须并报——**短窗结果几乎完全由起手状态决定，且方向会随窗口翻转**：
  近 12 个月：空仓起手 +1.86% vs 满仓起手 +29.06%（买入闸全年没开）
  近 60 个月：空仓起手 +71.77% vs 满仓起手 +28.19%（2024-01 抄在底部）
这不是策略变好变坏，是起点落在周期的哪个位置。
  A 新开户：期初 50 万全现金
  B 已持有：期初已按四腿满仓（承接既有持仓）

只读 CSV，不落库、不联网。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager  # noqa: E402

from long_window_backtest import FONT, LEGS, WARM  # noqa: E402

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}

ETF_CODE = {"沪深300": "510300.SH", "创业板": "159915.SZ",
            "科创50": "588000.SH", "红利": None}          # 红利 515080 无价格数据
ETF_NAME = {"沪深300": "510300", "创业板": "159915", "科创50": "588000", "红利": "515080"}
CASH_RATE = 0.015          # 货基/逆回购，V2 实测近三年档位
LOT = 100                  # A 股 ETF 最小交易单位


def load_leg(root: Path, f: str, col: str, trf: str | None):
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
    tr = None
    if trf:
        t = pd.read_csv(root / trf, dtype={"trade_date": str})
        t["c_tr"] = pd.to_numeric(t.close)
        d = d.merge(t[["trade_date", "c_tr"]], on="trade_date", how="left")
        tr = d.c_tr
    return d, tr


def run_leg(d, tr, px, fmap, nm, d0, d1, mode, cash0, shares0=0.0):
    """px = 该腿的可交易单价序列（ETF 真实价 或 指数近似）；按整手取整。"""
    dates, c = d.trade_date.values, d.c.values
    i0 = int(np.searchsorted(dates, d0))
    i1 = int(np.searchsorted(dates, d1, side="right"))
    cash, shares = float(cash0), float(shares0)
    last, pend, trades = -999, [], []
    armed, in_ep = np.ones(4, bool), False
    curve = []
    for i in range(i0, i1):
        ci, p = float(c[i]), float(px[i])
        if i > i0:
            days = (pd.Timestamp(dates[i]) - pd.Timestamp(dates[i - 1])).days / 365.25
            cash *= (1 + CASH_RATE) ** days
        r = d.iloc[i]
        for k, fr, _t, why in [x for x in pend if x[2] == i]:
            if k == "B":
                amt = cash * fr
                lots = int(amt / (p * LOT))                    # 整手向下取整
                if lots >= 1:
                    cost = lots * LOT * p
                    shares += lots * LOT
                    cash -= cost
                    trades.append(dict(日期=str(dates[i]), 方向="买", 触发=why, 单价=round(p, 4),
                                       份额=lots * LOT, 金额=round(cost, 2)))
            else:
                sh = shares * fr
                sh = float(int(sh))                            # 卖出允许零股，向下取整到整数股
                if sh >= 1:
                    cash += sh * p
                    shares -= sh
                    trades.append(dict(日期=str(dates[i]), 方向="卖", 触发=why, 单价=round(p, 4),
                                       份额=sh, 金额=round(sh * p, 2)))
        pend = [x for x in pend if x[2] > i]
        sig, f = [], fmap.get(dates[i], np.nan)
        if f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and ci < r.r1250:
            sig.append(("B", 0.50, f"恐慌抢买 fear={f:.0f}"))
        if f == f and f >= 75:
            last = i
        if mode == "ladder":
            dd = ci / r.peak - 1
            if dd <= -0.50:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate((.50, .55, .60, .65)) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False
                    sig.append(("B", (.30, .35, .40, .50)[j], f"阶梯 距峰{dd:+.0%}"))
            elif in_ep and dd >= -0.25:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            sig.append(("B", 0.20, f"锚买 价/中位线={ci / r.exp:.2f}"))
        mul = 1.43 if nm == "创业板" else 1.30
        if r.me and r.exp == r.exp and ci > r.exp * mul and shares > 0:
            sig.append(("S", 0.05, f"卖出闸 价/中位线={ci / r.exp:.2f}"))
        for k, fr, why in sig:
            pend.append((k, fr, min(i + 1, i1 - 1), why))
        curve.append(cash + shares * p)
    return dict(cash=cash, shares=shares, px_end=float(px[i1 - 1]),
                value=cash + shares * float(px[i1 - 1]), trades=trades,
                curve=np.array(curve), dates=dates[i0:i1],
                px0=float(px[i0]), idx0=float(c[i0]), idx1=float(c[i1 - 1]))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    ap.add_argument("--capital", type=float, default=500_000)
    ap.add_argument("--months", type=int, default=12)
    a = ap.parse_args()
    root = Path(a.data)

    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    fund = pd.read_csv(root / "fund_close_dump.csv", dtype={"code": str, "trade_date": str})
    fund["close"] = pd.to_numeric(fund.close)

    SRC = {"沪深300": ("index_dump_000300_SH.csv", "close", None),
           "创业板": ("spread_full_history.csv", "chinext", None),
           "科创50": ("index_dump_000688_SH.csv", "close", None),
           "红利": ("index_dump_000922_CSI.csv", "close", "index_dump_H00922_CSI.csv")}
    MODE = {nm: m for nm, _, _, _, _, m in LEGS}

    legs = {}
    for nm, (f, col, trf) in SRC.items():
        d, tr = load_leg(root, f, col, trf)
        code = ETF_CODE[nm]
        if code:
            fp = fund[fund.code == code][["trade_date", "close"]].rename(columns={"close": "etf"})
            d = d.merge(fp, on="trade_date", how="left")
            d["etf"] = d["etf"].ffill()
            px = d["etf"].to_numpy()
            src = f"真实 ETF {ETF_NAME[nm]}"
        else:
            # 红利：无 ETF 价格，用全收益指数按 ETF 量级折算（不影响收益率，只影响整手粒度）
            base = d["c_tr"] if "c_tr" in d else d["c"]
            px = (base / base.iloc[0] * 1.0).to_numpy()
            src = "指数近似（515080 无价格数据）"
        legs[nm] = dict(d=d, px=px, src=src)

    end = min(str(legs[nm]["d"].trade_date.iloc[-1]) for nm in legs)
    start = (pd.Timestamp(end) - pd.DateOffset(months=a.months)).strftime("%Y%m%d")
    per = a.capital / len(legs)

    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False
    SC = {}

    print("=" * 104)
    print(f"期初 {a.capital:,.0f} 元 · 四腿各 {per:,.0f} 元 · 区间 {start} ~ {end}"
          f"（近 {a.months} 个月）· 闲置现金 {CASH_RATE:.1%}/年")
    print("=" * 104)

    for scen, hold0 in (("A 新开户（期初全现金）", False), ("B 已持有（期初已满仓）", True)):
        print(f"\n{'─' * 104}\n【情景 {scen}】\n{'─' * 104}")
        tot_v, tot_tr, rows = 0.0, [], []
        for nm in legs:
            d, px = legs[nm]["d"], legs[nm]["px"]
            i0 = int(np.searchsorted(d.trade_date.values, start))
            if hold0:
                lots = int(per / (float(px[i0]) * LOT))
                c0, s0 = per - lots * LOT * float(px[i0]), lots * LOT
            else:
                c0, s0 = per, 0.0
            # 起手持仓必须在回测**开始前**就位，否则卖出闸判不到 shares>0（首版此处有 bug）
            r = run_leg(d, None, px, fmap, nm, start, end, MODE[nm], c0, shares0=s0)
            tot_v += r["value"]; tot_tr += [{"腿": nm, **t} for t in r["trades"]]
            rows.append((nm, r))
        # 组合资金曲线：各腿按共同日历对齐，缺失日用该腿最近可得值（腿间日历差 ≤2 天）
        ser = {nm: pd.Series(r["curve"], index=list(r["dates"])) for nm, r in rows}
        cal = sorted(set().union(*[set(x.index) for x in ser.values()]))
        eq = []
        for dd_ in cal:
            v = 0.0
            for nm in ser:
                x = ser[nm]
                v += float(x[dd_]) if dd_ in x.index else float(
                    x[x.index <= dd_].iloc[-1] if (x.index <= dd_).any() else x.iloc[0])
            eq.append(v)
        SC[scen[0]] = dict(rows=rows, trades=tot_tr, total=tot_v,
                           cal=cal, eq=np.array(eq))
        for nm, r in rows:
            pnl = r["value"] - per
            print(f"  {nm:8s}({ETF_NAME[nm]})  期末 {r['value']:>10,.0f} 元  "
                  f"盈亏 {pnl:>+9,.0f}（{pnl/per:>+6.2%}）｜持仓 {r['shares']:>8,.0f} 份 "
                  f"+ 现金 {r['cash']:>9,.0f}｜指数 {r['idx0']:.0f}→{r['idx1']:.0f} "
                  f"({r['idx1']/r['idx0']-1:+.1%})｜成交 {len(r['trades'])} 笔")
        pnl = tot_v - a.capital
        print(f"  {'合计':8s}          期末 {tot_v:>10,.0f} 元  盈亏 {pnl:>+9,.0f}（**{pnl/a.capital:+.2%}**）")
        if tot_tr:
            print(f"\n  成交流水（共 {len(tot_tr)} 笔）：")
            t = pd.DataFrame(tot_tr)
            print("    " + t.to_string(index=False).replace("\n", "\n    "))
        else:
            print("\n  ⚠ 全期零成交——四腿的买入闸一次都没开、卖出闸下无持仓可卖。")

    # 参照系
    print(f"\n{'=' * 104}\n参照：同期买入持有（50 万一次性等分买四个指数，不做任何交易）\n{'=' * 104}")
    tot = 0.0
    for nm in legs:
        d = legs[nm]["d"]
        i0 = int(np.searchsorted(d.trade_date.values, start))
        base = (d["c_tr"].ffill() if "c_tr" in d.columns and d["c_tr"].notna().any()
                else d["c"])
        r = float(base.dropna().iloc[-1]) / float(base.iloc[i0])
        tot += per * r
        print(f"  {nm:8s} {r - 1:+7.2%}" + ("（全收益口径含分红）" if nm == "红利" else ""))
    print(f"  {'合计':8s} 期末 {tot:,.0f} 元　盈亏 {tot - a.capital:+,.0f}（**{tot / a.capital - 1:+.2%}**）")

    # 买入持有资金曲线（同一日历）
    calA = SC["A"]["cal"]
    bh_eq = np.zeros(len(calA))
    for nm in legs:
        d = legs[nm]["d"]
        base = (d["c_tr"].ffill() if "c_tr" in d.columns and d["c_tr"].notna().any() else d["c"])
        sr = pd.Series(base.values, index=d.trade_date.values).ffill()
        i0v = float(sr[sr.index >= start].iloc[0])
        for j, dd_ in enumerate(calA):
            v = sr[sr.index <= dd_]
            bh_eq[j] += per * (float(v.iloc[-1]) / i0v if len(v) else 1.0)

    # 分年收益
    print(f"\n{'=' * 104}\n分年收益（情景A 空仓起手 vs 买入持有）\n{'=' * 104}")
    ea = pd.Series(SC["A"]["eq"], index=calA)
    eb = pd.Series(bh_eq, index=calA)
    yrs_ = sorted({x[:4] for x in calA})
    print(f"  {'年份':>8s}{'情景A 期末':>14s}{'当年收益':>10s}{'买入持有':>10s}{'差':>9s}")
    for y in yrs_:
        m_ = [x for x in calA if x[:4] == y]
        if len(m_) < 5:
            continue
        p0a = float(ea[ea.index < m_[0]].iloc[-1]) if (ea.index < m_[0]).any() else float(ea.iloc[0])
        p0b = float(eb[eb.index < m_[0]].iloc[-1]) if (eb.index < m_[0]).any() else float(eb.iloc[0])
        ra = float(ea[m_[-1]]) / p0a - 1
        rb = float(eb[m_[-1]]) / p0b - 1
        print(f"  {y:>8s}{float(ea[m_[-1]]):>14,.0f}{ra:>10.2%}{rb:>10.2%}{(ra - rb) * 100:>+8.1f}pp")

    _chart(SC, legs, start, end, per, a.capital, tot, calA, bh_eq)
    print(f"\n  数据源：" + "；".join(f"{nm}={legs[nm]['src']}" for nm in legs))


def _chart(SC, legs, start, end, per, capital, bh_total, calA, bh_eq):
    yrs = (pd.Timestamp(end) - pd.Timestamp(start)).days / 365.25
    fig = plt.figure(figsize=(17.5, 15))
    gs = fig.add_gridspec(3, 4, height_ratios=[0.95, 1.15, 0.85], hspace=0.34,
                          wspace=0.26, top=0.905)
    fig.suptitle(f"期初 {capital/10000:.0f} 万空仓起手 · 近 {yrs:.1f} 年"
                 f"（{start[:4]}-{start[4:6]} ~ {end[:4]}-{end[4:6]}）　四腿买卖点与损益",
                 fontsize=19, weight="bold", y=0.958)

    tr = pd.DataFrame(SC["A"]["trades"])
    import matplotlib.dates as mdates
    for k, nm in enumerate(legs):
        ax = fig.add_subplot(gs[0, k])
        d = legs[nm]["d"]
        m = (d.trade_date >= start) & (d.trade_date <= end)
        t = pd.to_datetime(d.trade_date[m])
        ax.plot(t, d.c[m], lw=1.2, color=COL[nm])
        exp = float(d.exp[m].iloc[-1])
        sl = exp * (1.43 if nm == "创业板" else 1.30)
        ax.axhline(sl, color="#c0392b", ls="--", lw=1.3)
        if nm != "科创50":
            ax.axhline(exp * (0.90 if nm == "创业板" else 1.0), color="#1e8449", ls="--", lw=1.3)
        sub = tr[tr["腿"] == nm] if len(tr) else pd.DataFrame()
        if len(sub):
            idx = d.set_index("trade_date")
            for _, x in sub.iterrows():
                if x["日期"] not in idx.index:
                    continue
                y = float(idx.loc[x["日期"], "c"])
                buy = x["方向"] == "买"
                ax.scatter(pd.Timestamp(x["日期"]), y,
                           s=(np.clip(x["金额"] / 700, 18, 190) if buy else 24),
                           marker=("^" if buy else "v"),
                           color=("#1e8449" if buy else "#c0392b"), zorder=5, alpha=.85)
        nb = int((sub["方向"] == "买").sum()) if len(sub) else 0
        ns = int((sub["方向"] == "卖").sum()) if len(sub) else 0
        r0, r1 = float(d.c[m].iloc[0]), float(d.c[m].iloc[-1])
        ax.set_title(f"{nm} 指数 {r1/r0-1:+.1%}\n买{nb}笔 卖{ns}笔", fontsize=11.5,
                     weight="bold", color=COL[nm])
        ax.grid(alpha=.22); ax.tick_params(labelsize=8.5)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%y"))

    # ① 资金曲线（这张是五年窗口的主角）
    ax = fig.add_subplot(gs[1, :3])
    tt = pd.to_datetime(calA)
    ax.plot(tt, SC["A"]["eq"] / 10000, lw=2.6, color="#1e8449",
            label=f"情景A 空仓起手  {SC['A']['total']/10000:.1f}万（{SC['A']['total']/capital-1:+.1%}）")
    ax.plot(tt, SC["B"]["eq"] / 10000, lw=1.8, color="#2980b9",
            label=f"情景B 满仓起手  {SC['B']['total']/10000:.1f}万（{SC['B']['total']/capital-1:+.1%}）")
    ax.plot(tt, bh_eq / 10000, lw=1.8, color="#95a5a6", ls="--",
            label=f"一次性买入持有  {bh_total/10000:.1f}万（{bh_total/capital-1:+.1%}）")
    ax.axhline(capital / 10000, color="#333", ls=":", lw=1.3)
    # 标注买入密集期
    if len(tr):
        b = tr[tr["方向"] == "买"]
        if len(b):
            big = b.nlargest(1, "金额").iloc[0]
            xj = pd.Timestamp(big["日期"])
            yj = float(pd.Series(SC["A"]["eq"], index=calA)[
                [x for x in calA if x <= big["日期"]][-1]]) / 10000
            ax.annotate(f"{big['日期'][:4]}-{big['日期'][4:6]} 恐慌抄底\n单笔最大 {big['金额']/10000:.1f}万",
                        xy=(xj, yj), xytext=(xj, yj - 14), fontsize=11, ha="center",
                        arrowprops=dict(arrowstyle="->", color="#1e8449", lw=1.6),
                        bbox=dict(fc="#eafaf1", ec="#1e8449", alpha=.95))
    ax.set_ylabel("账户总值（万元）", fontsize=11.5)
    ax.legend(fontsize=11.5, loc="upper left"); ax.grid(alpha=.25)
    ax.set_title("① 资金曲线——五年窗口里，空仓起手反而是最大优势", fontsize=13.5, weight="bold")

    # ② 期末对比
    ax = fig.add_subplot(gs[1, 3])
    labs = ["A 空仓", "B 满仓", "买入持有"]
    vals = [SC["A"]["total"], SC["B"]["total"], bh_total]
    b_ = ax.bar(labs, [v / 10000 for v in vals], color=["#1e8449", "#2980b9", "#95a5a6"], width=.6)
    ax.axhline(capital / 10000, color="#333", ls=":", lw=1.4)
    for r_, v in zip(b_, vals):
        ax.text(r_.get_x() + r_.get_width() / 2, v / 10000, f"{v/10000:.1f}万\n{v/capital-1:+.1%}\n"
                f"年化{(v/capital)**(1/yrs)-1:+.1%}",
                ha="center", va="bottom", fontsize=11, weight="bold")
    ax.set_ylim(0, max(vals) / 10000 * 1.32); ax.grid(alpha=.22, axis="y")
    ax.tick_params(labelsize=10)
    ax.set_title("② 期末总值", fontsize=13.5, weight="bold")

    # ③ 分年
    ax = fig.add_subplot(gs[2, :])
    ea = pd.Series(SC["A"]["eq"], index=calA); eb = pd.Series(bh_eq, index=calA)
    ys, ra, rb = [], [], []
    for y in sorted({x[:4] for x in calA}):
        mm = [x for x in calA if x[:4] == y]
        if len(mm) < 5:
            continue
        p0a = float(ea[ea.index < mm[0]].iloc[-1]) if (ea.index < mm[0]).any() else float(ea.iloc[0])
        p0b = float(eb[eb.index < mm[0]].iloc[-1]) if (eb.index < mm[0]).any() else float(eb.iloc[0])
        ys.append(y); ra.append(float(ea[mm[-1]]) / p0a - 1); rb.append(float(eb[mm[-1]]) / p0b - 1)
    x = np.arange(len(ys)); w = 0.38
    ax.bar(x - w/2, np.array(ra) * 100, w, color="#1e8449", label="情景A 空仓起手")
    ax.bar(x + w/2, np.array(rb) * 100, w, color="#95a5a6", label="一次性买入持有")
    for i, (u, v) in enumerate(zip(ra, rb)):
        ax.text(i - w/2, u*100, f"{u:+.1%}", ha="center",
                va=("bottom" if u >= 0 else "top"), fontsize=10, weight="bold")
        ax.text(i + w/2, v*100, f"{v:+.1%}", ha="center",
                va=("bottom" if v >= 0 else "top"), fontsize=9.5, color="#555")
    ax.axhline(0, color="#333", lw=1)
    ax.set_xticks(x); ax.set_xticklabels(ys, fontsize=12)
    ax.set_ylabel("当年收益（%）"); ax.legend(fontsize=11); ax.grid(alpha=.22, axis="y")
    ax.set_title("③ 分年：策略的钱几乎全是 2024 那一年抄底赚的", fontsize=13.5, weight="bold")

    out = Path("results") / "pnl_5y.png"
    fig.savefig(out, dpi=112, bbox_inches="tight")
    print(f"saved {out}")


if __name__ == "__main__":
    main()
