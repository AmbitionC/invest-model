#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
R3 判据③ 可交易性：把风格信号接入 BRIEF 写死的组合口径。

组合口径（BRIEF 写死，本脚本对模糊处的解释一并写死并在报告中声明）：
  一笔钱 100 / 闲置现金 2% 年化 / exec_lag=1 / 日频回撤 / 卖出 flat 5%（月频·中位线上方）
  / 买入检查周频 / 四腿等权 25% 基线 / 风格信号只调「成长两腿 vs 红利腿」相对权重
  （±10pp 上限、年度再平衡、因果）。

四腿 = 创业板 chinext + 科创50 star（成长两腿） + 中证红利 div（价值腿） + 沪深300 hs300（固定 25%）。
中位线 = 各腿自身 expanding 中位数（预热 250 个观测，沿用 P26 高抛低吸口径的简化版）。
"""
from __future__ import annotations
import os
import numpy as np
import pandas as pd
from agent_R3_flows import load_all, build_signals, END_DATE

BASE = os.path.dirname(os.path.abspath(__file__))
LEGS = ["chinext", "star", "div", "hs300"]
GROWTH_LEGS = ["chinext", "star"]
VALUE_LEG = "div"
INIT_CASH = 100.0
CASH_RATE = 0.02
FEE = 0.0003 + 0.0005          # 手续费 + 滑点（单边）
SELL_FRAC = 0.05               # 卖出 flat 5%
BUY_EVERY = 5                  # 买入检查周频 ≈ 每 5 个交易日
MED_WARM = 250                 # 中位线预热
TILT_CAP = 0.10                # ±10pp
Q_HI, Q_LO = 0.80, 0.20        # 年度再平衡的信号极端分位门槛
EXPAND_MIN = 250               # 信号 expanding 分位的最小历史


def run_portfolio(px, sig_series=None, sign=-1, label="baseline", freq="Y", mode="gate"):
    """sig_series: pd.Series indexed by trade_date（已按可得日滞后）；None = 等权基线。
    sign=-1 表示「信号高 → 低配成长」。freq: Y/Q/M 再平衡；mode: gate(阈值档) / linear(连续)。"""
    d = px[["trade_date"] + LEGS].dropna().reset_index(drop=True)
    d = d[d.trade_date <= END_DATE].reset_index(drop=True)
    dates = d.trade_date.values
    n = len(d)
    med = {lg: d[lg].expanding(MED_WARM).median().values for lg in LEGS}
    px_arr = {lg: d[lg].values for lg in LEGS}

    sig = None
    if sig_series is not None:
        # crowding_daily 2023-12~2024-01 存在 32 个交易日缺口 → 前值填充（≤20 日），
        # 超过 20 日陈旧则视为无信号（tilt=0），不做插值造数。
        # expanding 分位在信号自身的完整历史上算（因果），再映射到组合日历，
        # 避免因组合起点晚（2019-12）而浪费信号 2015/2016 起的历史、导致预热期空档。
        rank_full = sig_series.sort_index().expanding(EXPAND_MIN).rank(pct=True)
        sig = d.trade_date.map(sig_series).ffill(limit=20).values.astype(float)
        sq = d.trade_date.map(rank_full).ffill(limit=20).values.astype(float)
    tilt = 0.0

    cash = INIT_CASH
    sh = {lg: 0.0 for lg in LEGS}           # 持有"份数"（按指数点位计价）
    pending = []                            # (exec_idx, leg, 'buy'/'sell', amount)
    nav = np.zeros(n)
    ym = pd.to_datetime(d.trade_date.astype(str), format="%Y%m%d")
    yr = ym.dt.year.values
    mo = ym.dt.to_period("M").astype(str).values
    per = {"Y": yr, "Q": ym.dt.to_period("Q").astype(str).values, "M": mo}[freq]
    n_reb = 0

    for i in range(n):
        if i > 0:
            cash *= (1 + CASH_RATE / 244)
        # 执行 pending（exec_lag=1）
        for (ei, lg, side, amt) in [p for p in pending if p[0] == i]:
            p_ = px_arr[lg][i]
            if side == "buy":
                amt = min(amt, cash)
                if amt > 1e-9:
                    sh[lg] += amt * (1 - FEE) / p_
                    cash -= amt
            else:
                q = min(amt, sh[lg])
                if q > 1e-12:
                    sh[lg] -= q
                    cash += q * p_ * (1 - FEE)
        pending = [p for p in pending if p[0] > i]

        mv = {lg: sh[lg] * px_arr[lg][i] for lg in LEGS}
        total = cash + sum(mv.values())
        nav[i] = total

        if i + 1 >= n:
            continue
        # ── 再平衡日按当日可得信号定 tilt（因果，次日成交） ──
        if sig is not None and i > 0 and per[i] != per[i - 1]:
            q = sq[i]
            n_reb += 1
            if np.isnan(q):
                tilt = 0.0
            elif mode == "gate":
                tilt = TILT_CAP * sign if q >= Q_HI else (-TILT_CAP * sign if q <= Q_LO else 0.0)
            else:  # linear: 分位线性映射到 ±10pp
                tilt = TILT_CAP * sign * (q - 0.5) * 2
        tgt = {"hs300": 0.25,
               "chinext": 0.25 + tilt / 2, "star": 0.25 + tilt / 2,
               "div": 0.25 - tilt}
        # ── 卖出：月末，中位线上方各腿卖 5% ──
        if i + 1 < n and mo[i] != mo[i + 1]:
            for lg in LEGS:
                m = med[lg][i]
                if not np.isnan(m) and px_arr[lg][i] > m and sh[lg] > 0:
                    pending.append((i + 1, lg, "sell", sh[lg] * SELL_FRAC))
        # ── 买入：周频检查，权重低于目标且在中位线下方 ──
        if i % BUY_EVERY == 0 and total > 0:
            for lg in LEGS:
                m = med[lg][i]
                below = np.isnan(m) or px_arr[lg][i] < m
                gap = tgt[lg] * total - mv[lg]
                if below and gap > 0.01 * total:
                    pending.append((i + 1, lg, "buy", gap))

    out = pd.DataFrame({"trade_date": dates, "nav": nav})
    out["label"] = label
    out.attrs["n_reb"] = n_reb
    return out


def metrics(nav: pd.DataFrame) -> dict:
    v = nav.nav.values
    yrs = len(v) / 244
    ann = (v[-1] / v[0]) ** (1 / yrs) - 1
    r = np.diff(v) / v[:-1]
    vol = r.std() * np.sqrt(244)
    shp = (ann - 0.02) / vol if vol > 0 else np.nan
    mdd = float((v / np.maximum.accumulate(v) - 1).min())
    return {"年化": round(ann * 100, 2), "波动": round(vol * 100, 2),
            "夏普(rf=2%)": round(shp, 3), "最大回撤": round(mdd * 100, 2),
            "期末净值": round(float(v[-1]), 2), "年数": round(yrs, 2)}


def main():
    px, crowd, fear, etf = load_all()
    sig, _ = build_signals(px, crowd, fear, etf)
    base = run_portfolio(px, None, label="baseline_equal")
    rows = [{"策略": "等权基线（无风格信号）", **metrics(base)}]
    navs = [base]
    for sc in ["F3_dual_ratio_q250", "F4_dual_ratio_dev", "F2_margin_mom"]:
        s = sig.set_index("trade_date")[sc].dropna()
        for freq, mode, tag in (("Y", "gate", "年度·阈值档（BRIEF口径）"),
                                ("Y", "linear", "年度·连续档（敏感性）"),
                                ("Q", "gate", "季度·阈值档（敏感性）"),
                                ("M", "gate", "月度·阈值档（敏感性）")):
            nv = run_portfolio(px, s, sign=-1, label=f"{sc}|{tag}", freq=freq, mode=mode)
            rows.append({"策略": f"{sc} {tag}", "调仓次数": nv.attrs["n_reb"], **metrics(nv)})
            navs.append(nv)
        nv = run_portfolio(px, s, sign=+1, label=f"{sc}|反号", freq="M", mode="gate")
        rows.append({"策略": f"{sc} 反号对照（月度）", "调仓次数": nv.attrs["n_reb"], **metrics(nv)})

    res = pd.DataFrame(rows)
    pd.set_option("display.width", 250)
    print("回测区间（四腿齐备起）：", int(base.trade_date.iloc[0]), "→", int(base.trade_date.iloc[-1]))
    print(res.to_string(index=False))
    b = res.iloc[0]
    print("\n判据③（年化 ≥ 基线+0.5pp 且 夏普 ≥ 基线+0.05）：")
    for _, r in res.iloc[1:].iterrows():
        ok = (r["年化"] >= b["年化"] + 0.5) and (r["夏普(rf=2%)"] >= b["夏普(rf=2%)"] + 0.05)
        print(f"  {r['策略']:46s} Δ年化={r['年化']-b['年化']:+.2f}pp "
              f"Δ夏普={r['夏普(rf=2%)']-b['夏普(rf=2%)']:+.3f}  → {'过' if ok else '未过'}")
    res.to_csv(os.path.join(BASE, "agent_R3_portfolio.csv"), index=False)
    pd.concat(navs).to_csv(os.path.join(BASE, "agent_R3_nav.csv"), index=False)


if __name__ == "__main__":
    main()
