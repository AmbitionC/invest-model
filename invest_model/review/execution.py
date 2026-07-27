"""计划执行对账（P0·2026-07-27 产品评审落地）：计划让做的 vs 实际做了没。

原理：action_plan（计划动作/股数）× holding_snapshot（每日实际股数）快照差分推断执行。
只读，不改任何生产数据。核心口径 exec_recon = snapshot-diff / 1手容忍 / 观察窗5交易日 /
条件单以窗内 low/high 触及挂单价豁免 / 送转窗（复权因子变动）跳过。

一等状态（绝不硬算）：
  executed 已执行 | partial 部分执行 | not_executed 未执行·待确认 | reversed 反向操作
  cond_untriggered 条件未触发（豁免，不进执行率分母） | pre_executed 已执行(前置)
  corporate_action 送转窗跳过 | no_baseline/no_snapshot 无法对账·数据缺口

纪律呈现哲学：提示不强制——未执行≠违纪，只陈述计划说了什么/实际发生了什么/差异折合
多少钱；"该止损未止"仅当【强风控条款 + 未执行≥2交易日 + 触发条件事后仍成立】才升告警。
execution_ack 主动偏离通道属 P1（表未建），v1 全部按"待确认"滚动。
盲区（如实声明）：同日买卖做T快照差分不可见；同决策日修订覆盖后对账用终版；无成交流水，
执行价以计划日次一交易日收盘近似。
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

WINDOW_TD = 5           # 观察窗交易日数（与"待办·还没卖"提示节奏一致）
LOT = 100               # 取整容忍：1 手
STRONG_PAT = re.compile("硬止损|破MA|账户回撤|排雷|回撤止损")


def _calendar(repo, start: str, end: str) -> list[str]:
    """交易日历：用沪深300 index_daily 日期序列（轻查询、覆盖完整）。"""
    df = repo.read_sql(
        "SELECT trade_date FROM index_daily WHERE code='000300.SH' "
        "AND trade_date>=:s AND trade_date<=:e ORDER BY trade_date",
        {"s": start, "e": end})
    return df["trade_date"].astype(str).tolist() if not df.empty else []


def _parse_trigger(hint: str) -> tuple[float | None, float | None]:
    """从挂单提示解析 (回踩价, 突破价)：'回踩≈244.39 / 突破>275.8'、'勿追价·回踩挂单≈52.1'。"""
    h = str(hint or "")
    lo = re.search(r"[≈约]\s*([0-9]+\.?[0-9]*)", h)
    hi = re.search(r"[>＞]\s*([0-9]+\.?[0-9]*)", h)
    return (float(lo.group(1)) if lo else None, float(hi.group(1)) if hi else None)


def reconcile(repo, asof: str) -> dict:
    """返回 {orders:[...], off_plan:[...], metrics:{...}, coverage:{...}}；任何缺表返回空骨架。"""
    empty = {"orders": [], "off_plan": [], "metrics": {}, "coverage": {}}
    for t in ("action_plan", "holding_snapshot"):
        if not repo.table_exists(t):
            return empty
    snaps = repo.read_sql(
        "SELECT snapshot_date, code, shares FROM holding_snapshot "
        "WHERE LOWER(asset_type) NOT IN ('cash')")
    if snaps.empty:
        return empty
    snaps["shares"] = pd.to_numeric(snaps["shares"], errors="coerce").fillna(0.0)
    snaps["snapshot_date"] = snaps["snapshot_date"].astype(str)
    snap_dates = sorted(snaps["snapshot_date"].unique())
    s0_date = snap_dates[0]

    ap = repo.read_sql(
        "SELECT plan_date, code, name, action, shares_delta, reason, stop_price, "
        "ref_price, trigger_hint FROM action_plan "
        "WHERE action IN ('buy','add','trim','sell') AND plan_date>=:s AND plan_date<:e",
        {"s": s0_date, "e": asof})
    cal = _calendar(repo, s0_date, asof)
    if not cal:
        return empty

    def shares_at(code: str, date: str) -> tuple[float | None, str | None]:
        """≤date 最近快照的持股数；无快照返回 (None,None)，快照存在但无该票=0。"""
        ds = [d for d in snap_dates if d <= date]
        if not ds:
            return None, None
        d = ds[-1]
        row = snaps[(snaps["snapshot_date"] == d) & (snaps["code"] == code)]
        return (float(row["shares"].iloc[0]) if not row.empty else 0.0), d

    codes = sorted(set(ap["code"])) if not ap.empty else []
    px = pd.DataFrame()
    if codes:
        ph = ",".join(f":c{i}" for i in range(len(codes)))
        px = repo.read_sql(
            f"SELECT code, trade_date, close, low, high FROM stock_daily "
            f"WHERE code IN ({ph}) AND trade_date>=:s AND trade_date<=:e",
            {**{f"c{i}": c for i, c in enumerate(codes)}, "s": s0_date, "e": asof})
        for c in ("close", "low", "high"):
            px[c] = pd.to_numeric(px[c], errors="coerce")
        px["trade_date"] = px["trade_date"].astype(str)
    adj = repo.read_sql(
        "SELECT code, trade_date, adj_factor FROM stock_adj "
        "WHERE trade_date>=:s AND trade_date<=:e",
        {"s": s0_date, "e": asof}) if repo.table_exists("stock_adj") else pd.DataFrame()
    if not adj.empty:
        adj["adj_factor"] = pd.to_numeric(adj["adj_factor"], errors="coerce")
        adj["trade_date"] = adj["trade_date"].astype(str)

    orders = []
    for _, r in ap.iterrows():
        c, d0 = str(r["code"]), str(r["plan_date"])
        sd = float(pd.to_numeric(r["shares_delta"], errors="coerce") or 0)
        o = {"plan_date": d0, "code": c, "name": str(r["name"] or c),
             "action": str(r["action"]), "planned_shares": sd,
             "reason": str(r["reason"] or ""),
             "strong_risk": bool(STRONG_PAT.search(str(r["reason"] or ""))),
             "actual_shares": None, "executed_ratio": None,
             "delay_td": None, "delay_uncertain": False,
             "status": None, "nonexec_cost": None, "condition_still_valid": None}
        win = [t for t in cal if t > d0][:WINDOW_TD]
        if not win:
            o["status"] = "too_recent"
            orders.append(o)
            continue
        # 送转窗跳过
        if not adj.empty:
            fa = adj[(adj["code"] == c) & (adj["trade_date"] >= d0) &
                     (adj["trade_date"] <= win[-1])]["adj_factor"].dropna()
            if len(fa) >= 2 and abs(float(fa.iloc[-1]) / float(fa.iloc[0]) - 1) > 0.001:
                o["status"] = "corporate_action"
                orders.append(o)
                continue
        s0, s0d = shares_at(c, d0)
        if s0 is None:
            o["status"] = "no_baseline"
            orders.append(o)
            continue
        if o["action"] in ("sell", "trim") and s0 <= 0:
            o["status"] = "pre_executed"
            orders.append(o)
            continue
        # 条件单豁免（买类挂单价从未触及）
        gpx = px[(px["code"] == c) & (px["trade_date"].isin(win))] if not px.empty else pd.DataFrame()
        if o["action"] in ("buy", "add"):
            lo_p, hi_p = _parse_trigger(r["trigger_hint"])
            if (lo_p or hi_p) and not gpx.empty:
                touched = ((lo_p is not None and (gpx["low"] <= lo_p * 1.005).any())
                           or (hi_p is not None and (gpx["high"] >= hi_p).any()))
                if not touched:
                    o["status"] = "cond_untriggered"
                    orders.append(o)
                    continue
        wsnaps = [(t, shares_at(c, t)) for t in win if t in snap_dates]
        if not wsnaps:
            o["status"] = "no_snapshot"
            orders.append(o)
            continue
        gap = any(t not in snap_dates for t in win[:len(wsnaps)])
        end_sh = wsnaps[-1][1][0]
        delta_end = end_sh - s0
        o["actual_shares"] = delta_end
        exec_t = None
        for t, (sh, _) in wsnaps:
            dl = sh - s0
            if abs(dl) >= LOT and np.sign(dl) == np.sign(sd or dl):
                exec_t = t
                break
        if sd != 0 and delta_end != 0 and np.sign(delta_end) != np.sign(sd):
            o["status"] = "reversed"
        else:
            ratio = float(np.clip(delta_end / sd, 0, 1.5)) + 0.0 if sd else 0.0
            ratio = abs(ratio) if ratio == 0 else ratio    # 防 -0.0 显示成 -0%
            o["executed_ratio"] = round(ratio, 3)
            if abs(delta_end - sd) < LOT and (abs(delta_end) >= LOT or abs(sd) < LOT):
                o["executed_ratio"], ratio = 1.0, 1.0   # 1手容忍带
            o["status"] = ("executed" if ratio >= 0.9 else
                           "partial" if ratio >= 0.2 else "not_executed")
        if exec_t:
            o["delay_td"] = max(0, cal.index(exec_t) - cal.index(win[0]))
            o["delay_uncertain"] = gap
        # 未执行成本（次日收盘近似入场 → 最新收盘）
        if o["status"] in ("not_executed", "partial") and not gpx.empty:
            allpx = px[(px["code"] == c) & (px["trade_date"] > d0)].sort_values("trade_date")
            if len(allpx) >= 2:
                e_px = float(allpx["close"].iloc[0])
                l_px = float(allpx["close"].iloc[-1])
                undone = abs(sd - (delta_end if o["status"] == "partial" else 0))
                # 卖类：价跌→负=不卖拖着继续亏；买类：价涨→正=错过的涨幅
                o["nonexec_cost"] = round((l_px - e_px) * undone, 2)
                if o["strong_risk"]:
                    stop = pd.to_numeric(r["stop_price"], errors="coerce")
                    o["condition_still_valid"] = bool(l_px <= float(stop) * 1.02) \
                        if np.isfinite(stop) and stop else True
        orders.append(o)

    # 计划外操作：快照股数变动但近 10 交易日无对应指令
    off_plan = []
    ap_keys = {(str(r["plan_date"]), str(r["code"])) for _, r in ap.iterrows()}
    for i in range(1, len(snap_dates)):
        d_prev, d_cur = snap_dates[i - 1], snap_dates[i]
        a = snaps[snaps["snapshot_date"] == d_prev].set_index("code")["shares"]
        b = snaps[snaps["snapshot_date"] == d_cur].set_index("code")["shares"]
        for c in set(a.index) | set(b.index):
            dl = float(b.get(c, 0)) - float(a.get(c, 0))
            if abs(dl) < LOT:
                continue
            recent = [t for t in cal if t <= d_cur][-10:]
            if not any((t, c) in ap_keys for t in recent):
                off_plan.append({"date": d_cur, "code": c, "shares_delta": dl,
                                 "note": "近10交易日无对应指令"})

    # 当前仍持仓集合：告警只保留可操作的（已清仓的历史未执行单是"迟到执行"，不再滚动提示）
    last_snap = snap_dates[-1]
    held_now = set(snaps[(snaps["snapshot_date"] == last_snap) &
                         (snaps["shares"] >= LOT)]["code"].astype(str))
    for o in orders:
        o["still_held"] = o["code"] in held_now
    applicable = [o for o in orders if o["status"] in
                  ("executed", "partial", "not_executed", "reversed")]
    sells = [o for o in applicable if o["action"] in ("sell", "trim")]
    buys = [o for o in applicable if o["action"] in ("buy", "add")]
    delays = [o["delay_td"] for o in orders if o["delay_td"] is not None]
    cov_win = cal[-10:]
    metrics = {
        "risk_exit_exec_rate": {
            "num": sum(o["status"] == "executed" for o in sells), "den": len(sells)},
        "buy_fill_rate": {
            "num": sum(o["status"] == "executed" for o in buys), "den": len(buys)},
        "median_delay_td": float(np.median(delays)) if delays else None,
        "cum_nonexec_cost_sell": round(sum(o["nonexec_cost"] or 0 for o in sells), 2),
        "cum_nonexec_cost_buy": round(sum(o["nonexec_cost"] or 0 for o in buys), 2),
        "n_cond_untriggered": sum(o["status"] == "cond_untriggered" for o in orders),
        "n_unreconcilable": sum(o["status"] in ("no_baseline", "no_snapshot")
                                for o in orders),
    }
    coverage = {"trading_days_last10": len(cov_win),
                "snapshots_last10": sum(d in snap_dates for d in cov_win),
                "gaps_last10": [d for d in cov_win if d not in snap_dates]}
    return {"orders": orders, "off_plan": off_plan, "metrics": metrics,
            "coverage": coverage}
