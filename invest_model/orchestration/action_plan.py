"""实盘操作计划（最终交付物）：结合当前持仓 + 目标组合 + 风控评估 → 操作清单。

按需生成：对给定日期（默认最新数据日），对每只持仓/目标票给出
  动作（买/加/减/清/持）+ 当前→目标权重 + 触发理由 + 关键价位 + 账户层提示。

风控判定复用 :mod:`invest_model.portfolio.risk`，与回测同一套逻辑。
目标组合复用 :meth:`ClosedLoop._build_targets`（投顾为主 + 量化补充）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.orchestration.closed_loop import ClosedLoop, LoopConfig
from invest_model.portfolio.risk import evaluate_holding, replay_tier
from invest_model.repositories.holding_repo import HoldingRepo

logger = get_logger()


@dataclass
class ActionPlan:
    plan_date: str
    rows: list[dict] = field(default_factory=list)
    account: dict = field(default_factory=dict)

    def to_markdown(self) -> str:
        return render_markdown(self)


def _latest_data_date(loop: ClosedLoop) -> str:
    df = loop.repo.read_sql("SELECT MAX(trade_date) AS d FROM stock_daily")
    return str(df["d"].iloc[0])

def _latest_pred_date(loop: ClosedLoop, dt: str) -> str | None:
    df = loop.repo.read_sql(
        "SELECT MAX(trade_date) AS d FROM model_prediction WHERE version=:v AND trade_date<=:d",
        {"v": loop.cfg.version, "d": dt},
    )
    d = df["d"].iloc[0]
    return str(d) if d is not None else None


def _name_map(loop: ClosedLoop, codes: list[str]) -> dict[str, str]:
    if not codes:
        return {}
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    df = loop.repo.read_sql(
        f"SELECT ts_code AS code, name FROM stock_info WHERE ts_code IN ({ph})", params)
    return dict(zip(df["code"], df["name"]))


def _close_hist(loop: ClosedLoop, code: str, start: str, dt: str) -> pd.Series:
    df = loop.repo.read_sql(
        "SELECT trade_date, close FROM stock_daily "
        "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
        {"c": code, "s": start, "d": dt},
    )
    if df.empty:
        return pd.Series(dtype=float)
    return pd.to_numeric(df.set_index("trade_date")["close"], errors="coerce")


def _round_lot(shares: float) -> float:
    """A 股按 100 股取整（卖出允许零股，这里统一向最接近的手取整）。"""
    return float(round(shares / 100.0) * 100)


def build_action_plan(engine, cfg: LoopConfig | None = None, dt: str | None = None,
                      cash: float = 0.0, persist: bool = True,
                      min_trade: float = 0.01) -> ActionPlan:
    """生成操作计划。

    engine：数据库引擎；cfg：LoopConfig（含 risk / portfolio / version）；
    dt：决策日（默认最新数据日）；cash：账户现金（用于折算总权益与股数）。
    """
    loop = ClosedLoop(engine, cfg)
    dt = dt or _latest_data_date(loop)
    rc = loop.cfg.risk
    hrepo = HoldingRepo(engine)
    holdings = hrepo.get_all()

    # ── 当前持仓估值 ──
    held_codes = list(holdings["code"]) if not holdings.empty else []
    last_close: dict[str, float] = {}
    cost_map: dict[str, float] = {}
    shares_map: dict[str, float] = {}
    entry_map: dict[str, str] = {}
    for _, h in holdings.iterrows():
        s = _close_hist(loop, h["code"], dt, dt)
        px = float(s.iloc[-1]) if not s.empty else float(h["cost_price"] or 0)
        last_close[h["code"]] = px
        cost_map[h["code"]] = float(h["cost_price"] or 0)
        shares_map[h["code"]] = float(h["shares"] or 0)
        entry_map[h["code"]] = str(h["entry_date"] or "")
    mv = {c: last_close[c] * shares_map[c] for c in held_codes}
    equity = sum(mv.values()) + max(0.0, cash)
    if equity <= 0:
        equity = 1.0
    cur_w = {c: mv[c] / equity for c in held_codes}

    # ── 目标组合（投顾为主 + 量化补充）──
    pred_date = _latest_pred_date(loop, dt)
    preds = loop.pred_repo.get_predictions(pred_date, loop.cfg.version) if pred_date else pd.DataFrame()
    u = set(loop.uni_repo.get_universe(pred_date, loop.cfg.universe.method)) if pred_date else set()
    if u and not preds.empty:
        preds = preds[preds["code"].isin(u)]
    gross = loop.mt.gross_exposure(dt, list(u) if u else None)
    targets, meta = loop._build_targets(dt, preds, gross)
    exit_codes = loop.adv_repo.get_exit_codes(dt)

    # ── 逐票决策 ──
    all_codes = sorted(set(held_codes) | set(targets))
    names = _name_map(loop, all_codes)
    # 目标(非持仓)票补当日收盘价，用于折算股数/参考买入价
    missing_px = [c for c in all_codes if c not in last_close]
    if missing_px:
        ph = ",".join(f":c{i}" for i in range(len(missing_px)))
        params = {f"c{i}": c for i, c in enumerate(missing_px)}
        params["d"] = dt
        pxdf = loop.repo.read_sql(
            f"SELECT code, close FROM stock_daily WHERE trade_date=:d AND code IN ({ph})", params)
        for _, rr in pxdf.iterrows():
            last_close[rr["code"]] = float(pd.to_numeric(rr["close"], errors="coerce"))
    warm = (pd.to_datetime(dt) - pd.Timedelta(days=150)).strftime("%Y%m%d")
    rows: list[dict] = []
    for c in all_codes:
        cw = cur_w.get(c, 0.0)
        tw = float(targets.get(c, 0.0))
        px = last_close.get(c)
        reason, stop_price = "", float("nan")
        grade = (meta.get(c, {}) or {}).get("grade")

        # 持仓的风控评估（优先级最高）
        if c in held_codes:
            entry = entry_map[c] or warm
            hist = _close_hist(loop, c, min(entry, warm), dt)
            hold_hist = hist[hist.index >= entry] if entry else hist
            if not hold_hist.empty and rc.enabled:
                prev = replay_tier(hold_hist[hold_hist.index < dt], full=rc.trail_full)
                dec = evaluate_holding(hold_hist, cost_map[c], rc,
                                       in_exit_codes=(c in exit_codes), prev_tier=prev)
                stop_price = dec.stop_price
                if dec.action == "exit":
                    tw, reason = 0.0, dec.reason
                elif dec.action == "trim":
                    tw, reason = cw * dec.keep_frac, dec.reason
            if not np.isfinite(stop_price) and cost_map[c] > 0:
                stop_price = cost_map[c] * (1 - rc.hard_stop_pct)

        # 动作判定
        if reason:
            action = "sell" if tw <= 1e-6 else "trim"
        elif cw <= 1e-6 and tw > 1e-6:
            action, reason = "buy", _entry_reason(grade, meta.get(c, {}))
        elif tw <= 1e-6 and cw > 1e-6:
            action, reason = "sell", "换出（已不在目标）"
        elif tw - cw > min_trade:
            action, reason = "add", _entry_reason(grade, meta.get(c, {}))
        elif cw - tw > min_trade:
            action, reason = "trim", "目标减配"
        else:
            action, reason = "hold", "持有"

        if action == "hold" and abs(tw - cw) < min_trade:
            shares_delta = 0.0
        else:
            shares_delta = _round_lot((tw - cw) * equity / px) if px and px > 0 else 0.0

        rows.append({
            "plan_date": dt, "code": c, "name": names.get(c, ""),
            "action": action, "cur_weight": round(cw, 4), "tgt_weight": round(tw, 4),
            "shares_delta": shares_delta, "reason": reason,
            "stop_price": round(stop_price, 3) if np.isfinite(stop_price) else None,
            "ref_price": round(px, 3) if px else None, "grade": grade,
        })

    # ── 账户层 ──
    cost_basis = sum(cost_map[c] * shares_map[c] for c in held_codes)
    unreal = (sum(mv.values()) - cost_basis) / cost_basis if cost_basis > 0 else 0.0
    account = {
        "plan_date": dt, "equity": round(equity, 2),
        "invested_pct": round(sum(mv.values()) / equity, 4),
        "cash_pct": round(max(0.0, cash) / equity, 4),
        "n_holdings": len(held_codes),
        "unrealized_pnl_pct": round(unreal, 4),
        "gross_target": round(gross, 4),
        # 注：实盘缺账户峰值，用「持仓整体浮亏」近似账户级 -7% 风控提示
        "risk_off": bool(rc.enabled and rc.account_dd_stop and unreal <= -rc.account_dd_stop),
    }

    plan = ActionPlan(plan_date=dt, rows=rows, account=account)
    if persist and rows:
        cols = ["plan_date", "code", "name", "action", "cur_weight", "tgt_weight",
                "shares_delta", "reason", "stop_price", "ref_price", "grade"]
        loop.repo.upsert("action_plan", pd.DataFrame(rows)[cols], ["plan_date", "code"])
    return plan


def _entry_reason(grade, meta: dict) -> str:
    src = (meta or {}).get("source")
    if src == "advisor" and grade:
        return f"投顾{grade}级推荐"
    return "量化补充" if src == "quant" else "目标加配"


_ACTION_CN = {"buy": "买入", "add": "加仓", "trim": "减仓", "sell": "清仓", "hold": "持有"}


def render_markdown(plan: ActionPlan) -> str:
    a = plan.account
    lines = [f"# 操作计划 — {plan.plan_date}", ""]
    lines.append(
        f"- 总权益: {a.get('equity')} | 持仓占比: {a.get('invested_pct', 0):.0%} | "
        f"现金占比: {a.get('cash_pct', 0):.0%} | 目标仓位: {a.get('gross_target', 0):.0%}")
    lines.append(
        f"- 持仓数: {a.get('n_holdings')} | 整体浮盈亏: {a.get('unrealized_pnl_pct', 0):+.1%} | "
        f"账户风控(risk_off): {'⚠️ 触发，建议降仓' if a.get('risk_off') else '正常'}")
    lines.append("")
    order = {"sell": 0, "trim": 1, "buy": 2, "add": 3, "hold": 4}
    rows = sorted(plan.rows, key=lambda r: (order.get(r["action"], 9), -r["tgt_weight"]))
    lines.append("| 代码 | 名称 | 动作 | 现权重→目标 | 约股数 | 理由 | 止损价 | 参考价 | 分级 |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        sd = int(r["shares_delta"])
        sd_s = f"+{sd}" if sd > 0 else (str(sd) if sd < 0 else "—")
        lines.append(
            f"| {r['code']} | {r['name']} | {_ACTION_CN.get(r['action'], r['action'])} | "
            f"{r['cur_weight']:.1%}→{r['tgt_weight']:.1%} | {sd_s} | {r['reason']} | "
            f"{r['stop_price'] if r['stop_price'] is not None else '—'} | "
            f"{r['ref_price'] if r['ref_price'] is not None else '—'} | {r['grade'] or '—'} |")
    return "\n".join(lines)
