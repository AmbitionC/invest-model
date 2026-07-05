"""统一资金账本分配：单一资金池 → 三 sleeve（含恐慌弹药）。

对应文档第 6 节头寸与配比 + 恐慌弹药（fear≥阈值把现金→进攻/α）。始终经
ledger_invariant 校验零杠杆红线（在调用方 action_plan/ledger_backtest 断言）。
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from invest_model.arb.config import ArbConfig
from invest_model.portfolio.arb_risk import ledger_invariant


def _mid(lo: float, hi: float) -> float:
    return (lo + hi) / 2.0


def allocate_sleeves(cfg: ArbConfig, fear_score: float | None = None,
                     offense_demand: float | None = None) -> dict[str, float]:
    """把资金池分给 {defense_A, offense_B, alpha, cash}，Σ≤1 零杠杆。

    - 基线取各 sleeve 上下限中点。
    - offense_demand（引擎 B 内部择时需求，[0,1]）在 offense 区间内插值。
    - 恐慌弹药：fear≥trigger 时把 defense/cash 让度给 offense/alpha（买高赔率概率）。
    - cash = 1 - Σ 三 sleeve（Σ 一定 ≤1）。
    """
    b_lo, b_hi = cfg.offense_min, cfg.offense_max
    d_lo, d_hi = cfg.defense_min, cfg.defense_max
    a_lo, a_hi = cfg.alpha_min, cfg.alpha_max

    panic = bool(cfg.panic_ammo and fear_score is not None and fear_score >= cfg.fear_ammo_trigger)

    if panic:
        # 恐慌弹药：防守底盘（含逆回购现金弹药）让度给进攻/α，Σ 仍守零杠杆。
        # 优先把预算给进攻（β 机会）顶到上限，其次盲区 α，防守压到下限。
        defense = d_lo
        budget = 1.0 - defense
        offense = min(b_hi, budget)
        budget -= offense
        alpha = min(a_hi, max(a_lo if budget >= a_lo else 0.0, budget))
    else:
        offense = _mid(b_lo, b_hi)
        if offense_demand is not None:
            offense = b_lo + max(0.0, min(1.0, offense_demand)) * (b_hi - b_lo)
        defense = _mid(d_lo, d_hi)
        alpha = _mid(a_lo, a_hi)

    sleeves = {"defense_A": defense, "offense_B": offense, "alpha": alpha}
    ok, scale, _ = ledger_invariant(sleeves)
    if not ok:
        sleeves = {k: v * scale for k, v in sleeves.items()}
    cash = max(0.0, 1.0 - sum(sleeves.values()))
    return {**sleeves, "cash": cash, "_panic": panic}


def _round_lot(shares: float) -> float:
    """A 股 100 股/手取整。"""
    return float(int(max(0.0, shares) // 100 * 100))


def build_arb_plan(engine, dt: str, cfg: ArbConfig, equity: float, gross: float,
                   fear_score: float | None, held_codes: list[str]) -> dict:
    """统一资金账本：分配 sleeve、生成 carry/α 行、断言零杠杆。

    返回 dict：{alloc, arb_rows, account_extra, sleeve_rows, viol_hint, offense_scale}。
    - cfg.enabled=False（观察态）：不发 arb 行、不缩放引擎 B（offense_scale=1.0），
      仅给出观测性 alloc/account_extra，保证计划与今天逐字一致。
    - cfg.enabled=True：发 defense_A/α 行，offense_scale 把引擎 B 缩进 offense 预算。
    """
    from invest_model.arb.carry import build_carry_signals
    from invest_model.repositories.arb_repo import AlphaRepo, CarryRepo
    from invest_model.repositories.base import BaseRepository

    repo = BaseRepository(engine)
    alloc = allocate_sleeves(cfg, fear_score=fear_score, offense_demand=gross)
    ok, scale, viol = ledger_invariant(
        {k: alloc[k] for k in ("defense_A", "offense_B", "alpha")})
    account_extra = {
        "defense_pct": round(alloc["defense_A"], 4),
        "offense_pct": round(alloc["offense_B"], 4),
        "alpha_pct": round(alloc["alpha"], 4),
        "sleeve_gross": round(alloc["defense_A"] + alloc["offense_B"] + alloc["alpha"], 4),
        "ledger_ok": int(ok),
        "fear_score": round(fear_score, 2) if fear_score is not None else None,
    }
    bounds = cfg.sleeve_bounds()
    sleeve_rows = [{
        "plan_date": dt, "sleeve": s, "version": cfg.version,
        "target_pct": alloc.get(s if s != "cash" else "cash", 0.0),
        "actual_pct": None, "min_pct": bounds.get(s, (0.0, 1.0))[0],
        "max_pct": bounds.get(s, (0.0, 1.0))[1], "nav": None,
        "note": "observe" if not cfg.enabled else "live",
    } for s in ("defense_A", "offense_B", "alpha", "cash")]

    if not cfg.enabled:
        return {"alloc": alloc, "arb_rows": [], "account_extra": account_extra,
                "sleeve_rows": sleeve_rows, "viol_hint": viol, "offense_scale": 1.0,
                "carry_expected": None}

    # ── 启用态：生成 carry / α 行 ──
    try:
        build_carry_signals(engine, dt, cfg, persist=True)
    except Exception:  # noqa: BLE001
        pass
    carry_repo = CarryRepo(engine)
    arb_rows: list[dict] = []
    carry_expected_total = 0.0

    def _px(code: str) -> float:
        tbl = "cb_daily" if code.endswith((".SH", ".SZ")) and code[0] in "1" else "stock_daily"
        # 逆回购按面值 100；可转债/股票取最新收盘
        if code in ("204001.SH",) or code.startswith(("2040", "1318")):
            return 100.0
        for t in ("stock_daily", "cb_daily"):
            if repo.table_exists(t):
                d = repo.read_sql(
                    f"SELECT close FROM {t} WHERE code=:c AND trade_date<=:d "
                    f"ORDER BY trade_date DESC LIMIT 1", {"c": code, "d": dt})
                if not d.empty and pd.notna(d["close"].iloc[0]):
                    return float(d["close"].iloc[0])
        return 100.0

    # defense_A：逆回购(1) + 红利(top) + 可转债(top)，等分 defense 预算
    defense_budget = alloc["defense_A"]
    defense_items: list[tuple[str, str, float]] = []   # (code, note, expected_carry)
    for sleeve, label in (("reverse_repo", "逆回购carry"),
                          ("dividend_carry", "红利carry"),
                          ("convertible", "可转债双低")):
        cs = carry_repo.get(dt, {"reverse_repo": cfg.version, "dividend_carry": cfg.version,
                                 "convertible": cfg.version}.get(sleeve, cfg.version),
                            sleeve=sleeve)
        # carry_signal 按各自 version 落库；这里放宽 version 直接按 sleeve 读
        cs = repo.read_sql(
            "SELECT code, expected_carry, rank, metric FROM carry_signal "
            "WHERE trade_date=:d AND sleeve=:s ORDER BY rank LIMIT :n",
            {"d": dt, "s": sleeve, "n": 1 if sleeve == "reverse_repo" else cfg.dividend_top_n})
        for _, r in cs.iterrows():
            ec = float(r["expected_carry"]) if pd.notna(r["expected_carry"]) else 0.0
            defense_items.append((str(r["code"]), label, ec))
    if defense_items:
        w_each = defense_budget / len(defense_items)
        for code, label, ec in defense_items:
            px = _px(code)
            carry_expected_total += w_each * ec
            arb_rows.append({
                "plan_date": dt, "code": code, "name": label, "action": "buy",
                "cur_weight": 0.0, "tgt_weight": round(w_each, 4),
                "shares_delta": _round_lot(w_each * equity / px) if px > 0 else 0.0,
                "reason": f"{label}·稳吃利差", "stop_price": None,
                "ref_price": round(px, 3), "grade": None,
                "trigger": None, "model_rank": None, "model_view": "防守底盘",
                "sleeve": "defense_A"})

    # alpha：alpha_candidate 未证伪的等权（单票 ≤ name_cap，总 ≤ alpha 预算）
    if repo.table_exists("alpha_candidate"):
        acand = AlphaRepo(engine).get_active(dt, "arb_alpha_v1")
        if acand is not None and not acand.empty:
            acand = acand[acand.get("falsified", -1) != 1] if "falsified" in acand.columns else acand
            codes = list(acand["code"])
            if codes:
                w_each = min(cfg.alpha_name_cap, alloc["alpha"] / len(codes))
                for _, r in acand.iterrows():
                    px = _px(str(r["code"]))
                    arb_rows.append({
                        "plan_date": dt, "code": str(r["code"]),
                        "name": str(r.get("theme") or "盲区α"), "action": "buy",
                        "cur_weight": 0.0, "tgt_weight": round(w_each, 4),
                        "shares_delta": _round_lot(w_each * equity / px) if px > 0 else 0.0,
                        "reason": "盲区α·买高赔率概率（亏得起）", "stop_price": None,
                        "ref_price": round(px, 3), "grade": str(r.get("grade") or ""),
                        "trigger": "逻辑止损:水表反转", "model_rank": None,
                        "model_view": "盲区α", "sleeve": "alpha"})

    account_extra["carry_expected"] = round(carry_expected_total, 6)
    # 引擎 B 缩放系数：把现有股票目标权重整体缩进 offense 预算
    offense_scale = alloc["offense_B"] / gross if gross and gross > 0 else 1.0
    return {"alloc": alloc, "arb_rows": arb_rows, "account_extra": account_extra,
            "sleeve_rows": sleeve_rows, "viol_hint": viol,
            "offense_scale": offense_scale, "carry_expected": carry_expected_total}
