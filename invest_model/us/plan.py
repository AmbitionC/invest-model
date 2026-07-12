"""美股每日计划：三层结构（核心锚/期权造血/卫星α）建议生成 + 落库 + Markdown。

每条建议自带 reason 与〔规则US-xx〕编号，溯源 docs/us_rulebook.md → life-teachers。
纯建议输出（人工执行）：$20k 账户 + 期权流动性下，自动下单不在 V1 范围。
"""

from __future__ import annotations

import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository
from invest_model.us import config as C
from invest_model.us import fundamentals as F
from invest_model.us import options as O
from invest_model.us import signals as S

logger = get_logger()


def _closes(repo, code: str) -> pd.Series:
    df = repo.read_sql(
        "SELECT close FROM us_stock_daily WHERE code=:c ORDER BY trade_date", {"c": code})
    return df["close"] if not df.empty else pd.Series(dtype=float)


def _last_close(repo, code: str) -> float | None:
    s = _closes(repo, code)
    return float(s.iloc[-1]) if len(s) else None


def build_plan(engine, fetch_chain=None) -> dict:
    """生成当日计划。fetch_chain 可注入（测试用），默认 yfinance 实取。"""
    repo = BaseRepository(engine)
    latest = repo.read_sql(
        "SELECT MAX(trade_date) d FROM us_stock_daily WHERE code!='^VIX'")["d"].iloc[0]
    if latest is None:
        return {"plan": "skip:no-data"}
    plan_date = str(latest)

    # ── 市场信号 ──
    vix = _last_close(repo, C.VIX_CODE)
    bench = _closes(repo, C.TREND_BENCH)
    trend = S.ma_trend(bench)
    dd = S.drawdown_from_high(bench)
    regime = S.vix_regime(vix)
    dip = S.dip_window(vix, dd)
    put_mode = S.selling_puts_mode(vix, trend)

    # ── 估值锚（V2，全哥"第一前提是便宜"）──
    vals = repo.read_sql(
        "SELECT code, payback_years, verdict, anchor_price, chase_high "
        "FROM us_valuation WHERE asof=(SELECT MAX(asof) FROM us_valuation)")
    val_map = {str(r["code"]): r for _, r in vals.iterrows()}

    # ── 账户 ──
    snap = repo.read_sql(
        "SELECT cash, market_value, total_asset FROM us_account_snapshot "
        "ORDER BY snapshot_date DESC LIMIT 1")
    total = float(snap["total_asset"].iloc[0]) if not snap.empty else C.START_CASH
    cash = float(snap["cash"].iloc[0]) if not snap.empty else C.START_CASH
    holdings = repo.read_sql(
        "SELECT code, shares, cost_price, sleeve FROM us_current_holding")
    hold_map = {str(r["code"]): r for _, r in holdings.iterrows()}

    rows: list[dict] = []          # us_action_plan
    opt_rows: list[pd.DataFrame] = []

    # ── 1) 核心锚（US-C1）──
    core_budget = total * C.SLEEVE_CORE
    core_ratio = S.core_target_ratio(trend)
    core_target = core_budget * core_ratio
    core_px = _last_close(repo, C.CORE_ETF)
    core_hold = hold_map.get(C.CORE_ETF)
    core_val = float(core_hold["shares"]) * core_px if (core_hold is not None and core_px) else 0.0
    if core_px:
        gap = core_target - core_val
        act = "hold"
        if gap > core_px:          # 差额够买 1 股才行动（$20k 无碎股假设）
            act = "buy"
        elif gap < -core_px:
            act = "trim"
        rows.append({
            "plan_date": plan_date, "code": C.CORE_ETF, "sleeve": "core",
            "action": act, "grade": "A", "target_value": round(core_target, 2),
            "source_tag": "修复" if (act == "buy" and trend == "below") else "成长",
            "reason": (f"{C.TREND_BENCH} 在{C.MA_TREND}日线{'上' if trend == 'above' else '下'}"
                       f"→核心仓目标 {core_ratio:.0%}×{C.SLEEVE_CORE:.0%} 仓位"
                       f"=${core_target:,.0f}（现 ${core_val:,.0f}，"
                       f"{'加' if gap > 0 else '减' if act == 'trim' else '持'}）"
                       f"〔规则US-C1〕")})

    # ── 2) 卫星 α（US-F1~F3：二阶导 + 排雷 + 分级）──
    watch = repo.read_sql(
        "SELECT code, name, sleeve_hint FROM us_stock_info "
        "WHERE kind='stock' ORDER BY code")
    sat_budget = total * C.SLEEVE_SATELLITE
    graded: list[tuple[str, str, str, float | None]] = []
    for _, w in watch.iterrows():
        code = str(w["code"])
        q = repo.read_sql(
            "SELECT * FROM us_fundamental_q WHERE code=:c ORDER BY quarter_end",
            {"c": code})
        accel = F.growth_accel(q)
        flags = F.mine_probes(q)
        ni_yoy = None
        if not q.empty and pd.notna(q["ni_yoy"].iloc[-1]):
            ni_yoy = float(q["ni_yoy"].iloc[-1])
        grade, why = F.certainty_grade(accel, flags, ni_yoy)
        graded.append((code, grade, why, ni_yoy))
    order = {"A": 0, "B": 1, "C": 2}
    graded.sort(key=lambda x: (order[x[1]], x[0]))
    # V2 三闸合一：估值准入（第一前提是便宜）× 排雷/增速分级 × 追高禁令。
    # 确定性决定仓位：A=满卫星仓 B=半仓 C=仅追踪；expensive/追高 一律不 buy。
    for code, grade, why, _ in graded:
        held = code in hold_map
        v = val_map.get(code)
        vd = str(v["verdict"]) if v is not None else "unknown"
        pby = float(v["payback_years"]) if v is not None and pd.notna(v["payback_years"]) else None
        chase = bool(int(v["chase_high"])) if v is not None and pd.notna(v["chase_high"]) else False
        val_note = (f"回本{pby:.0f}年·{vd}" if pby is not None and pby < 9999
                    else "不赚真钱" if pby is not None else "估值未知")
        target = sat_budget if grade == "A" else sat_budget / 2 if grade == "B" else 0.0
        if grade == "C":
            action = "sell" if held else "watch"
            if held:
                why += "；已持仓 → 降级离场（宁错杀）"
        elif held:
            # 持有中：估值贵不加仓但不因贵卖出（卖出只由基本面恶化触发——
            # 全哥"别因情绪在估值性波动中割肉"，重远"持有是每期重新通过检验"）
            action = "hold"
        elif chase:
            action = "watch"
            why += f"；追高禁令：距一年低点涨幅>{C.VAL_CHASE_RALLY:.0%}且非cheap（套牢你的是买贵）〔规则US-V3〕"
        elif vd == "expensive":
            action = "watch"
            why += f"；估值闸：{val_note}——高于估值绝不碰，等回落或卖put等接〔规则US-V1〕"
        elif vd in ("cheap", "fair"):
            action = "buy" if (dip or grade == "A" or vd == "cheap") else "watch"
            why += f"；估值 {val_note} 通过准入〔规则US-V1〕"
        else:
            action = "watch"
            why += "；估值数据缺失，不装数据、仅观察〔规则US-V1〕"
        rows.append({
            "plan_date": plan_date, "code": code, "sleeve": "satellite",
            "action": action, "grade": grade,
            "target_value": round(target, 2) if action in ("buy", "hold") else
                            (round(target, 2) if grade in ("A", "B") else 0.0),
            "source_tag": "修复" if dip else "成长", "reason": why})

    # ── 3) 期权造血（US-O1~O4 V2：行权价锚定接盘价；恐慌切 strict 不停卖）──
    income_budget = total * C.SLEEVE_INCOME
    if put_mode == "strict":
        rows.append({
            "plan_date": plan_date, "code": "-", "sleeve": "income",
            "action": "hold", "grade": "-", "target_value": 0.0,
            "source_tag": "造血",
            "reason": (f"strict模式：VIX={vix:.1f}({regime})/趋势{trend}——情绪化最重=IV最厚"
                       f"=期权最好时机（全哥），但只允许 cheap 档+接盘锚九折以下的行权价"
                       f"（守本金收紧）〔规则US-O4·V2〕")})
    if fetch_chain is not None or _yf_available():
        fc = fetch_chain or _default_chain_fetcher
        grade_map = {c: g for c, g, _, _ in graded}
        # 准入：A/B 级 + 担保买得起；strict 模式再要求估值 cheap 档
        cand_codes = []
        for c, g, _, _ in graded:
            if g not in ("A", "B"):
                continue
            v = val_map.get(c)
            vd = str(v["verdict"]) if v is not None else "unknown"
            if put_mode == "strict" and vd != "cheap":
                continue
            px = _last_close(repo, c) or 1e9
            if px * 100 * (1 - C.OPT_MIN_SAFETY) <= income_budget:
                cand_codes.append(c)
        for code in cand_codes[:6]:                    # 控制 API 量
            close = _last_close(repo, code)
            v = val_map.get(code)
            anchor = (float(v["anchor_price"]) if v is not None
                      and pd.notna(v["anchor_price"]) else None)
            got = O.score_csp(fc(code), close, income_budget,
                              quality_ok=grade_map.get(code) in ("A", "B"),
                              anchor=anchor, mode=put_mode)
            if not got.empty:
                opt_rows.append(got.head(3))
        for code, h in hold_map.items():
            if float(h["shares"]) >= 100:
                close = _last_close(repo, code)
                if close:
                    got = O.score_cc(fc(code), close,
                                     float(h["cost_price"] or 0), float(h["shares"]))
                    if not got.empty:
                        # 全哥"判断偏高不大涨才卖call"：持仓估值 expensive 时优先推荐
                        v = val_map.get(code)
                        if v is not None and str(v["verdict"]) == "expensive":
                            got["reason"] = got["reason"] + "（持仓已高估，优先收租〔US-O2·V2〕）"
                        opt_rows.append(got.head(2))

    opts = pd.concat(opt_rows, ignore_index=True) if opt_rows else pd.DataFrame()
    if not opts.empty:
        opts = opts.head(C.OPT_MAX_CANDIDATES)
        opts.insert(0, "plan_date", plan_date)
        repo.upsert("us_option_candidate", opts,
                    ["plan_date", "code", "strategy", "expiry", "strike"])
        best = opts.iloc[0]
        rows.append({
            "plan_date": plan_date, "code": str(best["code"]), "sleeve": "income",
            "action": "csp", "grade": "-",
            "target_value": float(best["collateral"]),
            "source_tag": "造血",
            "reason": (f"造血首选：{best['reason']}（共 {len(opts)} 条候选见期权面板；"
                       f"预算 ${income_budget:,.0f}）")})

    # ── 落库 + 汇总 ──
    plan_df = pd.DataFrame(rows)
    repo.upsert("us_action_plan", plan_df, ["plan_date", "code", "sleeve"])
    mv = total - cash
    account = {
        "plan_date": plan_date, "total_asset": round(total, 2),
        "cash": round(cash, 2), "market_value": round(mv, 2),
        "vix": vix, "vix_regime": regime, "spy_trend": trend,
        "drawdown": round(dd, 4),
        "notes": ("恐慌抄底观察窗开启：现金弹药可分批接（先过个股基本面闸）〔规则US-T1〕"
                  if dip else ""),
    }
    repo.upsert("us_plan_account", pd.DataFrame([account]), ["plan_date"])
    md = render_markdown(plan_date, account, plan_df, opts)
    logger.info(f"us plan {plan_date}：{len(plan_df)} 行建议，{len(opts)} 条期权候选")
    return {"plan": f"ok:{plan_date}", "rows": len(plan_df),
            "options": len(opts), "markdown": md}


def _yf_available() -> bool:
    try:
        import yfinance  # noqa: F401
        return True
    except ImportError:
        return False


def _default_chain_fetcher(code: str) -> pd.DataFrame:
    from invest_model.us import datasource as ds
    return ds.fetch_option_chain(code, C.OPT_DTE_MIN, C.OPT_DTE_MAX)


def render_markdown(plan_date: str, account: dict, plan: pd.DataFrame,
                    opts: pd.DataFrame) -> str:
    """三段式 Markdown（推 GitHub issue + 前端展示同源）。"""
    zh_act = {"hold": "持有", "buy": "买入", "watch": "观察", "trim": "减仓",
              "sell": "离场", "csp": "卖出现金担保put", "cc": "卖出备兑call"}
    lines = [f"# 美股操作计划 — {plan_date}", "",
             f"**账户** 总资产 ${account['total_asset']:,.0f}｜现金 ${account['cash']:,.0f}｜"
             f"VIX {account['vix']:.1f}（{account['vix_regime']}）｜"
             f"{C.TREND_BENCH} {C.MA_TREND}日线{'上方' if account['spy_trend'] == 'above' else '下方'}｜"
             f"回撤 {account['drawdown']:.0%}"]
    if account.get("notes"):
        lines.append(f"\n> ⚡ {account['notes']}")
    for sleeve, title in (("core", "一、核心锚（宽基+趋势纪律）"),
                          ("income", "二、期权造血（现金担保/备兑）"),
                          ("satellite", "三、卫星 α（确定性分级）")):
        seg = plan[plan["sleeve"] == sleeve]
        if seg.empty:
            continue
        lines.append(f"\n## {title}\n")
        for _, r in seg.iterrows():
            act = zh_act.get(str(r["action"]), r["action"])
            tv = f"（目标 ${float(r['target_value']):,.0f}）" if float(r["target_value"]) else ""
            lines.append(f"- **{r['code']}** {act}{tv} `{r['grade']}` — {r['reason']}")
    if not opts.empty:
        lines.append("\n## 期权候选明细\n")
        lines.append("| 标的 | 策略 | 到期 | 行权价 | 权利金 | 年化 | 安全边际 | 担保 |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for _, o in opts.iterrows():
            lines.append(
                f"| {o['code']} | {'CSP' if o['strategy'] == 'csp' else 'CC'} | {o['expiry']} "
                f"| ${float(o['strike']):.1f} | ${float(o['premium']):.2f} "
                f"| {float(o['annualized_yield']):.0%} | {float(o['safety_margin']):.0%} "
                f"| ${float(o['collateral']):,.0f} |")
    lines.append("\n---\n*纯建议输出（人工执行）；每条规则编号见 docs/us_rulebook.md，"
                 "方法论出处 life-teachers 美股专题篇（V2·全哥体系）。零杠杆、绝不裸卖期权、"
                 "不做买方主仓。理性预期：三五年一倍已属顶尖，别追求一年多少倍。*")
    return "\n".join(lines)
