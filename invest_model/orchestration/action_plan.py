"""实盘操作计划（最终交付物）：结合当前持仓 + 目标组合 + 风控评估 → 操作清单。

按需生成：对给定日期（默认最新数据日），对每只持仓/目标票给出
  动作（买/加/减/清/持）+ 当前→目标权重 + 触发理由 + 关键价位 + 账户层提示。

风控判定复用 :mod:`invest_model.portfolio.risk`，与回测同一套逻辑。
目标组合复用 :meth:`ClosedLoop._build_targets`（投顾为主 + 量化补充）。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.orchestration.closed_loop import ClosedLoop, LoopConfig
from invest_model.portfolio.risk import (armed_ladder, evaluate_holding, profit_protect,
                                         replay_hold_tier, replay_pp_tier, time_stop)
from invest_model.portfolio.sizing import buy_shares, min_lot
from invest_model.repositories.holding_repo import HoldingRepo
from invest_model.signals.buypoint import BuyPointConfig, detect_buypoints

logger = get_logger()

WATCH_POOL_CAP = 30    # 观察池上限：A 级全留 + 最近的 B 级，控制在此数量内


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
    m = {str(c): str(n) for c, n in zip(df["code"], df["name"]) if str(n)}
    # 补名：ETF/转债等不在 stock_info，用最新持仓快照里的名称兜底
    miss = [c for c in codes if c not in m]
    if miss:
        try:
            if loop.repo.table_exists("holding_snapshot"):
                ph2 = ",".join(f":m{i}" for i in range(len(miss)))
                p2 = {f"m{i}": c for i, c in enumerate(miss)}
                sn = loop.repo.read_sql(
                    "SELECT code, name FROM holding_snapshot WHERE snapshot_date="
                    "(SELECT MAX(snapshot_date) FROM holding_snapshot) "
                    f"AND code IN ({ph2})", p2)
                for c, n in zip(sn["code"], sn["name"]):
                    if str(n):
                        m[str(c)] = str(n)
        except Exception:  # noqa: BLE001
            pass
    return m


def _close_hist(loop: ClosedLoop, code: str, start: str, dt: str) -> pd.Series:
    """收盘序列（前复权口径，P11）：除权除息缺口抹平后再喂均线/硬止损，
    避免分红/送转日的机械跳空假触发风控；无复权因子时 fail-open 退回原价。"""
    from invest_model.data.adjust import qfq_close_hist
    return qfq_close_hist(loop.repo, code, start, dt)


def _round_lot(shares: float) -> float:
    """A 股按 100 股取整（卖出允许零股，这里统一向最接近的手取整）。"""
    return float(round(shares / 100.0) * 100)


def _trailing_only() -> set[str]:
    """移动止盈白名单（config/trailing_only.txt）：核心主升浪仓，只按破MA20管，
    豁免硬止损与盈利保护（与 scripts/live_check.py 同一份名单、同一语义）。"""
    from pathlib import Path
    p = Path(__file__).resolve().parents[2] / "config" / "trailing_only.txt"
    if not p.exists():
        return set()
    try:
        return {ln.split("#")[0].strip() for ln in p.read_text(encoding="utf-8").splitlines()
                if ln.split("#")[0].strip()}
    except Exception:  # noqa: BLE001
        return set()


def _update_policy_shadow(loop: ClosedLoop, dt: str, reco: pd.DataFrame, bps: dict) -> None:
    """研报速通影子验证：逐日更新两条虚拟净值，供 4~6 周后复核该政策。

    fast＝信号次一交易日收盘直入；gate＝旧严格闸门首次触发日收盘入（未触发即空仓）。
    只记 research A/B 级、近 90 天的信号；每计划日刷新 last_close 与两侧收益。
    """
    if reco is None or reco.empty or "source_type" not in reco.columns:
        return
    back = (pd.to_datetime(dt) - pd.Timedelta(days=90)).strftime("%Y%m%d")
    sig = reco[(reco["source_type"] == "research") & (reco["grade"].isin({"A", "B"}))
               & (reco["rec_date"].astype(str) >= back)]
    if sig.empty:
        return
    exist = pd.DataFrame()
    if loop.repo.table_exists("policy_shadow"):
        exist = loop.repo.read_sql(
            "SELECT * FROM policy_shadow WHERE signal_date>=:b", {"b": back})
    ex_map = {(str(r["signal_date"]), str(r["code"])): dict(r)
              for _, r in exist.iterrows()} if not exist.empty else {}

    def _close_at(code: str, day: str) -> float | None:
        df = loop.repo.read_sql(
            "SELECT close FROM stock_daily WHERE code=:c AND trade_date=:d",
            {"c": code, "d": day})
        if df.empty:
            return None
        v = pd.to_numeric(df["close"].iloc[0], errors="coerce")
        return float(v) if pd.notna(v) and v > 0 else None

    out = []
    for _, s in sig.iterrows():
        key = (str(s["rec_date"]), str(s["code"]))
        row = ex_map.get(key) or {"signal_date": key[0], "code": key[1],
                                  "grade": str(s["grade"]), "d0_date": None,
                                  "d0_close": None, "gate_date": None, "gate_close": None}
        if not row.get("d0_close"):
            d0 = loop.repo.read_sql(
                "SELECT MIN(trade_date) AS d FROM stock_daily "
                "WHERE code=:c AND trade_date>:s AND trade_date<=:d",
                {"c": key[1], "s": key[0], "d": dt})["d"].iloc[0]
            if d0 is not None:
                row["d0_date"], row["d0_close"] = str(d0), _close_at(key[1], str(d0))
        bp = bps.get(key[1])
        if not row.get("gate_close") and bp is not None and getattr(bp, "is_buy", False):
            row["gate_date"], row["gate_close"] = dt, _close_at(key[1], dt)
        last = _close_at(key[1], dt)
        if last:
            row["last_date"], row["last_close"] = dt, last
            d0c = pd.to_numeric(row.get("d0_close"), errors="coerce")
            gtc = pd.to_numeric(row.get("gate_close"), errors="coerce")
            row["fast_ret"] = round(last / float(d0c) - 1, 6) if pd.notna(d0c) and d0c else None
            row["gate_ret"] = round(last / float(gtc) - 1, 6) if pd.notna(gtc) and gtc else None
        out.append({k: row.get(k) for k in (
            "signal_date", "code", "grade", "d0_date", "d0_close", "gate_date",
            "gate_close", "last_date", "last_close", "fast_ret", "gate_ret")})
    if out:
        loop.repo.upsert("policy_shadow", pd.DataFrame(out), ["signal_date", "code"])


def build_action_plan(engine, cfg: LoopConfig | None = None, dt: str | None = None,
                      cash: float = 0.0, persist: bool = True,
                      min_trade: float = 0.01, buypoint: bool = True,
                      bp_cfg: BuyPointConfig | None = None) -> ActionPlan:
    """生成操作计划。

    engine：数据库引擎；cfg：LoopConfig（含 risk / portfolio / version）；
    dt：决策日（默认最新数据日）；cash：账户现金（用于折算总权益与股数）；
    buypoint：True=研报标的先进观察池、仅买点触发才建议买入（手册第1-2步）。
    """
    loop = ClosedLoop(engine, cfg)
    dt = dt or _latest_data_date(loop)
    rc = loop.cfg.risk
    hrepo = HoldingRepo(engine)
    holdings = hrepo.get_all()

    # 持仓现价优先用"不早于最新行情日"的持仓快照(券商真实价)：抗 Tushare EOD 发布延迟、
    # 手动补跑也能反映当日；快照更旧则退回 EOD。均线/历史仍走 stock_daily。{code:(date,price)}
    snap_px: dict[str, tuple[str, float]] = {}
    try:
        if loop.repo.table_exists("holding_snapshot"):
            sp = loop.repo.read_sql(
                "SELECT snapshot_date, code, last_price FROM holding_snapshot "
                "WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM holding_snapshot)")
            for _, r in sp.iterrows():
                v = pd.to_numeric(r["last_price"], errors="coerce")
                if str(r["snapshot_date"]) >= dt and pd.notna(v) and float(v) > 0:
                    snap_px[str(r["code"])] = (str(r["snapshot_date"]), float(v))
    except Exception:  # noqa: BLE001
        pass

    # ── 当前持仓估值 ──
    held_codes = list(holdings["code"]) if not holdings.empty else []
    last_close: dict[str, float] = {}
    cost_map: dict[str, float] = {}
    shares_map: dict[str, float] = {}
    entry_map: dict[str, str] = {}
    for _, h in holdings.iterrows():
        s = _close_hist(loop, h["code"], dt, dt)
        px = float(s.iloc[-1]) if not s.empty else float(h["cost_price"] or 0)
        if h["code"] in snap_px:
            px = snap_px[h["code"]][1]                # 券商快照现价(≥最新行情日)优先
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
    # 模型质量分位（rank_pct = 全市场因子分位；用作投顾标的的"质量参谋"，显性展示）
    rank_map = (dict(zip(preds["code"], pd.to_numeric(preds["rank_pct"], errors="coerce")))
                if not preds.empty and "rank_pct" in preds.columns else {})
    # 因子层归因（可解释性）：每票 top3 贡献因子（score=Σwᵢfᵢ 分解，见 rulebook）
    tf_map = (dict(zip(preds["code"], preds["top_factors"]))
              if not preds.empty and "top_factors" in preds.columns else {})
    # 收益三来源定位（买前定位赚哪种钱：成长/修复/红利——价投批判篇）
    src_map: dict[str, str] = {}
    try:
        if pred_date:
            from invest_model.repositories.factor_repo import FactorRepository
            expo = FactorRepository(engine).get_exposures_wide(pred_date)
            src_map = _return_sources(expo)
    except Exception:  # noqa: BLE001 — 定位失败不阻断计划
        src_map = {}
    # 模型层置信度：注册表交叉验证 IC（信息系数）→ 该版本因子对未来收益的区分力
    m_ic_mean = m_ic_ir = m_hit = None
    try:
        if loop.repo.table_exists("model_registry"):
            mq = loop.repo.read_sql(
                "SELECT cv_ic_mean, cv_ic_ir, cv_hit_rate FROM model_registry WHERE version=:v",
                {"v": loop.cfg.version})
            if not mq.empty:
                m_ic_mean = _f(mq["cv_ic_mean"].iloc[0])
                m_ic_ir = _f(mq["cv_ic_ir"].iloc[0])
                m_hit = _f(mq["cv_hit_rate"].iloc[0])
    except Exception:  # noqa: BLE001
        pass
    model_trust = _model_trust(m_ic_ir)
    gross = loop.mt.gross_exposure(dt, list(u) if u else None)
    targets, meta = loop._build_targets(dt, preds, gross, cur_codes=set(held_codes))
    exit_codes = loop.adv_repo.get_exit_codes(dt)

    # ── 观察池 + 复合买点（手册第1-2步）：研报标的先观察，仅买点触发才建议买入 ──
    # 例外「研报速通」（20260703 数据验证）：research 信号的 α 集中在信号后前几日，
    # 等回踩=逆向选择（103条信号严格闸只放行2%，研报子集立即买均值+12%）。
    # A/B 级研报信号 3 个交易日内免闸半仓直入，余下半仓仍走回踩/突破闸补足；
    # 影子净值落库 policy_shadow 供 4~6 周复核，RESEARCH_FAST_ENTRY=0 一键回退。
    watch_rows: list[dict] = []
    buy_codes: set[str] = set()
    fresh_fast: set[str] = set()
    if buypoint:
        reco = loop.adv_repo.get_active_reco(dt)
        # 观察池收敛：A 级全留 + B 级取最近的，总量封顶（避免历史 B 级堆积把观察池撑爆）。
        pool: list[str] = []
        if not reco.empty:
            p = reco[(reco["direction"] == "long")
                     & (reco["grade"].isin({"A", "B"}))
                     & (~reco["code"].isin(exit_codes))].copy()
            if "rec_date" in p.columns:
                p = p.sort_values("rec_date", ascending=False)
            p = p.drop_duplicates("code")
            a_codes = p[p["grade"] == "A"]["code"].tolist()
            b_codes = p[p["grade"] == "B"]["code"].tolist()
            pool = a_codes + b_codes[: max(0, WATCH_POOL_CAP - len(a_codes))]
        bps = detect_buypoints(engine, dt, pool, gross, rank_map, bp_cfg)
        buy_codes = {c for c, bp in bps.items() if bp.is_buy}
        fast_on = os.getenv("RESEARCH_FAST_ENTRY", "1").lower() not in ("0", "false")
        if fast_on and not reco.empty and "source_type" in reco.columns:
            recent = loop.repo.read_sql(
                "SELECT DISTINCT trade_date FROM stock_daily WHERE trade_date<=:d "
                "ORDER BY trade_date DESC LIMIT 3", {"d": dt})
            cut = str(recent["trade_date"].min()) if len(recent) else dt
            rr = reco[(reco["source_type"] == "research")
                      & (reco["grade"].isin({"A", "B"}))
                      & (reco["rec_date"].astype(str) >= cut)]
            fresh_fast = {c for c in rr["code"]
                          if c not in held_codes and c not in exit_codes}
        # 目标里：投顾票未触发买点的 → 移出建议买入、转观察池（持仓的不动，交风控管；
        # 研报速通票不移出——免闸直入，但只给一半目标权重）
        for c in list(targets):
            if (meta.get(c, {}) or {}).get("source") == "advisor" \
                    and c not in buy_codes and c not in held_codes and c not in fresh_fast:
                targets.pop(c, None)
        for c in fresh_fast & set(targets):
            if c not in buy_codes:            # 闸门已确认的给全额，未确认的先半仓
                targets[c] = float(targets[c]) * 0.5
        if persist:
            try:
                _update_policy_shadow(loop, dt, reco, bps)
            except Exception as e:  # noqa: BLE001 - 影子验证失败不阻断计划生成
                logger.warning(f"policy_shadow 更新失败：{e}")
        # 观察池清单（含趋势未过/未现买点等原因），持仓中的/已进目标的（研报速通）不再列观察
        wnames = _name_map(loop, [c for c in pool if c not in held_codes])
        for c in pool:
            if c in buy_codes or c in held_codes or c in targets:
                continue
            bp = bps.get(c)
            g = reco.loc[reco["code"] == c, "grade"].iloc[0] if not reco.empty else None
            ma20 = getattr(bp, "ma20", float("nan")) if bp else float("nan")
            brk = getattr(bp, "breakout", float("nan")) if bp else float("nan")
            trig = (f"回踩≈{ma20} / 突破>{brk}" if np.isfinite(ma20) and np.isfinite(brk) else "—")
            w_reason = bp.reason if bp else "观察"
            if src_map.get(c):
                w_reason = f"{w_reason}｜定位:{src_map[c]}"
            watch_rows.append({
                "plan_date": dt, "code": c, "name": wnames.get(c, ""), "action": "watch",
                "cur_weight": 0.0, "tgt_weight": 0.0, "shares_delta": 0.0,
                "reason": w_reason, "stop_price": None,
                "ref_price": round(bp.last, 2) if bp and np.isfinite(getattr(bp, "last", float("nan"))) else None,
                "grade": g, "trigger": trig, "model_rank": rank_map.get(c),
                "model_view": _model_view(rank_map.get(c), model_trust, tf_map.get(c))})

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
    reset_floor = (pd.to_datetime(dt) - pd.Timedelta(days=35)).strftime("%Y%m%d")  # 档位回放窗口下限(≈1个调仓周期)
    trail_white = _trailing_only()
    # P16 顶部特征自动减半（用户 2026-07-13 定：控回撤证据充分，直升自动减仓）：
    # 浮盈达标持仓 波动骤放大+放量 → 目标减半一次。start_lb 取 ~2 年，够 250 日波动分位。
    top_start_lb = f"{int(dt[:4]) - 2}{dt[4:]}"
    top_trimmed: set[str] = set()                      # 近一个调仓周期内已因顶部特征减半者 → 不重复减
    try:
        _tt = loop.repo.read_sql(
            "SELECT DISTINCT code FROM action_plan WHERE plan_date>=:s AND plan_date<:d "
            "AND reason LIKE :r", {"s": reset_floor, "d": dt, "r": "%顶部特征%"})
        top_trimmed = set(_tt["code"].tolist()) if not _tt.empty else set()
    except Exception:  # noqa: BLE001 — 首日无历史计划不阻断
        top_trimmed = set()
    rows: list[dict] = []
    for c in all_codes:
        cw = cur_w.get(c, 0.0)
        tw = float(targets.get(c, 0.0))
        px = last_close.get(c)
        reason, stop_price = "", float("nan")
        grade = (meta.get(c, {}) or {}).get("grade")

        # 持仓的风控评估（优先级最高）
        if c in held_codes:
            real_entry = entry_map[c]                         # 真实建仓日（可能为空）
            hist = _close_hist(loop, c, warm, dt)             # 市场窗口：算真实 MA（与建仓日无关）
            cur_day = dt
            if c in snap_px:                                  # 追加当日券商现价 → 风控按最新价判定
                cur_day, snp = snap_px[c]
                # 当日 EOD 已入库则以官方收盘为准（风控是收盘价规则），快照只补 EOD 缺口——
                # 否则同日两行会污染均线（当日双计）、armed_ladder 的“截至昨日”回放
                # （iloc[:-1] 把今日 EOD 当昨日，梯子破位信号被永久吞掉）与时间止损天数。
                if cur_day not in hist.index:
                    hist = pd.concat([hist, pd.Series({cur_day: snp})])
            hold_hist = hist[hist.index >= real_entry] if real_entry else hist.iloc[0:0]  # 自建仓日(供时间止损)
            if not hist.empty and rc.enabled:
                # 移动止盈档位回放起点：真实建仓日 / 最近调仓日 / dt-35天 取最晚，
                # 限定在"当前调仓周期"内单调 → 与回测的每调仓日重置对齐；
                # 避免长持 winner 被几个月前的一次破位永久锁死"破MA20清仓"。
                reset_from = max(x for x in (real_entry, pred_date, reset_floor) if x)
                # P10 感知回放：均线用整段 hist（含 150 日预热）计算、只在窗口内推进档位，
                # 迁移规则与 evaluate_holding 同构 —— 保证「首破减半→之后持有」在实盘
                # 重建 prev_tier 时成立（此前喂窗口切片给 replay_tier：前 19 行 MA20=NaN
                # 记不上档 → 新仓每天重复减半；记上了又是档 3 → 收复 MA20 转盈即被清）。
                prev = replay_hold_tier(hist[hist.index < cur_day], cost_map[c], rc,
                                        replay_from=reset_from)
                # 白名单核心仓：只按均线移动止盈管，豁免硬止损/盈利保护/梯子
                # （与 live_check 盘中口径一致——此前盘后漏传导致对白名单票照发硬止损）
                exempt = c in trail_white
                dec = evaluate_holding(hist, cost_map[c], rc,
                                       in_exit_codes=(c in exit_codes), prev_tier=prev,
                                       exempt_hard_stop=exempt)
                stop_price = dec.stop_price
                if dec.action == "exit":
                    tw, reason = 0.0, dec.reason
                elif dec.action == "trim":
                    tw, reason = cw * dec.keep_frac, dec.reason
                # 盈利保护（回撤止盈）：浮盈达标后自峰值回撤锁盈——先于时间止损检查。
                # 补 MA20 追踪对高位票「回吐 30%+ 才触发」的缺口（如 巨化 54.8→49.4 无动作）。
                elif entry_map[c] and not hold_hist.empty:
                    pp_prev = replay_pp_tier(hold_hist[hold_hist.index < cur_day],
                                             cost_map[c], rc)
                    ppd = None if exempt else profit_protect(
                        hold_hist, cost_map[c], rc, prev_tier=pp_prev)
                    # 盈利后均线梯子：与峰值回撤并行，先触发者生效（回测：回吐 19.8%→13.3%）
                    lad = None if exempt else armed_ladder(hist, real_entry, cost_map[c], rc)
                    guard = None
                    for cand_dec in (ppd, lad):
                        if cand_dec is None:
                            continue
                        if guard is None or (cand_dec.action == "exit" and guard.action != "exit"):
                            guard = cand_dec
                    if guard is not None:
                        tw = 0.0 if guard.action == "exit" else cw * guard.keep_frac
                        reason = guard.reason
                    else:
                        # 时间止损（手册第3步）：仅在未触发其它风控时检查
                        ts = time_stop(hold_hist, rc, prev_tier=prev)
                        if ts is not None:
                            tw = 0.0 if ts.action == "exit" else cw * ts.keep_frac
                            reason = ts.reason
            if not np.isfinite(stop_price) and cost_map[c] > 0:
                stop_price = cost_map[c] * (1 - rc.hard_stop_pct)

            # P16 顶部特征自动减半：仅在其它风控未触发（reason 为空）、非白名单核心仓、
            # 未在本周期减过时；浮盈达标+波动骤放大+放量 → 目标减半一次（锁盈不砍损）。
            if (rc.enabled and not reason and c not in trail_white
                    and c not in top_trimmed and cw > 1e-6):
                try:
                    from invest_model.signals.top_feature import top_feature_now
                    tf_close = _close_hist(loop, c, top_start_lb, dt)
                    _vs = loop.repo.read_sql(
                        "SELECT trade_date, volume FROM stock_daily "
                        "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
                        {"c": c, "s": top_start_lb, "d": dt})
                    tf_vol = (pd.to_numeric(_vs.set_index("trade_date")["volume"], errors="coerce")
                              if not _vs.empty else pd.Series(dtype=float))
                    tf_vol.index = tf_vol.index.astype(str)
                    if (not tf_close.empty and top_feature_now(
                            tf_close, tf_vol.reindex(tf_close.index),
                            cost_map.get(c, 0.0), entry_map.get(c) or None)):
                        tw, reason = cw * 0.5, "顶部特征减半（P16·波动骤放大+放量、浮盈达标）"
                except Exception:  # noqa: BLE001 — 顶部信号失败不阻断计划
                    pass

        # 动作判定
        if reason:                                    # 风控已判定（清仓/减仓/时间止损/逻辑证伪）
            action = "sell" if tw <= 1e-6 else "trim"
            # 盈利仓的风控离场本质是止盈（如 巨化 +18% 破MA20 清仓），文案只写
            # "破MA20清仓"曾被误读成止损——展示层加「止盈·」前缀并附浮盈，不改判定。
            # 逻辑证伪除外：那是论点失效离场，与盈亏无关，标"止盈"会误导。
            cost = cost_map.get(c, 0.0)
            if (cost and cost > 0 and px and px > 0 and px / cost - 1 > 0
                    and not reason.startswith("逻辑证伪")):
                reason = f"止盈·{reason}（浮盈{px / cost - 1:+.1%}）"
        elif c in held_codes:
            # 尊重真实持仓、实事求是：风控没触发就持有——不因"没挤进模型 top-N 目标"而强制换出/减配。
            # 换出只保留给风控触发 / 投顾明确剔除（exit_codes 已在风控里判为逻辑证伪清仓）。
            action, reason, tw = "hold", "持有", cw
        elif cw <= 1e-6 and tw > 1e-6:                 # 非持仓、进入目标 → 新建仓（买点已在观察池闸控）
            action, reason = "buy", _entry_reason(grade, meta.get(c, {}))
            if src_map.get(c):                          # 买前定位赚哪种钱（收益三来源）
                reason = f"{reason}｜定位:{src_map[c]}"
        else:
            action, reason = "hold", "持有"

        if action == "hold" and abs(tw - cw) < min_trade:
            shares_delta = 0.0
        elif action == "buy":
            # 买入按可执行口径定股数：整手/科创板200股起。高价股一手远超目标增量时
            # 判不可执行 → 降级为观察并明说原因，不再输出「—」股数的死指令。
            shares_delta = buy_shares(c, (tw - cw) * equity, px or 0.0)
            if shares_delta <= 0:
                lot = min_lot(c)
                lot_txt = (f"最小一笔{lot}股≈{lot * px:,.0f}元(占{lot * px / equity:.1%})"
                           if px and px > 0 and equity else f"最小一笔{lot}股")
                action, tw = "watch", cw
                reason = f"买点有效但不可执行：{lot_txt} 远超目标增量——账户规模不足，跳过"
        else:
            shares_delta = _round_lot((tw - cw) * equity / px) if px and px > 0 else 0.0

        trigger = (f"挂单≈{round(px, 2)}" if action in ("buy", "add") and px else "—")
        if action == "buy" and c in fresh_fast and c not in buy_codes and px:
            # 研报速通：不追高开（次日平均跳空+3.2%后日内回落），尾盘建半仓
            reason = f"研报速通·半仓直入：{reason}"
            trigger = f"次日尾盘≤{round(px, 2)}建半仓；回踩带补半仓；3日内有效"
        rows.append({
            "plan_date": dt, "code": c, "name": names.get(c, ""),
            "action": action, "cur_weight": round(cw, 4), "tgt_weight": round(tw, 4),
            "shares_delta": shares_delta, "reason": reason,
            "stop_price": round(stop_price, 3) if np.isfinite(stop_price) else None,
            "ref_price": round(px, 3) if px else None, "grade": grade, "trigger": trigger,
            "model_rank": rank_map.get(c),
            "model_view": _model_view(rank_map.get(c), model_trust, tf_map.get(c)),
        })

    # ── 再入场（回测：右尾代价补回 2/3）──
    # 近 45 天内被盈利保护/梯子清仓的票，收盘创出离场以来区间新高 → 半仓接回，
    # 把「止盈下车后主升浪继续」的踏空补回来（002378 场景）。
    if rc.enabled and rc.reentry:
        try:
            back = (pd.to_datetime(dt) - pd.Timedelta(days=45)).strftime("%Y%m%d")
            ex_df = loop.repo.read_sql(
                "SELECT DISTINCT code FROM action_plan WHERE plan_date>=:b AND plan_date<:d "
                # 前缀匹配→包含匹配：reason 现在可能带「止盈·」前缀（见上"动作判定"）
                "AND action='sell' AND reason LIKE :r", {"b": back, "d": dt, "r": "%盈利保护%"})
            re_codes = [c for c in ex_df["code"]
                        if c not in held_codes and c not in targets and c not in exit_codes]
            re_names = _name_map(loop, re_codes)
            half_w = round(0.5 * (float(np.mean(list(targets.values()))) if targets else 0.05), 4)
            for c in re_codes:
                h = _close_hist(loop, c, back, dt)
                if len(h) < 5:
                    continue
                px = float(h.iloc[-1])
                if not (np.isfinite(px) and px > 0 and px >= float(h.iloc[:-1].max())):
                    continue
                re_sd = buy_shares(c, half_w * equity, px)
                if re_sd <= 0:                 # 高价股半仓不足最小一笔 → 不发不可执行指令
                    continue
                rows.append({
                    "plan_date": dt, "code": c, "name": re_names.get(c, ""),
                    "action": "buy", "cur_weight": 0.0, "tgt_weight": half_w,
                    "shares_delta": re_sd,
                    "reason": "创新高确认·半仓再入场（盈利止盈离场后趋势延续）",
                    "stop_price": round(px * (1 - rc.hard_stop_pct), 3),
                    "ref_price": round(px, 3), "grade": None,
                    "trigger": f"挂单≈{round(px, 2)}",
                    "model_rank": rank_map.get(c),
                    "model_view": _model_view(rank_map.get(c), model_trust)})
        except Exception as e:  # noqa: BLE001 - 再入场判定失败不阻断计划生成
            logger.warning(f"再入场判定失败：{e}")

    # ── 账户层 ──
    cost_basis = sum(cost_map[c] * shares_map[c] for c in held_codes)
    unreal = (sum(mv.values()) - cost_basis) / cost_basis if cost_basis > 0 else 0.0

    # 账户级风险提示：执行对账（上一日计划的清仓是否执行）/ 行业与单票集中度 / 仓位 vs 目标
    hints: list[str] = []
    try:
        prev_plan = loop.repo.read_sql(
            "SELECT code, name, reason FROM action_plan "
            "WHERE plan_date=(SELECT MAX(plan_date) FROM action_plan WHERE plan_date<:d) "
            "AND action='sell'", {"d": dt})
        stale = [str(r["name"] or r["code"]) for _, r in prev_plan.iterrows()
                 if r["code"] in held_codes]
        if stale:
            hints.append(f"上一交易日计划清仓未执行：{'、'.join(stale)}（纪律高于观点，优先处理）")
    except Exception:  # noqa: BLE001 - 首日无历史计划等情况不阻断
        pass
    try:
        ind_map = loop.industry_map()
        ind_w: dict[str, float] = {}
        for c in held_codes:
            ind_w[ind_map.get(c) or "未知"] = ind_w.get(ind_map.get(c) or "未知", 0.0) + cur_w.get(c, 0.0)
        for ind, w in sorted(ind_w.items(), key=lambda kv: -kv[1]):
            if ind != "未知" and w > 0.35:
                hints.append(f"行业集中度：{ind} 合计 {w:.0%} 超 35% 上限，回避同涨同跌")
        heavy = [(names.get(c, c), w) for c, w in cur_w.items() if w > 0.20]
        for nm, w in sorted(heavy, key=lambda kv: -kv[1]):
            hints.append(f"单票集中度：{nm} {w:.0%} 超 20% 上限，建议分批降至上限内")
        # E7 拥挤度（HHI+Top-3 聚合口径）：持仓行业按权重 + 投顾 long 信号池按主题，
        # 补单行业阈值盲区（多个中等行业同题材共振 / 投顾扎堆少数题材同涨同跌）。
        from invest_model.portfolio.crowding import crowding_hints
        adv_cat: list[str] = []
        try:
            ar = loop.adv_repo.get_active_reco(dt)
            if not ar.empty and "catalyst" in ar.columns:
                adv_cat = [str(x) for x in ar.loc[ar["direction"] == "long", "catalyst"]]
        except Exception:  # noqa: BLE001
            adv_cat = []
        hints.extend(crowding_hints(cur_w, ind_map, adv_cat))
    except Exception:  # noqa: BLE001
        pass
    invested = sum(mv.values()) / equity
    if invested - gross > 0.10:
        hints.append(f"实际仓位 {invested:.0%} 高于目标 {gross:.0%}，无现金缓冲，补足前不开新仓")

    # 排雷影子提示（提案 P7）：持仓/目标命中 ≥2 面红旗 → 建议深挖财报（不自动动仓）
    try:
        from invest_model.universe.quality_screen import latest_flags
        qf = latest_flags(engine, dt, list(set(held_codes) | set(targets)))
        for c, (nfl, fls) in sorted(qf.items(), key=lambda kv: -kv[1][0]):
            head = fls[0].split("（")[0] if fls else ""
            hints.append(
                f"排雷影子: {names.get(c, c)} 命中 {nfl} 面红旗（{head} 等）——"
                f"触发深挖非确认造假，建议核对财报（影子观察，不自动动仓）")
    except Exception:  # noqa: BLE001 — 影子提示失败不阻断计划
        pass

    # 戴维斯双杀预警（财报#1 快报时效层）：近 7 日新披露快报/预告显示增速失速
    try:
        hints.extend(_express_alerts(loop, dt, list(set(held_codes) | set(targets)), names))
    except Exception:  # noqa: BLE001
        pass

    # 顶部特征追加提示（P16 自动减半已在动作层执行）：对本周期已减半、但顶部特征仍在的持仓，
    # 提示可考虑进一步兑现（避免重复自动减仓，改由人工判断二次动作）。新触发已成 trim 动作行，不在此列。
    try:
        from invest_model.signals.top_feature import top_feature_now
        start_lb = f"{int(dt[:4]) - 2}{dt[4:]}"          # 约 2 年回看，够 250 日波动分位
        still_top: list[str] = []
        for c in held_codes:
            if c not in top_trimmed:                      # 仅看"已减半"的；新触发走动作行
                continue
            close = _close_hist(loop, c, start_lb, dt)
            if close.empty:
                continue
            vser = loop.repo.read_sql(
                "SELECT trade_date, volume FROM stock_daily "
                "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
                {"c": c, "s": start_lb, "d": dt})
            vol = (pd.to_numeric(vser.set_index("trade_date")["volume"], errors="coerce")
                   if not vser.empty else pd.Series(dtype=float))
            vol.index = vol.index.astype(str)
            if top_feature_now(close, vol.reindex(close.index), cost_map.get(c, 0.0),
                               entry_map.get(c) or None):
                still_top.append(names.get(c, c))
        if still_top:
            hints.append(
                f"顶部特征仍在（本周期已自动减半）：{'、'.join(still_top)}——"
                f"顶部风险未消，可人工考虑进一步兑现（P16，见 model_change_proposals）")
    except Exception:  # noqa: BLE001 — 顶部提示失败不阻断计划
        pass

    # ── 套利统一资金账本（一体两面）：单一资金池按 A/B/α 分配、强制零杠杆 ──
    # ARB_ENABLED=0（默认观察态）：不发 arb 行、不缩放引擎 B → 计划与今天逐字一致。
    arb_rows: list[dict] = []
    ledger_extra: dict = {}
    try:
        from invest_model.arb.config import ArbConfig
        from invest_model.arb.ledger import build_arb_plan
        acfg = ArbConfig.from_env()
        fear_score = None
        try:
            from invest_model.signals.fear import fear_gauge
            fear_score = fear_gauge(engine, dt).get("score")
        except Exception:  # noqa: BLE001
            fear_score = None
        lg = build_arb_plan(engine, dt, acfg, equity, gross, fear_score, held_codes)
        ledger_extra = lg["account_extra"]
        if lg.get("viol_hint"):
            hints.append(lg["viol_hint"])
        # 引擎 B 行统一标 sleeve；启用态按 offense_scale 缩进 offense 预算
        oscale = lg.get("offense_scale", 1.0)
        for r in rows:
            r.setdefault("sleeve", "offense_B")
            if acfg.enabled and oscale != 1.0 and r.get("action") in ("buy", "add", "hold"):
                r["tgt_weight"] = round(float(r.get("tgt_weight") or 0.0) * oscale, 4)
                if r.get("shares_delta"):
                    r["shares_delta"] = _round_lot(float(r["shares_delta"]) * oscale)
        for r in watch_rows:
            r.setdefault("sleeve", "offense_B")
        arb_rows = lg.get("arb_rows", [])
        # sleeve_target 落库（观察态也写，看板可见）
        if persist and lg.get("sleeve_rows"):
            try:
                from invest_model.repositories.arb_repo import LedgerRepo
                sr = pd.DataFrame(lg["sleeve_rows"])
                LedgerRepo(engine).save(sr)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"sleeve_target 落库失败（不阻断）：{e}")
    except Exception as e:  # noqa: BLE001 - 套利账本失败绝不阻断主计划
        logger.warning(f"套利资金账本构建失败（跳过，回退纯引擎B）：{e}")

    account = {
        "plan_date": dt, "equity": round(equity, 2),
        "invested_pct": round(sum(mv.values()) / equity, 4),
        "cash_pct": round(max(0.0, cash) / equity, 4),
        "n_holdings": len(held_codes),
        "unrealized_pnl_pct": round(unreal, 4),
        "gross_target": round(gross, 4),
        # 注：实盘缺账户峰值，用「持仓整体浮亏」近似账户级回撤(rc.account_dd_stop)风控提示
        "risk_off": bool(rc.enabled and rc.account_dd_stop and unreal <= -rc.account_dd_stop),
        "model_ic_mean": m_ic_mean, "model_ic_ir": m_ic_ir, "model_hit": m_hit,
        "model_conf_label": _conf_label(model_trust, m_ic_ir),
        "risk_hints": " | ".join(hints) if hints else None,
        **ledger_extra,
    }

    rows = rows + arb_rows + watch_rows
    plan = ActionPlan(plan_date=dt, rows=rows, account=account)
    if persist and rows:
        cols = ["plan_date", "code", "name", "action", "cur_weight", "tgt_weight",
                "shares_delta", "reason", "stop_price", "ref_price", "grade",
                "trigger_hint", "model_rank", "model_view", "sleeve"]
        df = pd.DataFrame(rows)
        if "sleeve" not in df.columns:
            df["sleeve"] = "offense_B"
        df["sleeve"] = df["sleeve"].fillna("offense_B")
        df["trigger_hint"] = df["trigger"]  # trigger 为 MySQL 保留字，落库改名
        loop.repo.upsert("action_plan", df[cols], ["plan_date", "code"])
        try:
            acct = {**account, "risk_off": int(account["risk_off"])}
            loop.repo.upsert("action_plan_account", pd.DataFrame([acct]), ["plan_date"])
        except Exception as e:  # noqa: BLE001 - 账户元数据落库失败不阻断计划生成
            print(f"WARN action_plan_account 落库失败：{e}")
    return plan


def _entry_reason(grade, meta: dict) -> str:
    src = (meta or {}).get("source")
    if src == "advisor" and grade:
        return f"投顾{grade}级推荐"
    return "量化补充" if src == "quant" else "目标加配"


def _return_sources(expo: pd.DataFrame) -> dict[str, str]:
    """收益三来源定位（买前定位赚哪种钱——业绩成长/估值修复/分红）。

    出处：价投批判篇（收益三来源框架）。由因子暴露（截面 zscore）推断：
    成长=盈利增速高分位；修复=便宜（EP/BP 高）且增速不高；红利=股息率高分位。
    影子候选 dividend_yield 无数据时红利档自然缺省。
    """
    if expo is None or expo.empty:
        return {}
    z = lambda col: pd.to_numeric(expo.get(col, pd.Series(np.nan, index=expo.index)),  # noqa: E731
                                  errors="coerce")
    growth = z("profit_yoy")
    cheap = pd.concat([z("ep"), z("bp")], axis=1).max(axis=1)
    div = z("dividend_yield")
    out: dict[str, str] = {}
    for c in expo.index:
        g, ch, dv = growth.get(c), cheap.get(c), div.get(c)
        if pd.notna(dv) and dv >= 1.0 and (pd.isna(g) or g < 0.5):
            out[str(c)] = "红利"
        elif pd.notna(g) and g >= 0.5:
            out[str(c)] = "成长"
        elif pd.notna(ch) and ch >= 0.5:
            out[str(c)] = "修复"
    return out


def _express_alerts(loop: ClosedLoop, dt: str, codes: list[str],
                    names: dict[str, str], back_days: int = 7,
                    drop_pp: float = 20.0) -> list[str]:
    """戴维斯双杀预警：近 N 日新披露的业绩快报/预告显示净利增速转负或骤降。

    出处：财报#1（快报是最后逃生窗口；增速失速→成长股 PE 重估跌 78%，
    验证 growth-deceleration-davis-killer）。口径：快报/预告的累计净利同比 vs
    最近定期报告的累计同比，转负或降幅 > drop_pp 即预警。只提示不动仓。
    """
    if not codes or not loop.repo.table_exists("fina_express"):
        return []
    back = (pd.to_datetime(dt) - pd.Timedelta(days=back_days)).strftime("%Y%m%d")
    ex = loop.repo.read_sql(
        "SELECT code, ann_date, report_date, kind, profit_yoy FROM fina_express "
        "WHERE ann_date>:b AND ann_date<=:d", {"b": back, "d": dt})
    if ex.empty:
        return []
    ex = ex[ex["code"].isin(set(codes))]
    if ex.empty:
        return []
    ex["profit_yoy"] = pd.to_numeric(ex["profit_yoy"], errors="coerce")
    ex = ex.dropna(subset=["profit_yoy"]).sort_values("ann_date").groupby("code").tail(1)
    # 对照基准：该票最近一期定期报告的累计净利同比
    stale = (pd.to_datetime(dt) - pd.Timedelta(days=540)).strftime("%Y%m%d")
    fi = loop.repo.read_sql(
        "SELECT code, ann_date, report_date, profit_yoy FROM stock_fina_indicator "
        "WHERE ann_date<=:d AND ann_date>=:lo", {"d": dt, "lo": stale})
    base: dict[str, float] = {}
    if not fi.empty:
        fi = fi[fi["code"].isin(set(ex["code"]))]
        fi = fi.sort_values(["code", "ann_date", "report_date"]).groupby("code").tail(1)
        base = {str(r["code"]): float(pd.to_numeric(r["profit_yoy"], errors="coerce"))
                for _, r in fi.iterrows() if pd.notna(pd.to_numeric(r["profit_yoy"], errors="coerce"))}
    kind_cn = {"express": "快报", "forecast": "预告"}
    alerts: list[str] = []
    for _, r in ex.iterrows():
        c, now = str(r["code"]), float(r["profit_yoy"])
        prev = base.get(c)
        slump = now < 0 or (prev is not None and prev - now > drop_pp)
        if not slump:
            continue
        prev_s = f"（上期 {prev:+.0f}%）" if prev is not None else ""
        alerts.append(
            f"戴维斯双杀预警: {names.get(c, c)} {kind_cn.get(str(r['kind']), '快报')}净利同比 "
            f"{now:+.0f}%{prev_s}——增速{'转负' if now < 0 else '骤降'}，成长股第一时间重估"
            f"（快报是最后逃生窗口，仅提示不自动动仓）")
    return alerts


_ACTION_CN = {"buy": "买入", "add": "加仓", "trim": "减仓", "sell": "清仓",
              "hold": "持有", "watch": "观察"}


def _f(x):
    """安全转 float；非数/NaN 返回 None。"""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if np.isfinite(v) else None


def _model_trust(ic_ir) -> float:
    """模型层置信度(0..1)：由交叉验证 IC_IR 映射。IC_IR≥0.6≈满信任，≤0 视为失效。"""
    v = _f(ic_ir)
    if v is None:
        return 0.0
    return float(min(1.0, max(0.0, v / 0.6)))


def _conf_label(trust: float, ic_ir) -> str:
    v = _f(ic_ir)
    if v is None:
        return "无（模型未就绪）"
    if v <= 0:
        return "失效（IC≤0，勿依赖）"
    return "高" if trust >= 0.66 else ("中" if trust >= 0.33 else "低")


def _model_verdict(mr: float) -> str:
    if mr >= 0.85:
        return "看好"
    if mr >= 0.65:
        return "偏多"
    if mr >= 0.45:
        return "中性"
    if mr >= 0.25:
        return "偏弱"
    return "看淡"


def _model_view(mr, trust: float, top_factors: str | None = None) -> str:
    """单票模型研判：方向(看好/中性/看淡) + 全市场分位 + 置信★(决断度×模型信任)
    + 因子归因（top3 贡献因子，如 ep↑ mom60↑——决策可解释，出处见 rulebook）。"""
    v = _f(mr)
    if v is None:
        return "—"                                # 无模型覆盖（如 ETF）
    top = (1.0 - v) * 100.0
    conviction = abs(v - 0.5) * 2.0               # 分位越极端越决断(0..1)
    c = conviction * trust
    stars = "★★★" if c >= 0.55 else ("★★" if c >= 0.28 else "★")
    base = f"{_model_verdict(v)} 前{top:.0f}% {stars}"
    attr = _fmt_attr(top_factors)
    return f"{base} · {attr}" if attr else base


def _fmt_attr(top_factors) -> str:
    """"ep+0.82|mom_60+1.15" → "ep↑mom_60↑"（因子名+推拉方向，紧凑展示）。"""
    if not top_factors or not isinstance(top_factors, str):
        return ""
    parts = []
    for seg in top_factors.split("|")[:3]:
        seg = seg.strip()
        i = max(seg.rfind("+"), seg.rfind("-"))
        if i <= 0:
            continue
        parts.append(f"{seg[:i]}{'↑' if seg[i] == '+' else '↓'}")
    return "".join(parts)


def _table(lines: list[str], rows: list[dict]) -> None:
    lines.append("| 代码 | 名称 | 动作 | 现权重→目标 | 约股数 | 买点/挂单价 | 理由 | 止损价 | 现价 | 分级 | 模型研判 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        sd = int(r["shares_delta"])
        sd_s = f"+{sd}" if sd > 0 else (str(sd) if sd < 0 else "—")
        lines.append(
            f"| {r['code']} | {r['name']} | {_ACTION_CN.get(r['action'], r['action'])} | "
            f"{r['cur_weight']:.1%}→{r['tgt_weight']:.1%} | {sd_s} | {r.get('trigger', '—')} | {r['reason']} | "
            f"{r['stop_price'] if r['stop_price'] is not None else '—'} | "
            f"{r['ref_price'] if r['ref_price'] is not None else '—'} | {r['grade'] or '—'} | "
            f"{r.get('model_view', '—')} |")


def render_markdown(plan: ActionPlan) -> str:
    a = plan.account
    lines = [f"# 操作计划 — {plan.plan_date}", ""]
    lines.append(
        f"- 总权益: {a.get('equity')} | 持仓占比: {a.get('invested_pct', 0):.0%} | "
        f"现金占比: {a.get('cash_pct', 0):.0%} | 目标仓位: {a.get('gross_target', 0):.0%}")
    lines.append(
        f"- 持仓数: {a.get('n_holdings')} | 整体浮盈亏: {a.get('unrealized_pnl_pct', 0):+.1%} | "
        f"账户风控(risk_off): {'⚠️ 触发，建议降仓' if a.get('risk_off') else '正常'}")
    mir = a.get("model_ic_ir")
    if mir is not None:
        lines.append(
            f"- 🔬 模型层置信度: **{a.get('model_conf_label')}**"
            f"（rank-IC {(a.get('model_ic_mean') or 0):.3f} · IC_IR {mir:.2f} · 胜率 {(a.get('model_hit') or 0):.0%}）"
            "— 衡量因子对未来约1月收益的区分力，决定下方「模型研判」的整体可信度")
    lines.append(
        "- 「模型研判」= 方向(看好/偏多/中性/偏弱/看淡) + 全市场分位 + 置信★"
        "（★越多＝模型对该票越决断且历史越准；ETF/无覆盖记「—」）"
        " + 因子归因（如 ep↑mom_60↑ = 该票排名主要由哪些因子推动）。"
        "**标的由投顾定，模型只做参谋+时机+风控，不选股。**")
    lines.append(
        "- 可解释性：理由列的「定位:成长/修复/红利」= 买前定位赚哪种钱；"
        "每条规则的参数、依据与知识库出处见 `docs/rulebook.md`（决策可溯源）。")

    # 套利 sleeve 行（defense_A/alpha）单列，不混入引擎 B 的买入/持仓/观察
    arb = [r for r in plan.rows if r.get("sleeve") in ("defense_A", "alpha")]
    core = [r for r in plan.rows if r.get("sleeve") not in ("defense_A", "alpha")]
    held = [r for r in core if r["cur_weight"] > 1e-6]
    buys = [r for r in core if r["action"] in ("buy", "add") and r["cur_weight"] <= 1e-6]
    watch = [r for r in core if r["action"] == "watch"]
    held.sort(key=lambda r: ({"sell": 0, "trim": 1, "hold": 2}.get(r["action"], 3), -r["cur_weight"]))
    buys.sort(key=lambda r: -r["tgt_weight"])
    watch.sort(key=lambda r: (r["grade"] or "Z", r["code"]))

    lines += ["", f"## 一、建议买入（买点触发，共 {len(buys)} 只）"]
    if buys:
        _table(lines, buys)
    else:
        lines.append("（今日无买点触发——观察池均在等待回踩/突破信号）")
    lines += ["", f"## 二、当前持仓·风控动作（{len(held)} 只）"]
    _table(lines, held) if held else lines.append("（无持仓）")
    lines += ["", f"## 三、观察池·等买点（{len(watch)} 只，方向确认、待时机）"]
    if watch:
        _table(lines, watch)
    else:
        lines.append("（观察池为空）")
    _render_ledger(lines, a, arb)
    return "\n".join(lines)


def _render_ledger(lines: list[str], a: dict, arb_rows: list[dict]) -> None:
    """套利统一资金账本段（一体两面）。account 无 sleeve 字段则跳过（向后兼容）。"""
    if a.get("offense_pct") is None and not arb_rows:
        return
    enabled = bool(arb_rows) or (a.get("ledger_ok") is not None)
    tag = "" if arb_rows else "（观察态·未动用资金）"
    lines += ["", f"## 四、套利/守恒 sleeve 账本{tag}"]
    dpct = a.get("defense_pct"); opct = a.get("offense_pct"); apct = a.get("alpha_pct")
    if opct is not None:
        ok = "✅零杠杆" if a.get("ledger_ok") else "⚠️超100%已收缩"
        fear = a.get("fear_score")
        fear_s = f" | 恐慌指数 {fear:.0f}{'（恐慌弹药↑进攻/α）' if fear and fear >= 75 else ''}" if fear is not None else ""
        lines.append(
            f"- 资金池分配：防守A **{(dpct or 0):.0%}** / 进攻B **{(opct or 0):.0%}** / "
            f"盲区α **{(apct or 0):.0%}** / 现金 **{max(0.0, 1-(dpct or 0)-(opct or 0)-(apct or 0)):.0%}** "
            f"（{ok}，Σ={a.get('sleeve_gross', 0):.0%}）{fear_s}")
        ce = a.get("carry_expected")
        if ce:
            lines.append(f"- 防守底盘预期 carry（加权年化）：约 {ce:.2%}")
        lines.append("- 红线：全程自有资金·零杠杆；α 小仓位对赔率、单笔亏得起；跟水不跟价（逻辑止损）。")
    if arb_rows:
        lines += ["", "| sleeve | 标的 | 动作 | 目标权重 | 参考价 | 逻辑 |",
                  "|---|---|---|---:|---:|---|"]
        for r in sorted(arb_rows, key=lambda x: (x.get("sleeve", ""), -float(x.get("tgt_weight") or 0))):
            lines.append(
                f"| {r.get('sleeve')} | {r.get('name')}({r.get('code')}) | {r.get('action')} "
                f"| {float(r.get('tgt_weight') or 0):.1%} | {r.get('ref_price', '—')} | {r.get('reason', '')} |")
    else:
        lines.append("- （启用 `ARB_ENABLED=1` 后此处列出逆回购/红利/可转债/盲区α 的具体挂单。）")
