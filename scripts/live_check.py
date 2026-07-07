"""盘中实时盯盘：用 rt_k 现价 + 库里均线/成本，给出持仓风控预警 + 观察池买点价位预警。

盘后请用 build_action_plan.py（落库 EOD 数据出完整三段式计划）；本工具用于盘中速查。

示例：
  python scripts/live_check.py --db sqlite:///./data/real.db --advisor-led --hard-stop 0.08
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.advisor_repo import AdvisorRepo  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.repositories.holding_repo import HoldingRepo  # noqa: E402
from invest_model.signals.realtime import get_realtime, get_realtime_etf  # noqa: E402


def _levels(repo: BaseRepository, codes: list[str], dt: str) -> dict[str, dict]:
    """从库里 EOD 收盘算每只的 MA20 / MA60斜率 / 20日高。"""
    if not codes:
        return {}
    start = (pd.to_datetime(dt) - pd.Timedelta(days=160)).strftime("%Y%m%d")
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    params.update(s=start, d=dt)
    df = repo.read_sql(
        f"SELECT code, trade_date, close FROM stock_daily "
        f"WHERE trade_date>=:s AND trade_date<=:d AND code IN ({ph})", params)
    out: dict[str, dict] = {}
    if df.empty:
        return out
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    for code, g in df.sort_values("trade_date").groupby("code"):
        cl = g["close"].dropna()
        if len(cl) < 20:
            continue
        ma20 = float(cl.tail(20).mean())
        ma5 = float(cl.tail(5).mean()) if len(cl) >= 5 else float("nan")
        ma10 = float(cl.tail(10).mean()) if len(cl) >= 10 else float("nan")
        ma60 = float(cl.tail(60).mean()) if len(cl) >= 60 else float("nan")
        ma60_prev = float(cl.tail(65).head(60).mean()) if len(cl) >= 65 else ma60
        hi20 = float(cl.iloc[-21:-1].max())   # 前 20 日高（不含今日）=突破位
        out[code] = {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
                     "ma60_up": ma60 >= ma60_prev, "hi20": hi20,
                     # 前一 EOD 收盘是否已在 MA20 下方：P10「新鲜破位」判定用——
                     # 前日已破位则今日不是首破，不重复提示减半/清仓
                     "prev_below": bool(float(cl.iloc[-1]) < ma20)}
    return out


def _entry_peaks(repo: BaseRepository, holds: pd.DataFrame, dt: str) -> dict[str, float]:
    """每只持仓自建仓日以来的最高收盘（盈利保护「自峰值回撤」的峰值基准）。

    无 entry_date 的持仓取不到峰值 → 不参与盈利保护（宁缺勿误报）。
    """
    out: dict[str, float] = {}
    if holds is None or holds.empty or "entry_date" not in holds.columns:
        return out
    for _, h in holds.iterrows():
        ed = str(h.get("entry_date") or "").strip()
        if not ed:
            continue
        try:
            df = repo.read_sql(
                "SELECT MAX(close) mx FROM stock_daily "
                "WHERE code=:c AND trade_date>=:e AND trade_date<=:d",
                {"c": h["code"], "e": ed, "d": dt})
            v = pd.to_numeric(df["mx"].iloc[0], errors="coerce")
            if pd.notna(v) and float(v) > 0:
                out[h["code"]] = float(v)
        except Exception:  # noqa: BLE001
            continue
    return out


def _fund_levels(pro, code: str, dt: str) -> dict | None:
    """ETF/基金前复权均线基准：{ma20, ma60, ma60_up, hi20, last, adj_ok}。

    用 fund_daily + fund_adj 做前复权（把份额拆分/折算的价格台阶抹平，否则均线会被污染），
    复权后最新值 == 原始最新收盘，故可直接与 rt_k 原始现价比较。取数失败/样本不足返回 None。
    """
    start = (pd.to_datetime(dt) - pd.Timedelta(days=130)).strftime("%Y%m%d")

    def _q(ep, **kw):
        for _ in range(6):
            try:
                d = pro.query(ep, **kw)
            except Exception:  # noqa: BLE001
                d = None
            if d is not None and len(d):
                return d
            time.sleep(6)
        return None

    df = _q("fund_daily", ts_code=code, start_date=start, end_date=dt)
    if df is None or df.empty:
        return None
    df = df.sort_values("trade_date")
    cl_raw = pd.to_numeric(df["close"], errors="coerce")
    adj = _q("fund_adj", ts_code=code, start_date=start, end_date=dt)
    if adj is None or not len(adj):
        cl = cl_raw.dropna()          # 复权因子取不到：退回原始价（趋势存疑）
        adj_ok = False
    else:
        adj = adj.sort_values("trade_date")
        fac = dict(zip(adj["trade_date"], pd.to_numeric(adj["adj_factor"], errors="coerce")))
        last_fac = pd.to_numeric(adj["adj_factor"], errors="coerce").iloc[-1]
        f = df["trade_date"].map(fac).astype(float).ffill().fillna(1.0)
        cl = (cl_raw * f / last_fac).dropna()
        adj_ok = True
    if len(cl) < 20:
        return None
    ma60 = float(cl.tail(60).mean()) if len(cl) >= 60 else float("nan")
    ma60_prev = float(cl.tail(65).head(60).mean()) if len(cl) >= 65 else ma60
    return {"ma20": float(cl.tail(20).mean()), "ma60": ma60,
            "ma60_up": ma60 >= ma60_prev,
            "hi20": float(cl.iloc[-21:-1].max()) if len(cl) >= 21 else float("nan"),
            "last": float(cl.iloc[-1]), "adj_ok": adj_ok,
            "prev_below": bool(float(cl.iloc[-1]) < float(cl.tail(20).mean()))}


def _etf_watch(dt: str) -> list[tuple]:
    """ETF 观察清单（无 rt_k 实时源）：用 fund_daily EOD 算趋势/回踩位/突破位。

    返回 [(代码, 备注, 最新收盘, MA20, MA60趋势, 突破位, 状态)]。读 config/watch_etf.txt。
    """
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base, "config", "watch_etf.txt")
    if not os.path.exists(path):
        return []
    items = []
    for ln in open(path, encoding="utf-8"):
        head = ln.split("#")[0].strip()
        if not head:
            continue
        toks = head.split()
        code = toks[0]
        grade = toks[1] if len(toks) > 1 else ""        # 可选级别列（A/B）
        note = ln.split("#", 1)[1].strip() if "#" in ln else ""
        if grade:
            note = f"[{grade}级] {note}"
        items.append((code, note))
    if not items:
        return []
    from invest_model.sources.tushare_client import TushareClient
    pro = TushareClient().pro
    rows = []
    for code, note in items:
        lv = _fund_levels(pro, code, dt)
        if lv is None:
            rows.append((code, note, float("nan"), float("nan"), "?", float("nan"), "取数失败"))
            continue
        last, ma20, hi20 = lv["last"], lv["ma20"], lv["hi20"]
        if not lv["adj_ok"]:
            rows.append((code, note, last, ma20, "?", hi20, "复权因子取数失败(趋势存疑,勿据此操作)"))
            continue
        ma60_up = lv["ma60_up"]
        dev = (last / ma20 - 1) if ma20 == ma20 else 0.0
        if not (last >= lv["ma60"] and ma60_up):
            st = "左侧（MA60未走平/上行，不买）"
        elif abs(dev) <= 0.03:
            st = "⚠️ 到回踩位，企稳放量则买点"
        elif last >= hi20:
            st = "⚠️ 突破平台，放量则买点"
        elif dev > 0.06:
            st = f"偏离MA20 {dev:+.0%}，勿追，等回踩"
        else:
            st = "上方运行，等回踩MA20"
        rows.append((code, note, last, ma20, ("MA60↑" if ma60_up else "MA60↓"), hi20, st))
    return rows


def _now_cst() -> datetime:
    """当前北京时间（CST=UTC+8）。"""
    return datetime.now(timezone.utc) + timedelta(hours=8)


def _trading(now: datetime) -> bool:
    """是否 A 股交易时段（工作日 09:30-11:30 / 13:00-15:00）。"""
    hm = now.hour * 60 + now.minute
    return (now.weekday() < 5) and (570 <= hm <= 690 or 780 <= hm <= 900)


def _is_trading_day(day: str) -> bool | None:
    """今天是否 A 股交易日（trade_cal，含节假日）。查不到/出错返回 None（调用方按"继续"处理）。"""
    try:
        from invest_model.sources.tushare_client import TushareClient
        df = TushareClient().pro.query("trade_cal", exchange="SSE",
                                       start_date=day, end_date=day)
    except Exception:  # noqa: BLE001
        return None
    if df is None or df.empty or "is_open" not in df.columns:
        return None
    return int(df.iloc[0]["is_open"]) == 1


def _latest_snapshot_df(repo: BaseRepository | None = None) -> pd.DataFrame:
    """最新持仓快照：优先仓库 config/holding_snapshot_*.csv（Actions checkout 里最新），
    没有 CSV 时回退查库内 holding_snapshot 表的最新 snapshot_date——FaaS 代码包
    刻意不带快照 CSV（打包即冻结、会过期），永远走 DB 拿最新快照。
    """
    files = sorted(glob.glob(os.path.join(_ROOT, "config", "holding_snapshot_*.csv")))
    if files:
        return pd.read_csv(files[-1], dtype=str)
    if repo is not None:
        try:
            if repo.table_exists("holding_snapshot"):
                return repo.read_sql(
                    "SELECT code, name, asset_type, shares, cost_price, market_value "
                    "FROM holding_snapshot WHERE snapshot_date="
                    "(SELECT MAX(snapshot_date) FROM holding_snapshot)")
        except Exception:  # noqa: BLE001
            pass
    return pd.DataFrame()


def _load_etf_holdings(repo: BaseRepository | None = None) -> list[dict]:
    """从最新持仓快照读 ETF 持仓（code/name/cost）。

    current_holding 表按设计只存 stock（rt_k 也只支持股票），故 ETF 持仓从快照读
    （CSV 或 DB，见 _latest_snapshot_df）。
    """
    df = _latest_snapshot_df(repo)
    if df.empty or "asset_type" not in df.columns:
        return []
    etf = df[df["asset_type"].astype(str).str.lower() == "etf"]
    out = []
    for _, r in etf.iterrows():
        cost = pd.to_numeric(r.get("cost_price"), errors="coerce")
        sh = pd.to_numeric(r.get("shares"), errors="coerce")
        out.append({"code": r["code"], "name": r.get("name") or r["code"],
                    "cost": float(cost) if cost == cost else 0.0,
                    "shares": float(sh) if sh == sh else 0.0})
    return out


def _account_cash(repo: BaseRepository | None = None) -> float:
    """最新快照的可用现金（asset_type=cash 行的 market_value），用于买入类挂单定量。"""
    return _snapshot_sum("cash", repo)


def _account_equity(repo: BaseRepository | None = None) -> float:
    """账户总权益 = 快照所有行 market_value 之和（含持仓市值 + 现金），用于算目标占比。"""
    return _snapshot_sum(None, repo)


def _snapshot_sum(asset_type: str | None, repo: BaseRepository | None = None) -> float:
    """汇总最新持仓快照的 market_value：asset_type 指定则只汇总该类，None 汇总全部。"""
    df = _latest_snapshot_df(repo)
    if df.empty or "market_value" not in df.columns:
        return 0.0
    if asset_type is not None:
        if "asset_type" not in df.columns:
            return 0.0
        df = df[df["asset_type"].astype(str).str.lower() == asset_type]
    return float(pd.to_numeric(df["market_value"], errors="coerce").sum() or 0.0)


def _buy_ticket(level: float, cash: float, equity: float, weight: float) -> str:
    """买入挂单：挂单价 + 按目标占比定量（股数/金额/占比），并标注资金是否够。

    数量按「目标权重 × 总权益 / 挂单价」取整手——不管现金够不够都给出该买多少，
    便于「先卖出腾资金 → 再按此挂买」；末尾注明现金够或还需腾多少。
    """
    if not level or level <= 0:
        return "买点未知"
    target_amt = weight * equity
    n = int(target_amt // (level * 100)) * 100     # A 股 100 股/手，向下取整手
    if n <= 0:
        n = 100                                    # 目标占比买不起1手 → 给最小1手，标出实际占比
    amt = n * level
    wt = (amt / equity) if equity else 0.0
    over = "⚠1手占比偏高" if wt > weight * 1.5 else ""
    fund = "现金够" if cash >= amt else f"需先卖出腾≈{amt - cash:.0f}"
    return f"挂买 限价{level:.2f} {n}股≈{amt:.0f}(占{wt:.1%}){over}; {fund}"


def _fetch_rt(ctx: dict) -> dict:
    """取实时价：股票+观察池走 rt_k，ETF 走 rt_etf_k（rt_k 不返回 ETF），隔离取数失败。"""
    rt = get_realtime(ctx["codes"])
    if ctx.get("etf_codes"):
        try:
            rt.update(get_realtime_etf(ctx["etf_codes"]))
        except Exception:  # noqa: BLE001
            pass
    return rt


def _build_context(args: argparse.Namespace) -> dict:
    """构建盘中不变的上下文：DB 基准日、持仓、观察池、均线基准（盘中复用，只需取一次）。"""
    engine = make_engine(args.db) if args.db else make_engine()
    repo = BaseRepository(engine)
    # 移动止盈白名单：这些票按"破MA20"管，不套用 -8% 硬止损（如业绩爆发的核心仓）
    tro = os.path.join(_ROOT, "config", "trailing_only.txt")
    trailing_only = set()
    if os.path.exists(tro):
        trailing_only = {ln.split("#")[0].strip() for ln in open(tro, encoding="utf-8")
                         if ln.split("#")[0].strip()}
    dt = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]
    holds = HoldingRepo(engine).get_all()
    reco = AdvisorRepo(engine).get_active_reco(dt)
    exit_codes = AdvisorRepo(engine).get_exit_codes(dt)
    held = list(holds["code"]) if not holds.empty else []
    if reco.empty:
        watch = []
    else:
        sel = reco[(reco["direction"] == "long") & (reco["grade"].isin({"A", "B"}))]
        watch = list(sel["code"])
    watch = [c for c in watch if c not in held and c not in exit_codes]
    levels = _levels(repo, held + watch, dt)
    peaks = _entry_peaks(repo, holds, dt)     # 持有期峰值收盘（盈利保护基准）
    g_of = dict(zip(reco["code"], reco["grade"])) if not reco.empty else {}
    # ETF 持仓：成本读快照（CSV 或 DB），均线基准用 fund_daily 前复权（可转债不盯，用户上市即卖）
    etf_holds = _load_etf_holdings(repo)
    etf_levels: dict[str, dict] = {}
    if etf_holds:
        from invest_model.sources.tushare_client import TushareClient
        pro = TushareClient().pro
        for e in etf_holds:
            lv = _fund_levels(pro, e["code"], dt)
            if lv is not None:
                etf_levels[e["code"]] = lv
    return {"engine": engine, "repo": repo, "dt": dt, "holds": holds,
            "watch": watch, "levels": levels, "peaks": peaks, "g_of": g_of,
            "trailing_only": trailing_only, "codes": held + watch,
            "etf_holds": etf_holds, "etf_levels": etf_levels,
            "etf_codes": [e["code"] for e in etf_holds],
            "cash": _account_cash(repo), "equity": _account_equity(repo)}


def _scan(ctx: dict, rt: dict, args: argparse.Namespace) -> tuple[list, list, list, list, float]:
    """用实时价 rt 评估持仓风控（股票+ETF） + 观察池买点。

    返回 (股票持仓行, 观察行, ETF持仓行, 预警, 距触发线最小距离) 五元组；
    预警为 [(dedup_key, 展示行, 严重级)]，dedup_key 含状态标签——同一票同一状态
    只算一次，状态切换才算新预警。严重级："crit"=卖出类风控（立即推送），
    "batch"=买点/提示类（合并摘要推送）。最小距离用于自适应轮询频率。
    """
    holds = ctx["holds"]; levels = ctx["levels"]
    trailing_only = ctx["trailing_only"]; g_of = ctx["g_of"]
    cash = ctx.get("cash", 0.0); equity = ctx.get("equity", 0.0) or 0.0
    buy_w = getattr(args, "buy_weight", 0.05)
    hold_rows, watch_rows, etf_rows, alerts = [], [], [], []
    dists: list[float] = []   # 各票距最近触发线的相对距离（仅未触发的正距离）

    def _track(px: float, *lvls: float | None) -> None:
        for lv in lvls:
            if lv and lv > 0 and px > 0:
                d = abs(px / lv - 1.0)
                if d > 1e-6:
                    dists.append(d)
    for _, h in holds.iterrows():
        c = h["code"]; q = rt.get(c, {}); lv = levels.get(c, {})
        px = q.get("price"); pre = q.get("pre_close"); cost = float(h["cost_price"] or 0)
        sh = float(h.get("shares") or 0)
        if not px:
            continue
        chg = (px / pre - 1) if pre else 0
        pnl = (px / cost - 1) if cost else 0
        stop = cost * (1 - args.hard_stop); ma20 = lv.get("ma20")
        ex = c in trailing_only            # 移动止盈白名单：不套硬止损
        # 盈利保护基准：持有期峰值收盘（EOD）与盘中现价取高
        peak = max(ctx.get("peaks", {}).get(c, 0.0), px)
        pp_on = bool(cost) and args.pp_trigger and peak / cost - 1 >= args.pp_trigger
        if not ex and pnl <= -args.hard_stop:
            st, hit = "⚠️ 已触发硬止损，清仓", True
        elif ma20 and px < ma20 * 0.995:   # 0.5% 缓冲，避免贴线来回抖动
            # P10：未盈利新仓破MA20→减半(不清)、盈利仓仅「新鲜破位」(前日在线上)才清；
            # 前日已破位=缓冲期，不重复给卖单（首破当日已提示减半）。与盘后 evaluate_holding 对齐。
            if ex:
                st, hit = "破MA20移动止盈，确认清仓", True
            elif cost and pnl < 0:
                st, hit = (("MA20下方缓冲持有(前日已破位·首破日已减仓,硬止损兜底)", True)
                           if lv.get("prev_below") else
                           ("破MA20减半(未盈利新仓缓冲，盘后确认)", True))
            elif cost and lv.get("prev_below"):
                st, hit = "MA20下方转盈·非新鲜破位持有(站回MA20后再破位才清)", True
            else:
                st, hit = "破MA20，盘后确认清仓", True
        elif pp_on and not ex and 1 - px / peak >= args.pp_exit_dd:
            st, hit = f"⚠️ 盈利保护：自峰值回撤超{args.pp_exit_dd:.0%}，止盈离场", True
        elif pp_on and not ex and lv.get("ma10") and px < lv["ma10"]:
            st, hit = "⚠️ 盈利保护：破MA10（盈利后梯子），止盈离场", True
        elif pp_on and not ex and 1 - px / peak >= args.pp_trim_dd:
            st, hit = f"⚠️ 盈利保护：自峰值回撤超{args.pp_trim_dd:.0%}，减半锁盈", True
        elif pp_on and not ex and lv.get("ma5") and px < lv["ma5"]:
            st, hit = "⚠️ 盈利保护：破MA5（盈利后梯子），减半锁盈", True
        elif not ex and pnl <= -args.hard_stop + 0.02:
            st, hit = "逼近止损，盯紧", True
        else:
            st, hit = ("持有(MA20移动止盈)" if ex else "持有"), False
        hold_rows.append((q.get("name", c), px, chg, cost, pnl, stop, ma20, st))
        if not hit:
            _track(px, None if ex else stop, ma20 * 0.995 if ma20 else None,
                   peak * (1 - args.pp_trim_dd) if pp_on and not ex else None,
                   lv.get("ma5") if pp_on and not ex else None)
        if hit:
            # 挂单信号：清仓类给「卖 全部股数 限价≈现价，占比→0，回笼资金」；
            # 盈利保护减半/未盈利破MA20缓冲给「卖一半」；逼近/缓冲持有只提示不给单
            if "逼近" in st or "持有" in st:
                ticket = ""
            elif "减半" in st:
                half = int(sh // 200) * 100
                cw = (sh * px / equity) if equity else 0.0
                ticket = (f" → 卖 {half}股 限价≈{px:.2f} 减半"
                          f"(占{cw:.1%}→{cw / 2:.1%},回笼≈{half * px:.0f})" if half else "")
            else:
                cw = (sh * px / equity) if equity else 0.0
                ticket = (f" → 卖 {int(sh)}股 市价/限价≈{px:.2f} 清"
                          f"(占{cw:.1%}→0,回笼≈{sh * px:.0f})")
            sev = "batch" if ("逼近" in st or "持有" in st) else "crit"
            alerts.append((f"H:{c}:{st}",
                           f"🔴 持仓 {q.get('name', c)} {px:.2f}({chg:+.1%}) — {st}{ticket}", sev))
    for c in ctx["watch"]:
        q = rt.get(c, {}); lv = levels.get(c, {}); px = q.get("price")
        if not px or not lv:
            continue
        ma20, hi20 = lv.get("ma20"), lv.get("hi20")
        dev = (px / ma20 - 1) if ma20 else 0
        if not lv.get("ma60_up") or (lv.get("ma60") and px < lv["ma60"]):
            st, hit = "趋势未上（不看）", False
        elif ma20 and abs(dev) <= args.pullback_pct:
            st, hit = "⚠️ 到回踩位，企稳放量则买点", True
        elif px >= hi20 and dev <= 0.06:
            st, hit = "⚠️ 突破平台，放量则买点", True
        elif dev > 0.06:
            st, hit = f"偏离MA20 {dev:+.0%}，勿追高，等回踩", False
        else:
            st, hit = "上方运行，等回踩MA20", False
        watch_rows.append((q.get("name", c), g_of.get(c, ""), px, ma20, hi20, st))
        if not hit and "趋势未上" not in st:
            _track(px, ma20, hi20)
        if hit:
            # 挂单信号：回踩位挂 MA20、突破位挂 20日高，按目标占比定量（股数/金额/占比）
            lvl = ma20 if "回踩" in st else hi20
            ticket = " → " + _buy_ticket(lvl, cash, equity, buy_w)
            alerts.append((f"W:{c}:{st}",
                           f"🟢 观察 {q.get('name', c)}({g_of.get(c, '')}) {px:.2f} — {st}{ticket}",
                           "batch"))
    # ETF 持仓风控：无移动止盈白名单概念，一律按 硬止损 + 破MA20 管
    for e in ctx.get("etf_holds", []):
        c = e["code"]; q = rt.get(c, {}); lv = ctx["etf_levels"].get(c, {})
        px = q.get("price"); pre = q.get("pre_close"); cost = e["cost"]
        if not px:                          # rt_k 取不到 ETF 现价 → 本轮跳过，不误报
            continue
        chg = (px / pre - 1) if pre else 0
        pnl = (px / cost - 1) if cost else 0
        stop = cost * (1 - args.hard_stop); ma20 = lv.get("ma20")
        if pnl <= -args.hard_stop:
            st, hit = "⚠️ 已触发硬止损，考虑清仓", True
        elif ma20 and px < ma20 * 0.995:
            # P10：未盈利 ETF 破MA20→减半缓冲(仅首破)、盈利仅新鲜破位才减/清；硬止损兜底
            if cost and pnl < 0:
                st, hit = (("MA20下方缓冲持有(前日已破位·首破日已减仓,硬止损兜底)", True)
                           if lv.get("prev_below") else
                           ("破MA20减半(未盈利缓冲，盘后确认)", True))
            elif cost and lv.get("prev_below"):
                st, hit = "MA20下方转盈·非新鲜破位持有(站回MA20后再破位才清)", True
            else:
                st, hit = "破MA20，盘后确认减/清", True
        elif pnl <= -args.hard_stop + 0.02:
            st, hit = "逼近止损，盯紧", True
        else:
            st, hit = "持有", False
        etf_rows.append((e["name"], px, chg, cost, pnl, stop, ma20, st))
        if not hit:
            _track(px, stop, ma20 * 0.995 if ma20 else None)
        if hit:
            if "逼近" in st or "持有" in st:
                ticket = ""
            elif "减半" in st:
                half = int(e["shares"] // 200) * 100
                cw = (e["shares"] * px / equity) if equity else 0.0
                ticket = (f" → 卖 {half}份 市价/限价≈{px:.3f} 减半"
                          f"(占{cw:.1%}→{cw / 2:.1%},回笼≈{half * px:.0f})" if half else "")
            else:
                cw = (e["shares"] * px / equity) if equity else 0.0
                ticket = (f" → 卖 {int(e['shares'])}份 市价/限价≈{px:.3f} 减/清"
                          f"(占{cw:.1%}→0,回笼≈{e['shares'] * px:.0f})")
            sev = "batch" if ("逼近" in st or "持有" in st) else "crit"
            alerts.append((f"E:{c}:{st}",
                           f"🟠 ETF持仓 {e['name']} {px:.3f}({chg:+.1%}) — {st}{ticket}", sev))
    # 套利模块预警（水表反转 / α 证伪 / 逆回购利率窗口）——DB 驱动，best-effort
    try:
        alerts += _arb_alerts(ctx.get("engine"))
    except Exception:  # noqa: BLE001
        pass
    min_dist = min(dists) if dists else float("inf")
    return hold_rows, watch_rows, etf_rows, alerts, min_dist


def _arb_alerts(engine) -> list[tuple[str, str, str]]:
    """套利盯盘预警：WATER 水表反转(crit) / ALPHA α证伪(crit) / CARRY 逆回购窗口(batch)。"""
    if engine is None:
        return []
    out: list[tuple[str, str, str]] = []
    repo = BaseRepository(engine)
    # WATER：flow_score 最近两期 composite 由正转负 → 水表反转
    if repo.table_exists("flow_score"):
        fs = repo.read_sql(
            "SELECT trade_date, key, composite FROM flow_score "
            "WHERE trade_date>=(SELECT MIN(td) FROM (SELECT DISTINCT trade_date td FROM "
            "flow_score ORDER BY trade_date DESC LIMIT 2) x)")
        if not fs.empty and fs["trade_date"].nunique() >= 2:
            fs["composite"] = pd.to_numeric(fs["composite"], errors="coerce")
            piv = fs.pivot_table(index="key", columns="trade_date", values="composite")
            cols = sorted(piv.columns)
            prev, cur = cols[0], cols[-1]
            for k, r in piv.iterrows():
                if pd.notna(r[prev]) and pd.notna(r[cur]) and r[prev] > 0 and r[cur] < 0:
                    out.append((f"WATER:{k}:reversed",
                                f"💧 水表反转：{k} 资金流由 {r[prev]:+.0f}→{r[cur]:+.0f} 转负，"
                                "相关 sleeve 逻辑止损（跟水不跟价）", "crit"))
    # ALPHA：alpha_candidate 已标 falsified=1
    if repo.table_exists("alpha_candidate"):
        af = repo.read_sql(
            "SELECT code, theme FROM alpha_candidate WHERE falsified=1 "
            "AND as_of_date=(SELECT MAX(as_of_date) FROM alpha_candidate)")
        for _, r in af.iterrows():
            out.append((f"ALPHA:{r['code']}:falsified",
                        f"🔴 盲区α证伪：{r.get('theme') or ''}({r['code']}) 产业侧资金未兑现，"
                        "逻辑止损离场", "crit"))
    # CARRY：逆回购计息>=3天（季末/节前）或利率跳升 → 打新/理财资金回流窗口
    if repo.table_exists("reverse_repo_daily"):
        rr = repo.read_sql(
            "SELECT rate, interest_days FROM reverse_repo_daily WHERE code='204001.SH' "
            "AND trade_date=(SELECT MAX(trade_date) FROM reverse_repo_daily)")
        if not rr.empty:
            rate = float(pd.to_numeric(rr["rate"].iloc[0], errors="coerce") or 0)
            days = int(pd.to_numeric(rr["interest_days"].iloc[0], errors="coerce") or 1)
            if rate >= 3.0 or days >= 3:
                out.append((f"CARRY:204001:{int(rate*100)}:{days}",
                            f"💰 逆回购carry窗口：GC001 年化{rate:.2f}%·计息{days}天，"
                            "闲钱可停靠稳吃利差", "batch"))
    return out


def _persist_alerts(engine, now: datetime, items: list[tuple[str, str, str]]) -> None:
    """盯盘预警落库 watch_alert（仪表盘消息流数据源）；失败仅打印告警，绝不影响推送。

    items: [(dedup_key, 展示行, 严重级)]。同日同 dedup_key 幂等（表上有唯一约束，
    upsert 重跑不产生重复行）。与 --notify 无关：只要检测到新预警就落库。
    """
    if not items:
        return
    try:
        from invest_model.data import create_schema

        kind_of = {"H": "hold", "W": "watch", "E": "etf",
                   "CARRY": "carry", "WATER": "water", "ALPHA": "alpha"}
        rows = []
        for k, line, sev in items:
            head = k.split(":", 1)[0]
            kind = "selfcheck" if head == "ETFSELF" else kind_of.get(head, "other")
            parts = k.split(":")
            code = parts[1] if kind in ("hold", "watch", "etf", "carry", "water", "alpha") \
                and len(parts) > 1 else None
            rows.append({"alert_date": now.strftime("%Y%m%d"),
                         # DATETIME 用 ISO 字符串绑定，规避 pandas Timestamp 与 sqlite3 不兼容
                         "alert_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                         "code": code, "kind": kind, "severity": sev,
                         "message": line, "dedup_key": k[:160]})
        repo = BaseRepository(engine)
        df = pd.DataFrame(rows)
        try:
            repo.upsert("watch_alert", df, ["alert_date", "dedup_key"])
        except Exception:  # noqa: BLE001 - 表可能未建（老库），补建后重试一次
            create_schema(engine)
            repo.upsert("watch_alert", df, ["alert_date", "dedup_key"])
    except Exception as e:  # noqa: BLE001
        print(f"WARN watch_alert 落库失败：{e}")


_GH_API = "https://api.github.com"
_GH_TITLE = "📟 盘中盯盘预警"


def _gh_env() -> tuple[str | None, str | None]:
    return (os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN"),
            os.getenv("GITHUB_REPOSITORY"))


def _gh_req(method: str, url: str, token: str, payload: dict | None = None):
    import json
    import urllib.request
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {token}")
    r.add_header("Accept", "application/vnd.github+json")
    r.add_header("User-Agent", "invest-live-watch")
    if data:
        r.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(r, timeout=30) as resp:
        return json.loads(resp.read().decode() or "null")


def _gh_issue_number(cache: list, token: str, repo: str) -> int:
    """解析（或创建）跟踪 issue「📟 盘中盯盘预警」的编号，缓存到 cache[0]。"""
    import urllib.parse
    if cache[0] is not None:
        return cache[0]
    mention = os.getenv("LIVE_WATCH_MENTION") or f"@{repo.split('/')[0]}"
    q = urllib.parse.quote(f'repo:{repo} is:issue is:open in:title "{_GH_TITLE}"')
    found = _gh_req("GET", f"{_GH_API}/search/issues?q={q}", token)
    items = [i for i in (found.get("items") or []) if i.get("title") == _GH_TITLE]
    if items:
        cache[0] = items[0]["number"]
    else:
        created = _gh_req("POST", f"{_GH_API}/repos/{repo}/issues", token, {
            "title": _GH_TITLE,
            "body": f"{mention} 本 issue 由 live-watch 盯盘工作流在触发止损/破位/"
                    "买点时追加预警评论（每条 @你 触发通知邮件）。",
        })
        cache[0] = created["number"]
    return cache[0]


def _gh_notify(cache: list, now: datetime, body: str, keys: list[str]) -> None:
    """把新触发的预警以评论形式追加到跟踪 issue（→ @你 触发 GitHub 邮件提醒）。

    评论尾部嵌隐藏标记 <!--lwk:key1|key2-->，记录本条推送的去重键，供下次启动
    重建「已推送集合」（见 _seed_seen），从而重启/重跑不重复推同一信号。
    在 GitHub Actions 内用自带 GITHUB_TOKEN + GITHUB_REPOSITORY；缺则静默跳过。
    """
    token, repo = _gh_env()
    if not token or not repo:
        print("  (未配置 GITHUB_TOKEN/REPOSITORY，跳过推送，仅打日志)")
        return
    mention = os.getenv("LIVE_WATCH_MENTION") or f"@{repo.split('/')[0]}"
    try:
        n = _gh_issue_number(cache, token, repo)
        marker = "<!--lwk:" + "|".join(keys) + "-->"
        _gh_req("POST", f"{_GH_API}/repos/{repo}/issues/{n}/comments", token,
                {"body": f"{mention} **{now:%Y-%m-%d %H:%M} CST**\n\n{body}\n\n{marker}"})
        print(f"  → 已推送到 issue #{n}")
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ 推送失败：{repr(e)[:160]}")


def _seed_seen(cache: list) -> set[str]:
    """启动时从跟踪 issue「今日」评论的隐藏标记重建已推送集合，使去重跨重启/重跑生效。

    重启（改参数/崩溃重跑/cancel-in-progress）不会重复推已报过的信号；跨天自动清零
    （只认当天日期的评论），保证新交易日重新提醒一次。
    """
    import re
    token, repo = _gh_env()
    if not token or not repo:
        return set()
    try:
        n = _gh_issue_number(cache, token, repo)
        since = (datetime.now(timezone.utc) - timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
        comments = _gh_req(
            "GET", f"{_GH_API}/repos/{repo}/issues/{n}/comments?since={since}&per_page=100", token)
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ 去重集重建失败（按空集起步）：{repr(e)[:120]}")
        return set()
    today = _now_cst().strftime("%Y-%m-%d")
    seen: set[str] = set()
    for c in (comments or []):
        b = c.get("body", "")
        if today not in b:                      # 只认当天评论，跨天自动重置
            continue
        m = re.search(r"<!--lwk:(.*?)-->", b, re.S)
        if m:
            seen.update(k for k in m.group(1).split("|") if k)
    return seen


def _next_interval(min_dist: float, base: int, lo: int, hi: int) -> int:
    """自适应轮询间隔：距最近触发线 <1% 时提频到 lo，<3% 用 base，更远放宽到 hi。

    效果：临界时刻更敏锐（不错过止损/买点），平静时段少打 API（限频友好），
    整体消耗反而低于固定 base 间隔。
    """
    if min_dist <= 0.01:
        return lo
    if min_dist <= 0.03:
        return base
    return hi


def _should_flush(has_crit: bool, pending_n: int, pending_age_s: float,
                  digest_s: float, hm: int) -> bool:
    """摘要缓冲是否该推送：
    - 出现关键预警（卖出类风控）→ 立即推，顺带清空缓冲；
    - 缓冲最早一条达到 digest 窗口龄期 → 合并推一封；
    - 临近收盘（14:45 后）→ 强制清空，不让当日机会过夜。
    """
    if has_crit:
        return True
    if pending_n <= 0:
        return False
    return pending_age_s >= digest_s or hm >= 885


def _once_flush_batch(hm: int, digest_min: int, step_min: int) -> bool:
    """once 模式（FaaS 定时逐次扫描）本次是否推送 batch 类预警。

    无状态设计：batch 预警不推就不写去重标记，下次扫描会重新检测到——
    等价于"缓冲"，只在每 digest_min 分钟边界后的首个扫描 tick 统一推送。
    14:45 后一律推（尾盘清仓不过夜）；digest_min<=0 → 每次都推。
    """
    if digest_min <= 0 or hm >= 885:
        return True
    return (hm % digest_min) < max(1, step_min)


def _is_trading_day_db(repo: BaseRepository, day: str) -> bool | None:
    """查库内 trade_calendar 判交易日（once 模式用，免 Tushare 初始化开销）。"""
    try:
        df = repo.read_sql(
            "SELECT is_open FROM trade_calendar WHERE cal_date=:d", {"d": day})
    except Exception:  # noqa: BLE001
        return None
    if df.empty:
        return None
    return int(df["is_open"].iloc[0]) == 1


def _once(args: argparse.Namespace) -> dict:
    """单次无状态扫描（FaaS 定时触发用）：取数→评估→分级推送→退出。

    去重集从当日 issue 评论恢复（_seed_seen），跨调用不重复报警；
    crit（卖出类风控）每次都推，batch（买点/提示）按 _once_flush_batch 的
    墙钟边界合并推。非交易时段/非交易日毫秒级退出。
    """
    now = _now_cst()
    hm = now.hour * 60 + now.minute
    if now.weekday() >= 5:
        return {"skipped": "weekend"}
    if not (568 <= hm <= 902):                  # 09:28 前 / 15:02 后
        return {"skipped": "off-hours"}
    if 690 < hm < 780:                          # 午休
        return {"skipped": "lunch-break"}
    engine = make_engine(args.db) if args.db else make_engine()
    if _is_trading_day_db(BaseRepository(engine), now.strftime("%Y%m%d")) is False:
        return {"skipped": "holiday"}

    ctx = _build_context(args)
    issue: list = [None]
    seen = _seed_seen(issue) if args.notify else set()
    rt = _fetch_rt(ctx)
    _, _, _, alerts, _ = _scan(ctx, rt, args)
    new = [(k, line, sev) for k, line, sev in alerts if k not in seen]
    _persist_alerts(ctx["engine"], now, new)
    crit = [(k, line) for k, line, sev in new if sev == "crit"]
    batch = [(k, line) for k, line, sev in new if sev != "crit"]
    # ETF 实时自检（每日一次，提示类）
    if ctx["etf_codes"]:
        got = [(c, rt.get(c, {}).get("price")) for c in ctx["etf_codes"]]
        n_ok = sum(1 for _, p in got if p)
        key = f"ETFSELF:{now:%Y%m%d}:{n_ok}"
        if key not in seen:
            batch.append((key, f"🔎 ETF实时自检：{n_ok}/{len(got)} 取到现价"))
            _persist_alerts(ctx["engine"], now,
                            [(key, f"🔎 ETF实时自检：{n_ok}/{len(got)} 取到现价", "batch")])
    flush = bool(crit) or _once_flush_batch(hm, args.digest_window, args.once_step)
    items = crit + (batch if flush else [])
    if items:
        head = ("🚨 即时风控预警" if crit
                else f"📬 摘要（{len(batch)} 条买点/提示，窗口 {args.digest_window}min）")
        body = head + "\n\n" + "\n".join(line for _, line in items)
        print(body)
        if args.notify:
            _gh_notify(issue, now, body, [k for k, _ in items])
    else:
        print(f"✓ {now:%H:%M} CST 无新触发（已推 {len(seen)}，待摘要 {len(batch)}）")
    return {"new": len(new), "pushed": len(items), "held_for_digest": 0 if flush else len(batch)}


def run_once(**overrides) -> dict:
    """FaaS handler 入口：全部参数走环境变量（可用 overrides 覆盖），执行一次扫描。

    环境变量：INVEST_DB_URL / TUSHARE_TOKEN / TUSHARE_HTTP_URL /
    GITHUB_TOKEN / GITHUB_REPOSITORY（推 issue 评论 → 邮件），可选
    LIVE_HARD_STOP / LIVE_PULLBACK / LIVE_BUY_WEIGHT / DIGEST_WINDOW / ONCE_STEP。
    """
    args = argparse.Namespace(
        db=None,
        hard_stop=float(os.getenv("LIVE_HARD_STOP", "0.08")),
        pp_trigger=float(os.getenv("LIVE_PP_TRIGGER", "0.15")),
        pp_trim_dd=float(os.getenv("LIVE_PP_TRIM", "0.08")),
        pp_exit_dd=float(os.getenv("LIVE_PP_EXIT", "0.12")),
        pullback_pct=float(os.getenv("LIVE_PULLBACK", "0.03")),
        buy_weight=float(os.getenv("LIVE_BUY_WEIGHT", "0.05")),
        digest_window=int(os.getenv("DIGEST_WINDOW", "20")),
        once_step=int(os.getenv("ONCE_STEP", "3")),
        notify=True,
    )
    for k, v in overrides.items():
        setattr(args, k, v)
    return _once(args)


def _watch(args: argparse.Namespace) -> None:
    """常驻盯盘：交易时段内轮询，预警分级推送——卖出类风控立即推，
    买点/提示类进缓冲按 --digest-window 合并推送（减少邮件打扰、不漏关键信号）。

    轮询间隔自适应：距任一触发线 <1% 提频到 --min-interval，平静时放宽到
    --max-interval。午休/盘前自动暂停，收盘（15:00 后）或非交易日自动退出。
    均线基准与持仓/观察池只在首个交易周期取一次并全天复用；每周期仅刷新实时价。
    """
    interval = max(15, args.interval)
    lo = max(10, args.min_interval)
    hi = max(interval, args.max_interval)
    digest_s = max(0, args.digest_window) * 60
    # 交易日守卫：cron 每工作日 09:25 起，节假日（trade_cal is_open=0）直接退出，不空转占用额度。
    today = _now_cst().strftime("%Y%m%d")
    if _is_trading_day(today) is False:
        print(f"⏹ {today} 非交易日（节假日/休市），盯盘退出。")
        return
    ctx = None
    issue = [None]
    # 跨重启去重：从今日 issue 评论恢复「已推送」集合，重启/重跑不重复推同一信号。
    # 摘要缓冲里「已检测未推送」的条目不写 marker，重启后会重新检测进缓冲，不丢。
    seen: set[str] = _seed_seen(issue) if args.notify else set()
    if seen:
        print(f"↺ 已从今日评论恢复 {len(seen)} 条已推送信号（重启不重复推，仅新增/变化才推）")
    pending: list[tuple[str, str]] = []      # 摘要缓冲 [(key, line)]
    pending_since: datetime | None = None    # 缓冲最早一条的入队时间
    while True:
        now = _now_cst()
        hm = now.hour * 60 + now.minute
        if now.weekday() >= 5:
            print(f"⏹ {now:%Y-%m-%d %H:%M} CST 非交易日，盯盘退出。")
            return
        if hm > 900:                       # 15:00 收盘后
            print(f"⏹ {now:%H:%M} CST 已收盘，盯盘结束（本轮累计推送 {len(seen)} 项）。")
            return
        if hm < 570:                       # 09:30 前
            print(f"⏳ {now:%H:%M} CST 未开盘，等待开盘…")
            time.sleep(interval)
            continue
        if 690 < hm < 780:                 # 11:30-13:00 午休
            print(f"🍱 {now:%H:%M} CST 午休，暂停轮询。")
            time.sleep(interval)
            continue
        first = ctx is None
        if first:
            ctx = _build_context(args)
            print(f"▶ 盯盘启动 | 基准 {ctx['dt']} | 股票持仓 {len(ctx['holds'])} 只 | "
                  f"ETF持仓 {len(ctx['etf_holds'])} 只 | 观察 {len(ctx['watch'])} 只 | "
                  f"间隔 {interval}s | 推送 {'开' if args.notify else '关'}")
        # 单轮取数/评估异常（Tushare 超时/限频/重启抢占导致的 ip超限 等）只跳过本轮、
        # 下轮重试，绝不让全天常驻 job 因一次瞬时错误崩溃。
        try:
            rt = _fetch_rt(ctx)
            _, _, _, alerts, min_dist = _scan(ctx, rt, args)
        except Exception as e:  # noqa: BLE001
            print(f"⚠️ {now:%H:%M} CST 本轮取数/评估失败，跳过：{repr(e)[:160]}")
            time.sleep(interval)
            continue
        if first and ctx["etf_codes"]:     # 首轮自检：ETF 实时价（rt_etf_k）是否取到
            got = [(c, rt.get(c, {}).get("price")) for c in ctx["etf_codes"]]
            n = sum(1 for _, p in got if p)
            detail = "，".join(f"{c}={p}" if p else f"{c}=无价" for c, p in got)
            tail = "" if n else "（腾讯免费源 qt.gtimg.cn 未取到，检查 Actions 外网/代码格式）"
            line = f"🔎 ETF实时自检：{n}/{len(got)} 取到现价（{detail}）{tail}"
            print(line)
            key = f"ETFSELF:{today}:{n}"    # 含结果数，状态变化即重发一次；去重跨重启
            if key not in seen:            # 自检属提示类：进摘要缓冲，不单发邮件
                seen.add(key)
                if not pending:
                    pending_since = now
                pending.append((key, line))
                _persist_alerts(ctx["engine"], now, [(key, line, "batch")])
        # 分级：卖出类风控(crit)立即推；买点/提示类(batch)进缓冲合并推
        new = [(k, line, sev) for k, line, sev in alerts if k not in seen]
        for k, _, _ in new:
            seen.add(k)
        _persist_alerts(ctx["engine"], now, new)
        crit = [(k, line) for k, line, sev in new if sev == "crit"]
        batch = [(k, line) for k, line, sev in new if sev != "crit"]
        if batch:
            if not pending:
                pending_since = now
            pending.extend(batch)
        if new:
            print(f"⏰ {now:%H:%M} CST 新触发 {len(new)} 项"
                  f"（即时 {len(crit)} / 缓冲 {len(batch)}，缓冲积压 {len(pending)}）：\n"
                  + "\n".join(line for _, line, _ in new))
        else:
            print(f"✓ {now:%H:%M} CST 无新触发（累计已报 {len(seen)}，缓冲积压 {len(pending)}）")
        age = (now - pending_since).total_seconds() if pending_since else 0.0
        if args.notify and _should_flush(bool(crit), len(pending), age, digest_s, hm):
            items = crit + pending
            if items:
                head = ("🚨 即时风控预警" if crit else
                        f"📬 摘要（{len(pending)} 条买点/提示，窗口 {args.digest_window}min）")
                body = head + "\n\n" + "\n".join(line for _, line in items)
                _gh_notify(issue, now, body, [k for k, _ in items])
                pending, pending_since = [], None
        elif crit and not args.notify:
            pass                            # 未开推送时 crit 已随日志打印
        interval = _next_interval(min_dist, max(15, args.interval), lo, hi)
        time.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser(description="盘中实时盯盘")
    ap.add_argument("--db", default=None,
                    help="DB URL；省略则读环境变量 INVEST_DB_URL/DB_*（生产 MySQL）")
    ap.add_argument("--hard-stop", type=float, default=0.08)
    ap.add_argument("--pp-trigger", type=float, default=float(os.getenv("LIVE_PP_TRIGGER", "0.15")),
                    help="盈利保护启动阈值：持有期峰值较成本浮盈达此值后开始盯回撤")
    ap.add_argument("--pp-trim-dd", type=float, default=float(os.getenv("LIVE_PP_TRIM", "0.08")),
                    help="自峰值回撤达此值 → 减半锁盈")
    ap.add_argument("--pp-exit-dd", type=float, default=float(os.getenv("LIVE_PP_EXIT", "0.12")),
                    help="自峰值回撤达此值 → 清仓止盈")
    ap.add_argument("--pullback-pct", type=float, default=0.03)
    ap.add_argument("--buy-weight", type=float, default=0.05,
                    help="买入类挂单的单只目标占比（占账户总权益，默认 5%%）")
    ap.add_argument("--alert", action="store_true",
                    help="只报触发/逼近项 + 交易时段感知（盯盘轮询用）")
    ap.add_argument("--watch", action="store_true",
                    help="常驻盯盘：交易时段内每 --interval 秒轮询，只报新触发")
    ap.add_argument("--once", action="store_true",
                    help="单次无状态扫描后退出（FaaS 定时触发用；去重靠 issue 评论恢复）")
    ap.add_argument("--once-step", type=int, default=3,
                    help="--once 模式的定时器间隔（分钟），用于摘要边界判定（默认 3）")
    ap.add_argument("--interval", type=int, default=60,
                    help="--watch 常态轮询间隔秒数（默认 60；距触发线远近自适应升降频）")
    ap.add_argument("--min-interval", type=int, default=20,
                    help="距任一触发线 <1%% 时的提频间隔秒数（默认 20）")
    ap.add_argument("--max-interval", type=int, default=180,
                    help="距所有触发线 >3%% 时的降频间隔秒数（默认 180，限频友好）")
    ap.add_argument("--digest-window", type=int, default=20,
                    help="买点/提示类预警的摘要合并窗口（分钟，默认 20；"
                         "卖出类风控不受此限、始终立即推送；0=全部立即推）")
    ap.add_argument("--notify", action="store_true",
                    help="--watch 时把新触发追加到跟踪 issue（需 GITHUB_TOKEN/REPOSITORY）")
    args = ap.parse_args()

    if args.once:
        print(_once(args))
        return
    if args.watch:
        _watch(args)
        return

    now = _now_cst()
    if args.alert and not _trading(now):
        print(f"⏸ {now:%Y-%m-%d %H:%M} CST 非交易时段，跳过。")
        return

    ctx = _build_context(args)
    rt = _fetch_rt(ctx)
    hold_rows, watch_rows, etf_hold_rows, alerts, _ = _scan(ctx, rt, args)

    if args.alert:
        ts = f"{now:%H:%M} CST"
        if alerts:
            print(f"⏰ {ts} 触发 {len(alerts)} 项：")
            print("\n".join(line for _, line, _sev in alerts))
        else:
            print(f"✓ {ts} 无触发（持仓未碰止损/破位，观察池未到买点）")
        return

    print(f"# 盘中速查 — 实时价 / 落库基准 {ctx['dt']}\n")
    try:
        from invest_model.signals.fear import fear_gauge, format_fear
        print(format_fear(fear_gauge(ctx["engine"], ctx["dt"])) + "\n")
    except Exception:  # noqa: BLE001
        pass
    print("## 持仓监控")
    print("| 名称 | 现价 | 涨跌 | 成本 | 浮盈亏 | 止损价 | MA20 | 状态 |")
    print("|---|---|---|---|---|---|---|---|")
    for n, px, chg, cost, pnl, stop, ma20, st in hold_rows:
        print(f"| {n} | {px:.2f} | {chg:+.1%} | {cost:.2f} | {pnl:+.1%} | {stop:.2f} | {ma20:.2f} | {st} |")
    if etf_hold_rows:
        print("\n## ETF持仓监控（rt_k 实时价 + fund 前复权MA20）")
        print("| 名称 | 现价 | 涨跌 | 成本 | 浮盈亏 | 止损价 | MA20 | 状态 |")
        print("|---|---|---|---|---|---|---|---|")
        for n, px, chg, cost, pnl, stop, ma20, st in etf_hold_rows:
            ma20s = f"{ma20:.3f}" if ma20 else "—"
            print(f"| {n} | {px:.3f} | {chg:+.1%} | {cost:.3f} | {pnl:+.1%} | {stop:.3f} | {ma20s} | {st} |")
    print("\n## 观察池·买点价位预警")
    print("| 名称 | 级 | 现价 | 回踩位(MA20) | 突破位(20日高) | 状态 |")
    print("|---|---|---|---|---|---|")
    for n, g, px, ma20, hi20, st in watch_rows:
        print(f"| {n} | {g} | {px:.2f} | {ma20:.2f} | {hi20:.2f} | {st} |")

    etf_rows = _etf_watch(ctx["dt"])
    if etf_rows:
        print("\n## ETF观察·EOD买点位（无实时源，盘后基准）")
        print("| 代码 | 备注 | 收盘 | 回踩位(MA20) | MA60 | 突破位(20日高) | 状态 |")
        print("|---|---|---|---|---|---|---|")
        for code, note, last, ma20, ma60t, hi20, st in etf_rows:
            print(f"| {code} | {note} | {last:.3f} | {ma20:.3f} | {ma60t} | {hi20:.3f} | {st} |")


if __name__ == "__main__":
    main()
