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
from invest_model.signals.realtime import get_realtime  # noqa: E402


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
        ma60 = float(cl.tail(60).mean()) if len(cl) >= 60 else float("nan")
        ma60_prev = float(cl.tail(65).head(60).mean()) if len(cl) >= 65 else ma60
        hi20 = float(cl.iloc[-21:-1].max())   # 前 20 日高（不含今日）=突破位
        out[code] = {"ma20": ma20, "ma60": ma60, "ma60_up": ma60 >= ma60_prev, "hi20": hi20}
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
            "last": float(cl.iloc[-1]), "adj_ok": adj_ok}


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


def _load_etf_holdings() -> list[dict]:
    """从最新 config/holding_snapshot_*.csv 读 ETF 持仓（code/name/cost）。

    current_holding 表按设计只存 stock（rt_k 也只支持股票），故 ETF 持仓直接从快照
    CSV 读——该 CSV 正是入库 current_holding 的同一来源，盘中在 Actions checkout 里可用。
    """
    files = sorted(glob.glob(os.path.join(_ROOT, "config", "holding_snapshot_*.csv")))
    if not files:
        return []
    df = pd.read_csv(files[-1], dtype=str)
    if "asset_type" not in df.columns:
        return []
    etf = df[df["asset_type"].astype(str).str.lower() == "etf"]
    out = []
    for _, r in etf.iterrows():
        cost = pd.to_numeric(r.get("cost_price"), errors="coerce")
        out.append({"code": r["code"], "name": r.get("name") or r["code"],
                    "cost": float(cost) if cost == cost else 0.0})
    return out


def _fetch_rt(ctx: dict) -> dict:
    """取实时价：股票+观察池走一批，ETF 单独一批（隔离 ETF 取数失败，不拖累股票）。"""
    rt = get_realtime(ctx["codes"])
    if ctx.get("etf_codes"):
        try:
            rt.update(get_realtime(ctx["etf_codes"]))
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
    g_of = dict(zip(reco["code"], reco["grade"])) if not reco.empty else {}
    # ETF 持仓：成本读快照 CSV，均线基准用 fund_daily 前复权（可转债不盯，用户上市即卖）
    etf_holds = _load_etf_holdings()
    etf_levels: dict[str, dict] = {}
    if etf_holds:
        from invest_model.sources.tushare_client import TushareClient
        pro = TushareClient().pro
        for e in etf_holds:
            lv = _fund_levels(pro, e["code"], dt)
            if lv is not None:
                etf_levels[e["code"]] = lv
    return {"engine": engine, "repo": repo, "dt": dt, "holds": holds,
            "watch": watch, "levels": levels, "g_of": g_of,
            "trailing_only": trailing_only, "codes": held + watch,
            "etf_holds": etf_holds, "etf_levels": etf_levels,
            "etf_codes": [e["code"] for e in etf_holds]}


def _scan(ctx: dict, rt: dict, args: argparse.Namespace) -> tuple[list, list, list, list]:
    """用实时价 rt 评估持仓风控（股票+ETF） + 观察池买点。

    返回 (股票持仓行, 观察行, ETF持仓行, 预警) 四元组；预警为 [(dedup_key, 展示行)]，
    dedup_key 含状态标签——同一票同一状态只算一次，状态切换才算新预警。
    """
    holds = ctx["holds"]; levels = ctx["levels"]
    trailing_only = ctx["trailing_only"]; g_of = ctx["g_of"]
    hold_rows, watch_rows, etf_rows, alerts = [], [], [], []
    for _, h in holds.iterrows():
        c = h["code"]; q = rt.get(c, {}); lv = levels.get(c, {})
        px = q.get("price"); pre = q.get("pre_close"); cost = float(h["cost_price"] or 0)
        if not px:
            continue
        chg = (px / pre - 1) if pre else 0
        pnl = (px / cost - 1) if cost else 0
        stop = cost * (1 - args.hard_stop); ma20 = lv.get("ma20")
        ex = c in trailing_only            # 移动止盈白名单：不套硬止损
        if not ex and pnl <= -args.hard_stop:
            st, hit = "⚠️ 已触发硬止损，清仓", True
        elif ma20 and px < ma20 * 0.995:   # 0.5% 缓冲，避免贴线来回抖动
            st, hit = ("破MA20移动止盈，确认清仓" if ex else "破MA20，盘后确认清仓"), True
        elif not ex and pnl <= -args.hard_stop + 0.02:
            st, hit = "逼近止损，盯紧", True
        else:
            st, hit = ("持有(MA20移动止盈)" if ex else "持有"), False
        hold_rows.append((q.get("name", c), px, chg, cost, pnl, stop, ma20, st))
        if hit:
            alerts.append((f"H:{c}:{st}",
                           f"🔴 持仓 {q.get('name', c)} {px:.2f}({chg:+.1%}) — {st}"))
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
        if hit:
            alerts.append((f"W:{c}:{st}",
                           f"🟢 观察 {q.get('name', c)}({g_of.get(c, '')}) {px:.2f} — {st}"))
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
            st, hit = "破MA20，盘后确认减/清", True
        elif pnl <= -args.hard_stop + 0.02:
            st, hit = "逼近止损，盯紧", True
        else:
            st, hit = "持有", False
        etf_rows.append((e["name"], px, chg, cost, pnl, stop, ma20, st))
        if hit:
            alerts.append((f"E:{c}:{st}",
                           f"🟠 ETF持仓 {e['name']} {px:.3f}({chg:+.1%}) — {st}"))
    return hold_rows, watch_rows, etf_rows, alerts


def _gh_notify(cache: list, now: datetime, body: str) -> None:
    """把新触发的预警以评论形式追加到跟踪 issue「📟 盘中盯盘预警」（→ GitHub 邮件提醒）。

    在 GitHub Actions 内运行，用自带 GITHUB_TOKEN + GITHUB_REPOSITORY；缺则静默跳过。
    issue 号首次解析后缓存在 cache[0]，后续直接复用。
    """
    import json
    import urllib.parse
    import urllib.request

    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    repo = os.getenv("GITHUB_REPOSITORY")
    if not token or not repo:
        print("  (未配置 GITHUB_TOKEN/REPOSITORY，跳过推送，仅打日志)")
        return
    api = "https://api.github.com"
    title = "📟 盘中盯盘预警"
    # @提及接收人：机器人评论默认不会触发邮件（除非你 watch 了该 issue），
    # @提及则无视订阅状态直接发通知邮件。默认 @仓库 owner，可用 LIVE_WATCH_MENTION 覆盖。
    mention = os.getenv("LIVE_WATCH_MENTION") or f"@{repo.split('/')[0]}"

    def _req(method: str, url: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        r = urllib.request.Request(url, data=data, method=method)
        r.add_header("Authorization", f"Bearer {token}")
        r.add_header("Accept", "application/vnd.github+json")
        r.add_header("User-Agent", "invest-live-watch")
        if data:
            r.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(r, timeout=30) as resp:
            return json.loads(resp.read().decode() or "null")

    try:
        if cache[0] is None:
            q = urllib.parse.quote(f'repo:{repo} is:issue is:open in:title "{title}"')
            found = _req("GET", f"{api}/search/issues?q={q}")
            items = [i for i in (found.get("items") or []) if i.get("title") == title]
            if items:
                cache[0] = items[0]["number"]
            else:
                created = _req("POST", f"{api}/repos/{repo}/issues", {
                    "title": title,
                    "body": f"{mention} 本 issue 由 live-watch 盯盘工作流在触发止损/"
                            "破位/买点时追加预警评论（每条 @你 以触发通知邮件）。",
                })
                cache[0] = created["number"]
        _req("POST", f"{api}/repos/{repo}/issues/{cache[0]}/comments",
             {"body": f"{mention} **{now:%Y-%m-%d %H:%M} CST**\n\n{body}"})
        print(f"  → 已推送到 issue #{cache[0]}")
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠️ 推送失败：{repr(e)[:160]}")


def _watch(args: argparse.Namespace) -> None:
    """常驻盯盘：交易时段内每 interval 秒轮询一次，只在出现「新」触发时推送。

    午休/盘前自动暂停，收盘（15:00 后）或非交易日自动退出，避免空转占用 job。
    均线基准与持仓/观察池只在首个交易周期取一次并全天复用；每周期仅刷新实时价。
    """
    interval = max(15, args.interval)
    ctx = None
    seen: set[str] = set()
    issue = [None]
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
            _, _, _, alerts = _scan(ctx, rt, args)
        except Exception as e:  # noqa: BLE001
            print(f"⚠️ {now:%H:%M} CST 本轮取数/评估失败，跳过：{repr(e)[:160]}")
            time.sleep(interval)
            continue
        if first and ctx["etf_codes"]:     # 首轮自检：ETF 实时价是否取到（rt_k 是否支持 ETF）
            got = sum(1 for c in ctx["etf_codes"] if rt.get(c, {}).get("price"))
            print(f"  ETF实时自检：{got}/{len(ctx['etf_codes'])} 取到现价"
                  + ("" if got else "（rt_k 未返回 ETF 价，ETF 盘中风控本轮无效，见收盘 EOD 视图）"))
        new = [(k, line) for k, line in alerts if k not in seen]
        for k, _ in new:
            seen.add(k)
        if new:
            body = "\n".join(line for _, line in new)
            print(f"⏰ {now:%H:%M} CST 新触发 {len(new)} 项：\n{body}")
            if args.notify:
                _gh_notify(issue, now, body)
        else:
            print(f"✓ {now:%H:%M} CST 无新触发（累计已报 {len(seen)}）")
        time.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser(description="盘中实时盯盘")
    ap.add_argument("--db", default=None,
                    help="DB URL；省略则读环境变量 INVEST_DB_URL/DB_*（生产 MySQL）")
    ap.add_argument("--hard-stop", type=float, default=0.08)
    ap.add_argument("--pullback-pct", type=float, default=0.03)
    ap.add_argument("--alert", action="store_true",
                    help="只报触发/逼近项 + 交易时段感知（盯盘轮询用）")
    ap.add_argument("--watch", action="store_true",
                    help="常驻盯盘：交易时段内每 --interval 秒轮询，只报新触发")
    ap.add_argument("--interval", type=int, default=60,
                    help="--watch 轮询间隔秒数（默认 60）")
    ap.add_argument("--notify", action="store_true",
                    help="--watch 时把新触发追加到跟踪 issue（需 GITHUB_TOKEN/REPOSITORY）")
    args = ap.parse_args()

    if args.watch:
        _watch(args)
        return

    now = _now_cst()
    if args.alert and not _trading(now):
        print(f"⏸ {now:%Y-%m-%d %H:%M} CST 非交易时段，跳过。")
        return

    ctx = _build_context(args)
    rt = _fetch_rt(ctx)
    hold_rows, watch_rows, etf_hold_rows, alerts = _scan(ctx, rt, args)

    if args.alert:
        ts = f"{now:%H:%M} CST"
        if alerts:
            print(f"⏰ {ts} 触发 {len(alerts)} 项：")
            print("\n".join(line for _, line in alerts))
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
