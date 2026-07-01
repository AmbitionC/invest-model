"""盘中实时盯盘：用 rt_k 现价 + 库里均线/成本，给出持仓风控预警 + 观察池买点价位预警。

盘后请用 build_action_plan.py（落库 EOD 数据出完整三段式计划）；本工具用于盘中速查。

示例：
  python scripts/live_check.py --db sqlite:///./data/real.db --advisor-led --hard-stop 0.08
"""

from __future__ import annotations

import argparse
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
    start = (pd.to_datetime(dt) - pd.Timedelta(days=130)).strftime("%Y%m%d")
    rows = []
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

    for code, note in items:
        df = _q("fund_daily", ts_code=code, start_date=start, end_date=dt)
        if df is None or df.empty:
            rows.append((code, note, float("nan"), float("nan"), "?", float("nan"), "取数失败"))
            continue
        df = df.sort_values("trade_date")
        # 前复权：用 fund_adj 把份额拆分/折算抹平（否则拆分日价格台阶会污染均线）。
        # 取不到复权因子时不静默用原始价（会因拆分台阶误判趋势），直接标注存疑。
        adj = _q("fund_adj", ts_code=code, start_date=start, end_date=dt)
        cl_raw = pd.to_numeric(df["close"], errors="coerce")
        if adj is None or not len(adj):
            last = float(cl_raw.dropna().iloc[-1])
            rows.append((code, note, last, float("nan"), "?", float("nan"), "复权因子取数失败(趋势存疑,勿据此操作)"))
            continue
        adj = adj.sort_values("trade_date")
        fac = dict(zip(adj["trade_date"], pd.to_numeric(adj["adj_factor"], errors="coerce")))
        last_fac = pd.to_numeric(adj["adj_factor"], errors="coerce").iloc[-1]
        f = df["trade_date"].map(fac).astype(float).ffill().fillna(1.0)
        cl = (cl_raw * f / last_fac).dropna()
        last = float(cl.iloc[-1])
        ma20 = float(cl.tail(20).mean()) if len(cl) >= 20 else float("nan")
        ma60 = float(cl.tail(60).mean()) if len(cl) >= 60 else float("nan")
        ma60_prev = float(cl.tail(65).head(60).mean()) if len(cl) >= 65 else ma60
        ma60_up = ma60 >= ma60_prev
        hi20 = float(cl.iloc[-21:-1].max()) if len(cl) >= 21 else float("nan")
        dev = (last / ma20 - 1) if ma20 == ma20 else 0.0
        if not (last >= ma60 and ma60_up):
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
    return {"engine": engine, "repo": repo, "dt": dt, "holds": holds,
            "watch": watch, "levels": levels, "g_of": g_of,
            "trailing_only": trailing_only, "codes": held + watch}


def _scan(ctx: dict, rt: dict, args: argparse.Namespace) -> tuple[list, list, list]:
    """用实时价 rt 评估持仓风控 + 观察池买点。

    返回 (持仓行, 观察行, 预警) 三元组；预警为 [(dedup_key, 展示行)]，
    dedup_key 含状态标签——同一票同一状态只算一次，状态切换才算新预警。
    """
    holds = ctx["holds"]; levels = ctx["levels"]
    trailing_only = ctx["trailing_only"]; g_of = ctx["g_of"]
    hold_rows, watch_rows, alerts = [], [], []
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
    return hold_rows, watch_rows, alerts


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
                    "body": "本 issue 由 live-watch 盯盘工作流在触发止损/破位/买点时追加预警评论。",
                })
                cache[0] = created["number"]
        _req("POST", f"{api}/repos/{repo}/issues/{cache[0]}/comments",
             {"body": f"**{now:%Y-%m-%d %H:%M} CST**\n\n{body}"})
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
        if ctx is None:
            ctx = _build_context(args)
            print(f"▶ 盯盘启动 | 基准 {ctx['dt']} | 持仓 {len(ctx['holds'])} 只 | "
                  f"观察 {len(ctx['watch'])} 只 | 间隔 {interval}s "
                  f"| 推送 {'开' if args.notify else '关'}")
        rt = get_realtime(ctx["codes"])
        _, _, alerts = _scan(ctx, rt, args)
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
    rt = get_realtime(ctx["codes"])
    hold_rows, watch_rows, alerts = _scan(ctx, rt, args)

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
