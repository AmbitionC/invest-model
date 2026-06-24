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
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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


def main() -> None:
    ap = argparse.ArgumentParser(description="盘中实时盯盘")
    ap.add_argument("--db", default="sqlite:///./data/real.db")
    ap.add_argument("--hard-stop", type=float, default=0.08)
    ap.add_argument("--pullback-pct", type=float, default=0.03)
    ap.add_argument("--alert", action="store_true",
                    help="只报触发/逼近项 + 交易时段感知（盯盘轮询用）")
    args = ap.parse_args()

    # 交易时段感知（CST=UTC+8；A股 09:30-11:30 / 13:00-15:00，工作日）
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    hm = now.hour * 60 + now.minute
    trading = (now.weekday() < 5) and (570 <= hm <= 690 or 780 <= hm <= 900)
    if args.alert and not trading:
        print(f"⏸ {now:%Y-%m-%d %H:%M} CST 非交易时段，跳过。")
        return

    engine = make_engine(args.db)
    repo = BaseRepository(engine)
    # 移动止盈白名单：这些票按"破MA20"管，不套用 -8% 硬止损（如业绩爆发的核心仓）
    import os
    tro = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "config", "trailing_only.txt")
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
    rt = get_realtime(held + watch)
    g_of = dict(zip(reco["code"], reco["grade"])) if not reco.empty else {}

    # 计算各行状态
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
            alerts.append(f"🔴 持仓 {q.get('name', c)} {px:.2f}({chg:+.1%}) — {st}")
    for c in watch:
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
            alerts.append(f"🟢 观察 {q.get('name', c)}({g_of.get(c, '')}) {px:.2f} — {st}")

    if args.alert:
        ts = f"{now:%H:%M} CST"
        if alerts:
            print(f"⏰ {ts} 触发 {len(alerts)} 项：")
            print("\n".join(alerts))
        else:
            print(f"✓ {ts} 无触发（持仓未碰止损/破位，观察池未到买点）")
        return

    print(f"# 盘中速查 — 实时价 / 落库基准 {dt}\n")
    try:
        from invest_model.signals.fear import fear_gauge, format_fear
        print(format_fear(fear_gauge(engine, dt)) + "\n")
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

    etf_rows = _etf_watch(dt)
    if etf_rows:
        print("\n## ETF观察·EOD买点位（无实时源，盘后基准）")
        print("| 代码 | 备注 | 收盘 | 回踩位(MA20) | MA60 | 突破位(20日高) | 状态 |")
        print("|---|---|---|---|---|---|---|")
        for code, note, last, ma20, ma60t, hi20, st in etf_rows:
            print(f"| {code} | {note} | {last:.3f} | {ma20:.3f} | {ma60t} | {hi20:.3f} | {st} |")


if __name__ == "__main__":
    main()
