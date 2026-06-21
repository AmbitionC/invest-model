"""盘中实时盯盘：用 rt_k 现价 + 库里均线/成本，给出持仓风控预警 + 观察池买点价位预警。

盘后请用 build_action_plan.py（落库 EOD 数据出完整三段式计划）；本工具用于盘中速查。

示例：
  python scripts/live_check.py --db sqlite:///./data/real.db --advisor-led --hard-stop 0.08
"""

from __future__ import annotations

import argparse
import sys
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


def main() -> None:
    ap = argparse.ArgumentParser(description="盘中实时盯盘")
    ap.add_argument("--db", default="sqlite:///./data/real.db")
    ap.add_argument("--hard-stop", type=float, default=0.08)
    ap.add_argument("--pullback-pct", type=float, default=0.03)
    args = ap.parse_args()

    engine = make_engine(args.db)
    repo = BaseRepository(engine)
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
    if not rt:
        print("⚠️ 实时行情拉取为空（非交易时段或接口限流）。盘后请用 build_action_plan.py。")

    print(f"# 盘中速查 — 实时价 / 落库基准 {dt}\n")

    # ── 持仓监控 ──
    print("## 持仓监控")
    print("| 名称 | 现价 | 涨跌 | 成本 | 浮盈亏 | 止损价 | MA20 | 状态 |")
    print("|---|---|---|---|---|---|---|---|")
    for _, h in holds.iterrows():
        c = h["code"]; q = rt.get(c, {}); lv = levels.get(c, {})
        px = q.get("price"); pre = q.get("pre_close"); cost = float(h["cost_price"] or 0)
        if not px:
            continue
        chg = (px / pre - 1) if pre else 0
        pnl = (px / cost - 1) if cost else 0
        stop = cost * (1 - args.hard_stop)
        ma20 = lv.get("ma20")
        if pnl <= -args.hard_stop:
            st = "⚠️ 已触发硬止损，清仓"
        elif ma20 and px < ma20:
            st = "破MA20，盘后确认清仓"
        elif pnl <= -args.hard_stop + 0.02:
            st = "逼近止损，盯紧"
        else:
            st = "持有"
        print(f"| {q.get('name', c)} | {px:.2f} | {chg:+.1%} | {cost:.2f} | {pnl:+.1%} | "
              f"{stop:.2f} | {ma20:.2f} | {st} |")

    # ── 观察池价位预警 ──
    print("\n## 观察池·买点价位预警")
    print("| 名称 | 级 | 现价 | 回踩位(MA20) | 突破位(20日高) | 状态 |")
    print("|---|---|---|---|---|---|")
    g_of = dict(zip(reco["code"], reco["grade"])) if not reco.empty else {}
    for c in watch:
        q = rt.get(c, {}); lv = levels.get(c, {})
        px = q.get("price")
        if not px or not lv:
            continue
        ma20, hi20 = lv.get("ma20"), lv.get("hi20")
        dev = (px / ma20 - 1) if ma20 else 0       # 距 MA20 偏离
        if not lv.get("ma60_up") or (lv.get("ma60") and px < lv["ma60"]):
            st = "趋势未上（不看）"
        elif ma20 and abs(dev) <= args.pullback_pct:
            st = "⚠️ 到回踩位，企稳放量则买点"      # 最佳买点（趋势中继）
        elif px >= hi20 and dev <= 0.06:
            st = "⚠️ 突破平台，放量则买点"          # 刚突破、未过度偏离
        elif dev > 0.06:
            st = f"偏离MA20 {dev:+.0%}，勿追高，等回踩"
        else:
            st = "上方运行，等回踩MA20"
        print(f"| {q.get('name', c)} | {g_of.get(c, '')} | {px:.2f} | "
              f"{ma20:.2f} | {hi20:.2f} | {st} |")


if __name__ == "__main__":
    main()
