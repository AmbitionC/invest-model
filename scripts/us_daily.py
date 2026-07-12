"""美股每日任务 CLI：数据更新 + 计划生成 + GitHub issue 推送。

  python scripts/us_daily.py [--skip-update] [--skip-plan] [--no-notify]

在 GitHub Actions（.github/workflows/us-update.yml）上按美股收盘后 cron 运行；
与 A 股 FC 链路零交集。环境变量：INVEST_DB_URL（生产 MySQL）/ GITHUB_TOKEN。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.us.plan import build_plan  # noqa: E402
from invest_model.us.update import run_update  # noqa: E402

_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    ap = argparse.ArgumentParser(description="美股每日：更新+计划+推送")
    ap.add_argument("--db", default=os.getenv("INVEST_DB_URL") or "sqlite:///./data/real.db")
    ap.add_argument("--watchlist", default=str(_ROOT / "config" / "us_watchlist.txt"))
    ap.add_argument("--skip-update", action="store_true")
    ap.add_argument("--skip-plan", action="store_true")
    ap.add_argument("--no-notify", action="store_true")
    ap.add_argument("--verify", action="store_true",
                    help="运行后回读库内状态发验证评论（ops观测通道，绕开同日去重）")
    args = ap.parse_args()

    engine = make_engine(args.db)
    out: dict = {}
    if not args.skip_update:
        out["update"] = run_update(engine, args.watchlist)
    if not args.skip_plan:
        res = build_plan(engine)
        out["plan"] = res.get("plan")
        md = res.get("markdown")
        if md and out.get("update") is not None:
            u = out["update"]
            md += (f"\n\n> 运行统计：日线+{u.get('us_stock_daily', 0)}行"
                   f"（最新 {u.get('max_trade_date', '?')}）｜估值锚 "
                   f"{u.get('us_valuation', 0)} 只｜基本面+{u.get('us_fundamental_q', 0)}行")
        if md and not args.no_notify:
            from faas import gh_notify
            day = res["plan"].split(":")[-1]
            r = gh_notify.post_issue_comment(
                "🇺🇸 美股每日计划",
                seed_body="本 issue 由 us-update workflow 每个美股交易日收盘后追加计划评论。",
                comment_body=md,
                dedupe_prefix=f"# 美股操作计划 — {day}")
            out["notify"] = r.get("reason")
        elif md:
            print(md)
    if args.verify:
        _post_verify(engine, out)
    print(f"[us_daily] {out}")


def _post_verify(engine, out: dict) -> None:
    """回读 us_* 关键状态 → 发验证评论（不受同日计划去重影响）。"""
    from datetime import datetime, timezone

    from faas import gh_notify
    from invest_model.repositories.base import BaseRepository
    repo = BaseRepository(engine)
    v = repo.read_sql(
        "SELECT verdict, COUNT(*) n, MAX(asof) asof FROM us_valuation "
        "WHERE asof=(SELECT MAX(asof) FROM us_valuation) GROUP BY verdict")
    cheap = repo.read_sql(
        "SELECT code, payback_years, anchor_price FROM us_valuation "
        "WHERE asof=(SELECT MAX(asof) FROM us_valuation) AND verdict='cheap' "
        "ORDER BY payback_years LIMIT 8")
    plan = repo.read_sql(
        "SELECT action, COUNT(*) n FROM us_action_plan "
        "WHERE plan_date=(SELECT MAX(plan_date) FROM us_action_plan) GROUP BY action")
    sample = repo.read_sql(
        "SELECT code, reason FROM us_action_plan "
        "WHERE plan_date=(SELECT MAX(plan_date) FROM us_action_plan) "
        "AND sleeve='satellite' AND reason LIKE '%US-V%' LIMIT 3")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d %H:%M UTC")
    lines = [f"## 验证 {ts}", "",
             f"运行结果：`{ {k: v for k, v in out.items() if k != 'markdown'} }`", "",
             "**估值锚分布**（最新 asof）：" + "；".join(
                 f"{r['verdict']}={int(r['n'])}" for _, r in v.iterrows()) if not v.empty
             else "**估值锚分布**：空（us_valuation 无数据 ⚠️）"]
    if not cheap.empty:
        lines.append("**cheap 档**：" + "；".join(
            f"{r['code']}(回本{float(r['payback_years']):.0f}年,锚${float(r['anchor_price']):.0f})"
            for _, r in cheap.iterrows()))
    if not plan.empty:
        lines.append("**最新计划动作分布**：" + "；".join(
            f"{r['action']}={int(r['n'])}" for _, r in plan.iterrows()))
    for _, r in sample.iterrows():
        lines.append(f"- {r['code']}: {str(r['reason'])[:130]}")
    gh_notify.post_issue_comment(
        "🇺🇸 美股每日计划",
        seed_body="本 issue 由 us-update workflow 每个美股交易日收盘后追加计划评论。",
        comment_body="\n".join(lines),
        dedupe_prefix=f"## 验证 {ts}")


if __name__ == "__main__":
    main()
