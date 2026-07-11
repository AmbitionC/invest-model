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
    args = ap.parse_args()

    engine = make_engine(args.db)
    out: dict = {}
    if not args.skip_update:
        out["update"] = run_update(engine, args.watchlist)
    if not args.skip_plan:
        res = build_plan(engine)
        out["plan"] = res.get("plan")
        md = res.get("markdown")
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
    print(f"[us_daily] {out}")


if __name__ == "__main__":
    main()
