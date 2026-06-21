"""生成实盘操作计划（结合当前持仓 + 投顾信号 + 量化目标 + 风控）。

示例：
  python scripts/build_action_plan.py --db sqlite:///./data/real.db \
      --advisor-led --risk --cash 0 --out results/action_plan.md
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.orchestration.action_plan import build_action_plan  # noqa: E402
from invest_model.orchestration import LoopConfig  # noqa: E402
from invest_model.portfolio import PortfolioConfig, RiskConfig  # noqa: E402
from invest_model.universe import UniverseConfig  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="生成实盘操作计划")
    ap.add_argument("--db", default=None)
    ap.add_argument("--date", default=None, help="决策日 YYYYMMDD（默认最新数据日）")
    ap.add_argument("--cash", type=float, default=0.0, help="账户现金（折算总权益/股数）")
    ap.add_argument("--version", default="ic_v1")
    ap.add_argument("--universe-method", default="alla")
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--max-weight", type=float, default=0.08)
    ap.add_argument("--benchmark", default="000300.SH")
    ap.add_argument("--advisor-led", action="store_true")
    ap.add_argument("--risk", action="store_true")
    ap.add_argument("--hard-stop", type=float, default=0.08)
    ap.add_argument("--account-dd-stop", type=float, default=0.15)
    ap.add_argument("--no-ma-trailing", action="store_true")
    ap.add_argument("--trail-full", action="store_true")
    ap.add_argument("--trend-filter", action="store_true")
    ap.add_argument("--no-timing", action="store_true")
    ap.add_argument("--out", default=None, help="输出 Markdown 路径（默认仅打印）")
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)

    cfg = LoopConfig(
        version=args.version, benchmark=args.benchmark,
        timing_enabled=not args.no_timing,
        risk=RiskConfig(enabled=args.risk, hard_stop_pct=args.hard_stop,
                        account_dd_stop=args.account_dd_stop,
                        ma_trailing=not args.no_ma_trailing, trail_full=args.trail_full,
                        trend_filter=args.trend_filter),
        universe=UniverseConfig(method=args.universe_method),
        portfolio=PortfolioConfig(top_n=args.top_n, max_weight=args.max_weight,
                                  advisor_led=args.advisor_led),
    )
    plan = build_action_plan(engine, cfg, dt=args.date, cash=args.cash)
    md = plan.to_markdown()
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")
        print(f"\n已写入 {args.out}")


if __name__ == "__main__":
    main()
