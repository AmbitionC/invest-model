"""统一 CLI：截面多因子自主闭环。

示例：
  # 本地验证（合成数据已灌入 SQLite）
  python scripts/run_pipeline.py --mode all --db sqlite:///./data/local.db \
      --start 20210101 --end 20260613

  # 生产（MySQL，从 .env 读取连接）：先更新数据，再跑全流程
  python scripts/run_pipeline.py --mode update --start 20190101
  python scripts/run_pipeline.py --mode all
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.logger import get_logger  # noqa: E402
from invest_model.orchestration import ClosedLoop, LoopConfig  # noqa: E402
from invest_model.portfolio import PortfolioConfig  # noqa: E402
from invest_model.universe import UniverseConfig  # noqa: E402

logger = get_logger()


def _quarters(start: str, end: str) -> list[str]:
    import pandas as pd
    qs = pd.date_range(start, end, freq="QE")
    return [d.strftime("%Y%m%d") for d in qs]


def main() -> None:
    ap = argparse.ArgumentParser(description="截面多因子自主闭环")
    ap.add_argument("--mode", default="all",
                    choices=["update", "universe", "factors", "train", "predict", "backtest", "all"])
    ap.add_argument("--db", default=None, help="sqlite:///./data/local.db 或留空走 .env 的 MySQL")
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="")
    ap.add_argument("--version", default="ic_v1")
    ap.add_argument("--rebalance", default="monthly", choices=["monthly", "biweekly"])
    ap.add_argument("--universe-method", default="alla")
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--max-weight", type=float, default=0.08)
    ap.add_argument("--benchmark", default="000300.SH")
    ap.add_argument("--ic-window", type=int, default=12)
    ap.add_argument("--ic-mode", default="icir", choices=["icir", "ic"])
    ap.add_argument("--no-timing", action="store_true", help="关闭指数择时（恒满仓）")
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)

    cfg = LoopConfig(
        start=args.start, end=args.end, version=args.version,
        rebalance=args.rebalance, benchmark=args.benchmark,
        ic_window=args.ic_window, ic_mode=args.ic_mode,
        timing_enabled=not args.no_timing,
        universe=UniverseConfig(method=args.universe_method),
        portfolio=PortfolioConfig(top_n=args.top_n, max_weight=args.max_weight),
    )

    if args.mode == "update":
        from invest_model.orchestration.update import run_data_update
        end = args.end or __import__("datetime").datetime.now().strftime("%Y%m%d")
        try:
            run_data_update(engine, args.start, end, quarters=_quarters(args.start, end))
        except Exception as e:  # noqa: BLE001
            logger.error(f"数据更新失败（通常因无法访问 Tushare）：{e}")
            sys.exit(1)
        return

    loop = ClosedLoop(engine, cfg)
    metrics = loop.run(args.mode)
    if metrics:
        print("\n=== 回测指标 ===")
        for k, v in metrics.items():
            print(f"  {k:26s}: {v}")


if __name__ == "__main__":
    main()
