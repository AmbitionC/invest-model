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
from invest_model.portfolio import PortfolioConfig, RiskConfig  # noqa: E402
from invest_model.universe import UniverseConfig  # noqa: E402

logger = get_logger()


def _quarters(start: str, end: str) -> list[str]:
    import pandas as pd
    qs = pd.date_range(start, end, freq="QE")
    return [d.strftime("%Y%m%d") for d in qs]


def main() -> None:
    ap = argparse.ArgumentParser(description="截面多因子自主闭环")
    ap.add_argument("--mode", default="all",
                    choices=["update", "universe", "factors", "train", "predict",
                             "backtest", "all", "arb"],
                    help="arb=套利统一资金账本回测（需先 all/predict 出引擎B预测）")
    ap.add_argument("--db", default=None, help="sqlite:///./data/local.db 或留空走 .env 的 MySQL")
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="")
    ap.add_argument("--version", default="ic_v1")
    ap.add_argument("--rebalance", default="monthly", choices=["monthly", "biweekly"])
    ap.add_argument("--universe-method", default="alla")
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--max-weight", type=float, default=0.08)
    ap.add_argument("--scheme", default="inv_vol",
                    choices=["rank_weight", "equal", "score_weight", "inv_vol"],
                    help="组合加权：inv_vol=rank 权重×20日波动倒数（P4 已晋升为默认，2026-07-04）")
    ap.add_argument("--hold-buffer", type=float, default=1.5,
                    help="缓冲区换手抑制：已持有且排名在 top_n×此倍数内保留（P4 已晋升默认 1.5；0=关）")
    ap.add_argument("--benchmark", default="000300.SH")
    ap.add_argument("--ic-window", type=int, default=12)
    ap.add_argument("--ic-mode", default="icir", choices=["icir", "ic"])
    ap.add_argument("--model", default="ic", choices=["ic", "ranker"],
                    help="ic=多因子IC加权合成（默认）；ranker=截面ML排序（需 xgboost）")
    ap.add_argument("--no-timing", action="store_true", help="关闭指数择时（恒满仓）")
    # ── 风控 / 投顾融合 ──
    ap.add_argument("--risk", action="store_true", help="开启日频风控（硬止损+均线移动止盈）")
    ap.add_argument("--hard-stop", type=float, default=0.08, help="单票硬止损浮亏阈值")
    ap.add_argument("--account-dd-stop", type=float, default=0.15, help="账户级回撤止损阈值（0=关闭）")
    ap.add_argument("--no-ma-trailing", action="store_true", help="关闭均线移动止盈（只留硬止损）")
    ap.add_argument("--trail-full", action="store_true",
                    help="均线移动止盈用完整档位（破5减半/破10再减半/破20清仓）；默认仅破MA20清仓")
    ap.add_argument("--trend-filter", action="store_true", help="开启左侧趋势过滤（仅买 MA60 走平向上）")
    ap.add_argument("--advisor-led", action="store_true", help="投顾为主融合（A/B 推荐定仓 + 量化补充）")
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)

    risk = RiskConfig(
        enabled=args.risk,            # 仅控日频止损/止盈；趋势过滤由 trend_filter 独立开关
        hard_stop_pct=args.hard_stop,
        account_dd_stop=args.account_dd_stop,
        ma_trailing=not args.no_ma_trailing,
        trail_full=args.trail_full,
        trend_filter=args.trend_filter,
    )
    cfg = LoopConfig(
        start=args.start, end=args.end, version=args.version,
        rebalance=args.rebalance, benchmark=args.benchmark,
        ic_window=args.ic_window, ic_mode=args.ic_mode,
        model_kind=args.model,
        timing_enabled=not args.no_timing,
        risk=risk,
        universe=UniverseConfig(method=args.universe_method),
        portfolio=PortfolioConfig(top_n=args.top_n, max_weight=args.max_weight,
                                  scheme=args.scheme, hold_buffer=args.hold_buffer,
                                  advisor_led=args.advisor_led),
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
