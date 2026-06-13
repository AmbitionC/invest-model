#!/usr/bin/env python3
"""一键执行：数据采集 → 信号回填 → 回测 → 输出报告。

用法：
    # 完整流程（采集 + 回填 + 回测）
    python3 scripts/run_collect_and_backtest.py

    # 只跑回测（跳过数据采集）
    python3 scripts/run_collect_and_backtest.py --backtest-only

    # 指定回测区间
    python3 scripts/run_collect_and_backtest.py --start 20230101 --end 20260613

运行前请确认：
    1. .env 中的 TUSHARE_TOKEN 和 MySQL 连接信息已配置
    2. 股票池已更新（scripts/update_stock_pool.py）
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from invest_model.db import get_engine
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.logger import get_logger

logger = get_logger()

# 新加入的标的（需要历史回填）
NEW_CODES = ["000510.SZ", "002851.SZ", "300750.SZ", "600118.SH", "600160.SH"]
HISTORY_START = "20210101"
TODAY = datetime.now().strftime("%Y%m%d")


def collect_data(engine, source, pool_repo):
    """Step 1: 数据采集（增量 + 新标的历史回填）"""
    from invest_model.pipeline.daily_pipeline import DailyPipeline

    print("\n" + "="*50)
    print("Step 1: 增量数据采集（现有标的 → 今日）")
    print("="*50)
    pipeline = DailyPipeline()
    results = pipeline.run("full")
    for step, status in results.items():
        icon = "✅" if "成功" in str(status) or "OK" in str(status) else "⚠️"
        print(f"  {icon} {step}: {status}")

    print("\n" + "="*50)
    print(f"Step 2: 新标的历史回填 {HISTORY_START} → {TODAY}")
    print("="*50)

    # 仅回填真正新加入的标的
    existing_dates = {}
    from invest_model.repositories.stock_daily_repo import StockDailyRepository
    daily_repo = StockDailyRepository(engine)
    for code in NEW_CODES:
        latest = daily_repo.get_latest_date(code=code)
        if latest and latest >= TODAY:
            print(f"  ⏭️  {code} 数据已是最新，跳过")
        else:
            existing_dates[code] = latest

    codes_to_fill = list(existing_dates.keys())
    if codes_to_fill:
        print(f"  需要回填: {codes_to_fill}")
        _backfill_collectors(source, engine, codes_to_fill)
    else:
        print("  所有新标的数据均已完整")


def _backfill_collectors(source, engine, codes):
    from invest_model.collectors.stock_daily_collector import StockDailyCollector
    from invest_model.collectors.technical_collector import TechnicalCollector
    from invest_model.collectors.fundamental_collector import FundamentalCollector

    collectors = [
        ("日线数据", StockDailyCollector(source, engine)),
        ("技术指标", TechnicalCollector(source, engine)),
        ("基本面数据", FundamentalCollector(source, engine)),
    ]
    for name, collector in collectors:
        print(f"  回填 {name}...")
        try:
            collector.collect_history(codes, HISTORY_START)
            print(f"    ✅ {name} 回填完成")
        except Exception as e:
            print(f"    ⚠️  {name} 回填异常: {e}")

    # 资金流和融资数据（可选）
    try:
        from invest_model.collectors.cashflow_collector import CashflowCollector
        CashflowCollector(source, engine).collect_history(codes, HISTORY_START)
        print("    ✅ 资金流数据回填完成")
    except Exception:
        pass

    try:
        from invest_model.collectors.market_collector import MarginCollector
        MarginCollector(source, engine).collect_history(codes, HISTORY_START)
        print("    ✅ 融资融券数据回填完成")
    except Exception:
        pass


def backfill_signals(engine, pool_repo, start_date, end_date):
    """Step 3: 信号回填"""
    from invest_model.scoring.scorer import CompositeScorer

    all_codes = pool_repo.get_pool_codes("core") + pool_repo.get_pool_codes("etf")
    print("\n" + "="*50)
    print(f"Step 3: 信号回填 {start_date} ~ {end_date}")
    print(f"  标的: {all_codes}")
    print("="*50)

    scorer = CompositeScorer(engine)
    total = scorer.backfill_history(
        codes=all_codes,
        start_date=start_date,
        end_date=end_date,
        skip_existing=True,
        persist=True,
    )
    print(f"  ✅ 信号回填完成，共 {total} 条记录")


def run_backtest(engine, pool_repo, start_date, end_date):
    """Step 4: 回测执行"""
    from invest_model.backtest.runner import BacktestRunner

    core_codes = pool_repo.get_pool_codes("core")
    etf_codes = pool_repo.get_pool_codes("etf")
    all_codes = core_codes + etf_codes

    print("\n" + "="*50)
    print(f"Step 4: 运行回测 {start_date} ~ {end_date}")
    print(f"  标的: {all_codes}")
    print("="*50)

    runner = BacktestRunner(engine, commission=0.001, slippage=0.0005)
    run_name = f"core_pool_v3_{start_date[:4]}-{end_date[:6]}"
    run_id = runner.run(
        name=run_name,
        codes=all_codes,
        start_date=start_date,
        end_date=end_date,
        rebalance_days=5,
        top_k=None,
    )

    print(f"\n  ✅ 回测完成 run_id={run_id}")
    runner.print_report(run_id)
    return run_id


def parse_args():
    parser = argparse.ArgumentParser(description="数据采集 + 回测")
    parser.add_argument("--backtest-only", action="store_true", help="跳过数据采集，直接运行回测")
    parser.add_argument("--start", default="20220101", help="回测开始日期（默认20220101）")
    parser.add_argument("--end", default=TODAY, help="回测结束日期（默认今日）")
    return parser.parse_args()


def main():
    args = parse_args()
    engine = get_engine()
    pool_repo = StockPoolRepository(engine)

    if not args.backtest_only:
        from invest_model.sources.tushare_client import TushareClient
        source = TushareClient()
        collect_data(engine, source, pool_repo)
        backfill_signals(engine, pool_repo, HISTORY_START, args.end)
    else:
        print("⏭️  跳过数据采集，直接运行回测")

    run_backtest(engine, pool_repo, args.start, args.end)


if __name__ == "__main__":
    main()
