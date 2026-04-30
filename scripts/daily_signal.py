#!/usr/bin/env python3
"""每日信号报告 CLI。

用法:
    python scripts/daily_signal.py                           # core 池今日
    python scripts/daily_signal.py --pool core --date 20260430
    python scripts/daily_signal.py --save                    # 同时写入数据库
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from invest_model.db import get_engine
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.advisor import StockAdvisor
from invest_model.advisor.persistence import save_advisor_signals


ACTION_COLORS = {
    "强买": "\033[1;32m",  # bold green
    "买入": "\033[32m",    # green
    "观望": "\033[0m",     # default
    "减仓": "\033[33m",    # yellow
    "清仓": "\033[1;31m",  # bold red
}
RESET = "\033[0m"


def main():
    parser = argparse.ArgumentParser(description="每日信号报告")
    parser.add_argument("--pool", default="core", help="股票池名称 (default: core)")
    parser.add_argument("--date", default=None, help="交易日 YYYYMMDD (default: 最新)")
    parser.add_argument("--save", action="store_true", help="同时写入数据库")
    args = parser.parse_args()

    engine = get_engine()
    pool_repo = StockPoolRepository(engine)

    if args.pool == "all":
        groups = pool_repo.get_pool_groups()
        import pandas as _pd
        frames = [pool_repo.get_pool(g) for g in groups]
        pool_df = _pd.concat(frames, ignore_index=True) if frames else _pd.DataFrame()
    else:
        pool_df = pool_repo.get_pool(args.pool)

    if pool_df.empty:
        print(f"[!] 股票池 '{args.pool}' 为空")
        return

    codes = pool_df["code"].tolist()
    code_name_map = dict(zip(pool_df["code"], pool_df["name"]))

    if args.date:
        trade_date = args.date
    else:
        from invest_model.repositories.etf_repo import ETFRepository
        daily_repo = StockDailyRepository(engine)
        etf_repo = ETFRepository(engine)
        latest_dates = []
        for c in codes:
            d = daily_repo.get_latest_date(code=c)
            if not d:
                d = etf_repo.get_latest_date(c)
            if d:
                latest_dates.append(d)
        if not latest_dates:
            print("[!] 无日线数据")
            return
        trade_date = max(latest_dates)

    print(f"\n{'='*70}")
    print(f"  {trade_date} 每日信号报告  |  池: {args.pool} ({len(codes)} 只)")
    print(f"{'='*70}\n")

    advisor = StockAdvisor(engine)
    signals = advisor.advise_batch(codes, trade_date, code_name_map)

    if not signals:
        print("[!] 无信号输出")
        return

    # 表头
    print(f"{'代码':12s} {'名称':10s} {'操作':6s} {'置信度':6s} {'仓位%':6s} "
          f"{'综合':8s} {'技术':8s} {'资金流':8s} {'触发规则'}")
    print("-" * 90)

    for s in signals:
        color = ACTION_COLORS.get(s.action_cn, "")
        triggers_str = ", ".join(s.triggers) if s.triggers else "无"
        pos_str = f"{s.position_pct:.0%}" if s.position_pct else "0%"
        print(
            f"{color}"
            f"{s.code:12s} {s.name:10s} {s.action_cn:6s} {s.confidence:5d}  "
            f"{pos_str:>5s}  {s.composite:+7.3f} "
            f"{s.sub_scores.get('tech_score', 0):+7.3f} "
            f"{s.sub_scores.get('flow_score', 0):+7.3f}  "
            f"{triggers_str}"
            f"{RESET}"
        )

    print(f"\n{'─'*70}")
    print("逐票归因:")
    print(f"{'─'*70}")
    for s in signals:
        tag = s.action_cn
        print(f"\n  [{tag}] {s.code} {s.name} (置信度 {s.confidence})")
        print(f"    {s.attribution}")

    if args.save:
        n = save_advisor_signals(engine, signals)
        print(f"\n[DB] 已保存 {n} 条顾问信号")


if __name__ == "__main__":
    main()
