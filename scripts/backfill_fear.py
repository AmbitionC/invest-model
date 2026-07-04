"""恐慌指数历史回填：为最近 N 个交易日逐日计算并落库 fear_daily。

日更任务每天只落库当日一个点，仪表盘曲线需要历史才有意义。本脚本用
``fear_gauge`` 逐个历史交易日重算（纯 EOD 数据、无未来函数），upsert 进
``fear_daily``，一次性把曲线补齐。之后每日任务继续追加当日点即可。

用法：
  python scripts/backfill_fear.py --days 90
  python scripts/backfill_fear.py --db mysql+pymysql://... --days 90
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.fear import fear_gauge  # noqa: E402
from scripts.fear_gauge import persist_fear  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="恐慌指数历史回填")
    ap.add_argument("--db", default=None, help="数据库 URL（默认走环境变量 make_engine）")
    ap.add_argument("--days", type=int, default=90, help="回填最近多少个交易日")
    ap.add_argument("--benchmark", default="000300.SH")
    args = ap.parse_args()

    engine = make_engine(args.db) if args.db else make_engine()
    repo = BaseRepository(engine)
    dates = repo.read_sql(
        "SELECT DISTINCT trade_date FROM stock_daily ORDER BY trade_date DESC LIMIT :n",
        {"n": args.days},
    )["trade_date"].tolist()
    # 最新日排前：即便超时中断，仪表盘徽标依赖的最新点也已优先落库
    dates = sorted((str(d) for d in dates), reverse=True)
    if not dates:
        print("stock_daily 无数据，无法回填。")
        return

    # 一次性批量载入覆盖全部目标日的窗口（stock 回看 200 天、index 回看 420 天），
    # 逐日在内存切片复用——把 N 次全市场查询压成 1 次，回填从十几分钟降到一两分钟。
    span_lo_stock = (pd.Timestamp(min(dates)) - pd.Timedelta(days=200)).strftime("%Y%m%d")
    span_lo_idx = (pd.Timestamp(min(dates)) - pd.Timedelta(days=420)).strftime("%Y%m%d")
    hi = max(dates)
    print(f"批量载入 stock_daily [{span_lo_stock}~{hi}] 与 index_daily [{span_lo_idx}~{hi}] …")
    stock_df = repo.read_sql(
        "SELECT code, trade_date, close, pct_chg FROM stock_daily "
        "WHERE trade_date>=:s AND trade_date<=:d",
        {"s": span_lo_stock, "d": hi})
    idx_df = repo.read_sql(
        "SELECT code, trade_date, close FROM index_daily "
        "WHERE trade_date>=:s AND trade_date<=:d",
        {"s": span_lo_idx, "d": hi})
    print(f"  stock_daily {len(stock_df)} 行 · index_daily {len(idx_df)} 行")

    ok = skip = 0
    for i, dt in enumerate(dates, 1):
        try:
            g = fear_gauge(engine, dt, benchmark=args.benchmark,
                           stock_df=stock_df, idx_df=idx_df)
            persist_fear(engine, g)
            ok += 1
        except Exception as e:  # noqa: BLE001 — 单日样本不足/缺基准，跳过不阻断
            skip += 1
            print(f"  跳过 {dt}: {repr(e)[:90]}")
        if i % 20 == 0 or i == len(dates):
            # 最新日优先：进度可见，即便超时中断也知道已覆盖到哪一天
            print(f"  进度 {i}/{len(dates)}（已落库至 {dt}，最新 {max(dates)} 已完成）", flush=True)
    print(f"✓ 恐慌指数回填完成：{ok} 天落库，{skip} 天跳过（范围 {min(dates)}~{max(dates)}）")


if __name__ == "__main__":
    main()
