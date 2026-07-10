"""指数日线历史回填：为 E9（P12 指数 MA60 事件闸）等验证补齐 index_daily 深度。

背景：日常 update 只从 PIPELINE_START（2023）拉指数，趋势市"有效跌破 MA60"
事件稀疏，E9 功效不足。本脚本只回填 index_daily（几个指数 × 十年 ≈ 数万行，
分钟级），不动个股数据。幂等 upsert，可重复跑。

  python scripts/backfill_index.py --start 20150101
  python scripts/backfill_index.py --codes 000300.SH,399006.SZ --start 20100101
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.orchestration.update import BENCHMARKS  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402

# E9 双指数：基准三兄弟 + 创业板指（重远 60 日线论述的主对象）
DEFAULT_CODES = [*BENCHMARKS, "399006.SZ"]


def main() -> None:
    ap = argparse.ArgumentParser(description="index_daily 历史回填（只动指数，不动个股）")
    ap.add_argument("--db", default=None)
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--end", default=datetime.now(timezone.utc).strftime("%Y%m%d"))
    ap.add_argument("--codes", default=",".join(DEFAULT_CODES),
                    help="逗号分隔指数代码，默认基准三指数+创业板指")
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)
    repo = BaseRepository(engine)
    client = TushareClient()
    total = 0
    for code in [c.strip() for c in args.codes.split(",") if c.strip()]:
        df = client.get_index_daily(code, args.start, args.end)
        if df.empty:
            print(f"WARN {code} 无数据（{args.start}~{args.end}）")
            continue
        n = repo.upsert("index_daily", df, ["code", "trade_date"])
        span = f"{df['trade_date'].min()}~{df['trade_date'].max()}"
        print(f"{code}: upsert {n} 行（{span}）")
        total += n
    print(f"合计 {total} 行")


if __name__ == "__main__":
    main()
