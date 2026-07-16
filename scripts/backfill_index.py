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
# E10 底仓验证：中证红利低波(H30269) + 中证红利(000922)——价格指数，权限不足时
# get_index_daily 返回空并 WARN，E10 会如实降级"数据不足不判定"
# E9v2/E12 多周期回测：补宽基（上证综指/深证成指/上证50/中证1000/科创50）扩样本；
# 权限不足者 get_index_daily 空返回 WARN、下游按"无数据"跳过，不影响其它指数
BROAD_EXTRA = ["000001.SH", "399001.SZ", "000016.SH", "000852.SH", "000688.SH"]
# P19/高低切监控 + 重远兑现统计复核：大金融行业指数（券商/银行/保险）——防守腿
# 候选与"高低切是否在发生"的观察口径；权限不足同样空返回 WARN 不影响其它
SECTOR_DEFENSE = ["399975.SZ", "399986.SZ", "399809.SZ"]
DEFAULT_CODES = [*BENCHMARKS, "399006.SZ", "H30269.CSI", "000922.CSI",
                 *BROAD_EXTRA, *SECTOR_DEFENSE]


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
