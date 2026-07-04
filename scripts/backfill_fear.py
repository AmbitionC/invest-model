"""恐慌指数历史回填：为最近 N 个交易日逐日计算并落库 fear_daily。

日更任务每天只落库当日一个点，仪表盘曲线需要历史才有意义。本脚本用
``fear_gauge`` 逐个历史交易日重算（纯 EOD 数据、无未来函数），upsert 进
``fear_daily``，一次性把曲线补齐。之后每日任务继续追加当日点即可。

用法：
  python scripts/backfill_fear.py --days 120
  python scripts/backfill_fear.py --db mysql+pymysql://... --days 120
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.fear import fear_gauge  # noqa: E402
from scripts.fear_gauge import persist_fear  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="恐慌指数历史回填")
    ap.add_argument("--db", default=None, help="数据库 URL（默认走环境变量 make_engine）")
    ap.add_argument("--days", type=int, default=120, help="回填最近多少个交易日")
    ap.add_argument("--benchmark", default="000300.SH")
    args = ap.parse_args()

    engine = make_engine(args.db) if args.db else make_engine()
    repo = BaseRepository(engine)
    dates = repo.read_sql(
        "SELECT DISTINCT trade_date FROM stock_daily ORDER BY trade_date DESC LIMIT :n",
        {"n": args.days},
    )["trade_date"].tolist()
    dates = sorted(str(d) for d in dates)
    if not dates:
        print("stock_daily 无数据，无法回填。")
        return

    ok = skip = 0
    for dt in dates:
        try:
            g = fear_gauge(engine, dt, benchmark=args.benchmark)
            persist_fear(engine, g)
            ok += 1
        except Exception as e:  # noqa: BLE001 — 单日样本不足/缺基准，跳过不阻断
            skip += 1
            print(f"  跳过 {dt}: {repr(e)[:90]}")
    print(f"✓ 恐慌指数回填完成：{ok} 天落库，{skip} 天跳过（范围 {dates[0]}~{dates[-1]}）")


if __name__ == "__main__":
    main()
