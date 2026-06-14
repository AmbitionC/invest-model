"""从 Tushare 拉真实全 A 数据灌入本地 SQLite，用于真实数据回测验证。

需可访问 Tushare（.env 配好 TUSHARE_TOKEN；若在受限网络，需把 api.tushare.pro
加入出口白名单）。拉取：交易日历 / 股票列表 / 全市场日线 / 全市场估值 /
季度财务（VIP）/ 基准指数。

用法：
    python scripts/fetch_local_sample.py --db sqlite:///./data/real.db \
        --start 20210101 --end 20260613
然后：
    python scripts/run_pipeline.py --mode all --db sqlite:///./data/real.db \
        --start 20210101 --end 20260613
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.logger import get_logger  # noqa: E402
from invest_model.orchestration.update import run_data_update  # noqa: E402

logger = get_logger()


def _quarters(start: str, end: str) -> list[str]:
    import pandas as pd
    return [d.strftime("%Y%m%d") for d in pd.date_range(start, end, freq="QE")]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="sqlite:///./data/real.db")
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default=datetime.now().strftime("%Y%m%d"))
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)
    try:
        stats = run_data_update(engine, args.start, args.end, _quarters(args.start, args.end))
    except Exception as e:  # noqa: BLE001
        logger.error(
            f"拉取失败：{e}\n"
            "若为 'Host not in allowlist' / 连接被拒，需在运行环境的网络出口设置中"
            "放行 api.tushare.pro；或在本地（可访问 Tushare 的机器）运行本脚本。"
        )
        sys.exit(1)
    logger.info(f"完成：{stats}")


if __name__ == "__main__":
    main()
