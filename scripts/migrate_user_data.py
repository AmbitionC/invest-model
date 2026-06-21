"""把用户数据（投顾信号 + 当前持仓）从一个库搬到另一个库（如 SQLite→生产 MySQL）。

市场数据/预测由数据更新管线(--mode all)在目标库自动生成，这里只搬"人工录入"的三张表：
  advisor_reco / advisor_theme / current_holding。幂等可重复跑。

示例（在能连到 MySQL 的机器上）：
  export INVEST_DB_URL='mysql+pymysql://user:pass@host:3306/invest?charset=utf8mb4'
  python scripts/migrate_user_data.py --src sqlite:///./data/real.db
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

TABLES = {
    "advisor_reco": ["rec_date", "code", "source_type"],
    "advisor_theme": ["rec_date", "theme", "source_type"],
    "current_holding": ["code"],
}


def main() -> None:
    ap = argparse.ArgumentParser(description="迁移投顾信号+持仓到目标库")
    ap.add_argument("--src", required=True, help="源库 URL（如 sqlite:///./data/real.db）")
    ap.add_argument("--dst", default=None, help="目标库 URL（默认走 INVEST_DB_URL / .env 的 MySQL）")
    args = ap.parse_args()

    src = BaseRepository(make_engine(args.src))
    dst_engine = make_engine(args.dst)
    create_schema(dst_engine)
    dst = BaseRepository(dst_engine)

    for table, keys in TABLES.items():
        if not src.table_exists(table):
            print(f"  跳过 {table}（源库无此表）")
            continue
        df = src.read_sql(f"SELECT * FROM {table}")
        if "created_at" in df.columns:
            df = df.drop(columns=["created_at"])
        if "updated_at" in df.columns:
            df = df.drop(columns=["updated_at"])
        n = dst.upsert(table, df, keys) if not df.empty else 0
        print(f"  {table}: 迁移 {n} 行")
    print("完成。后续在目标库跑 `run_pipeline.py --mode all` 生成 universe/因子/预测。")


if __name__ == "__main__":
    main()
