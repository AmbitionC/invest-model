"""当前持仓录入：把 CSV（code, shares, cost_price, entry_date）upsert 进 current_holding。

示例：
  python scripts/ingest_holding.py --db sqlite:///./data/real.db \
      --csv config/holding_template.csv [--replace]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.holding_repo import HoldingRepo  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="当前持仓录入")
    ap.add_argument("--db", default=None)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--replace", action="store_true", help="先清空再写入（全量同步）")
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)
    df = pd.read_csv(args.csv, dtype={"code": str, "entry_date": str})
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce")
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")

    repo = HoldingRepo(engine)
    if args.replace:
        repo.clear()
    n = repo.save(df[["code", "shares", "cost_price", "entry_date"]])
    print(f"已录入 {n} 条持仓。")


if __name__ == "__main__":
    main()
