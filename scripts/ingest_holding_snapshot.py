"""每日持仓快照录入：把券商持仓 CSV + 现金写入 holding_snapshot / account_snapshot。

CSV 列（缺 market_value/pnl 会自动算）：
  snapshot_date,code,name,asset_type,shares,available,cost_price,last_price[,market_value,pnl,pnl_pct]

示例：
  python scripts/ingest_holding_snapshot.py --csv config/holding_snapshot_20260630.csv --cash 1061
不依赖 stock_info / Tushare，仅写 DB（ETF/转债等非 A 股标的也可记）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="每日持仓+现金快照录入")
    ap.add_argument("--db", default=None)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--cash", type=float, default=None,
                    help="账户现金余额；不传则从 CSV 中 asset_type=cash 的行读取")
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)
    repo = BaseRepository(engine)

    raw = pd.read_csv(args.csv, dtype={"code": str, "snapshot_date": str})
    # 分离现金行（asset_type=cash），其余为持仓
    is_cash = raw.get("asset_type", "").astype(str).str.lower() == "cash"
    cash_rows = raw[is_cash]
    df = raw[~is_cash].copy()
    for c in ["shares", "available", "cost_price", "last_price", "market_value", "pnl", "pnl_pct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if args.cash is not None:
        cash = float(args.cash)
    elif not cash_rows.empty:
        cash = float(pd.to_numeric(cash_rows["market_value"], errors="coerce").sum())
    else:
        cash = 0.0
    # 缺失列自动补算
    if "market_value" not in df.columns:
        df["market_value"] = (df["shares"] * df["last_price"]).round(2)
    if "pnl" not in df.columns:
        df["pnl"] = ((df["last_price"] - df["cost_price"]) * df["shares"]).round(2)
    if "pnl_pct" not in df.columns:
        df["pnl_pct"] = ((df["last_price"] / df["cost_price"] - 1) * 100).round(4)
    if "available" not in df.columns:
        df["available"] = df["shares"]

    snap_date = str(df["snapshot_date"].iloc[0])
    cols = ["snapshot_date", "code", "name", "asset_type", "shares", "available",
            "cost_price", "last_price", "market_value", "pnl", "pnl_pct"]
    df = df[[c for c in cols if c in df.columns]]
    n = repo.upsert("holding_snapshot", df, ["snapshot_date", "code"])

    mv = float(df["market_value"].sum())
    acct = pd.DataFrame([{
        "snapshot_date": snap_date,
        "cash": round(cash, 2),
        "market_value": round(mv, 2),
        "total_asset": round(mv + cash, 2),
    }])
    repo.upsert("account_snapshot", acct, ["snapshot_date"])

    print(f"快照 {snap_date}: 持仓 {n} 行；现金 {cash:.2f}；"
          f"总市值 {mv:.2f}；总资产 {mv + cash:.2f}")


if __name__ == "__main__":
    main()
