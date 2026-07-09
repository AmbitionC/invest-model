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

    raw = pd.read_csv(args.csv, dtype={"code": str, "snapshot_date": str, "entry_date": str})
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
    # 当天先删后插：让单日快照成为权威（修代码/删持仓都能干净覆盖）
    repo.execute_sql("DELETE FROM holding_snapshot WHERE snapshot_date=:d", {"d": snap_date})
    n = repo.upsert("holding_snapshot", df, ["snapshot_date", "code"])

    # 同步刷新 current_holding（实盘持仓表，供 build_action_plan/盯盘）：
    # 取 stock + etf（ETF 前复权日线已由 ingest_etf_daily 灌进 stock_daily，可同套风控）；
    # 排除现金/转债（无日线、不做 MA 风控）。全量替换=当前持仓。含 entry_date。
    kinds = raw["asset_type"].astype(str).str.lower()
    pos = raw[kinds.isin(["stock", "etf"])].copy()
    if not pos.empty:
        pos["shares"] = pd.to_numeric(pos["shares"], errors="coerce")
        pos["cost_price"] = pd.to_numeric(pos["cost_price"], errors="coerce")
        # entry_date 三级回退：CSV 显式值 > 旧 current_holding 值 > 快照日。
        # 券商快照对 ETF 常不带建仓日，此前每次上传都被重置为快照日 →
        # 时间止损/盈利保护的持有期窗口坍缩为 1 天、对 ETF 永不生效。
        try:
            old = repo.read_sql("SELECT code, entry_date FROM current_holding")
            prev_entry = {str(r["code"]): str(r["entry_date"])
                          for _, r in old.iterrows()
                          if pd.notna(r["entry_date"]) and str(r["entry_date"]).strip()}
        except Exception:  # noqa: BLE001  首次运行无表等，回退快照日
            prev_entry = {}
        if "entry_date" not in pos.columns:
            pos["entry_date"] = None
        pos["entry_date"] = pos.apply(
            lambda r: str(r["entry_date"]).strip()
            if pd.notna(r["entry_date"]) and str(r["entry_date"]).strip()
            else (prev_entry.get(str(r["code"])) or snap_date), axis=1)
        ch = pos[["code", "shares", "cost_price", "entry_date"]].copy()
        repo.execute_sql("DELETE FROM current_holding")
        m = repo.upsert("current_holding", ch, ["code"])
        print(f"current_holding 刷新 {m} 只（stock+etf）")

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
