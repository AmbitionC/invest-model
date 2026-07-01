"""数据库盘点：各表行数 + 关键日期范围（行情写到哪一天）。只读，用于状态/体检。

  python scripts/db_status.py            # 走 .env / INVEST_DB_URL
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data.engine import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

# 表 -> 日期列（None 表示无日期列，只报行数）
TABLES = {
    "trade_calendar": "cal_date",
    "stock_info": None,
    "stock_daily": "trade_date",
    "stock_fundamental": "trade_date",
    "stock_fina_indicator": "report_date",
    "index_daily": "trade_date",
    "universe_snapshot": "trade_date",
    "factor_exposure": "trade_date",
    "model_prediction": "trade_date",
    "portfolio_target": "trade_date",
    "backtest_run": None,
    "backtest_nav": "trade_date",
    "advisor_reco": "rec_date",
    "advisor_theme": "rec_date",
    "current_holding": None,
    "holding_snapshot": "snapshot_date",
    "account_snapshot": "snapshot_date",
}


def main() -> None:
    repo = BaseRepository(make_engine())
    print("== 数据库盘点 ==")
    print(f"{'表':<22}{'行数':>12}   {'最早':>10}  {'最新':>10}")
    print("-" * 60)
    for tbl, datecol in TABLES.items():
        if not repo.table_exists(tbl):
            print(f"{tbl:<22}{'(表不存在)':>12}")
            continue
        try:
            n = repo.get_row_count(tbl)
            if datecol and n > 0:
                d = repo.read_sql(
                    f"SELECT MIN(`{datecol}`) lo, MAX(`{datecol}`) hi FROM `{tbl}`")
                lo, hi = str(d["lo"].iloc[0]), str(d["hi"].iloc[0])
                print(f"{tbl:<22}{n:>12,}   {lo:>10}  {hi:>10}")
            else:
                print(f"{tbl:<22}{n:>12,}")
        except Exception as e:  # noqa: BLE001
            print(f"{tbl:<22}  ERROR: {e}")

    # 行情写到哪一天（重点）
    try:
        d = repo.read_sql(
            "SELECT MAX(trade_date) hi, COUNT(DISTINCT trade_date) days FROM stock_daily")
        print("-" * 60)
        print(f"行情 stock_daily 最新交易日: {d['hi'].iloc[0]}  （共 {d['days'].iloc[0]} 个交易日）")
    except Exception:  # noqa: BLE001
        pass


if __name__ == "__main__":
    main()
