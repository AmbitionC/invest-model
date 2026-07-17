"""重算指定日的 account_snapshot（2026-07-17 事故修复：0716 行用了 0715 ETF 收盘偏高 5,184）。

流程：① 取证——打印该日 ETF 行情行的 created_at（确认行情迟到时间线）；
② 删除该日 account_snapshot（仅机器重估行；防误删加 --date 显式参数）；
③ 调 faas.jobs._persist_account_snapshot_daily 补洞式重写（自愈逻辑会用当前库内
   最新可用收盘重算该日）；④ 打印新行核对。

用法（Actions）：python scripts/redo_account_snapshot.py --date 20260716
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="重算指定日 account_snapshot")
    ap.add_argument("--date", required=True)
    args = ap.parse_args()
    d = args.date
    engine = make_engine()
    repo = BaseRepository(engine)

    # 生产 stock_daily 无 created_at 列（建表早于该字段），取证只看行是否存在
    for_df = repo.read_sql(
        "SELECT code, trade_date, close FROM stock_daily "
        "WHERE trade_date=:d AND code IN ('515050.SH','516120.SH','588010.SH')", {"d": d})
    print(f"取证：{d} ETF 行情行")
    print(for_df.to_string(index=False) if not for_df.empty else "（无行）")

    old = repo.read_sql(
        "SELECT snapshot_date, cash, market_value, total_asset FROM account_snapshot "
        "WHERE snapshot_date=:d", {"d": d})
    print(f"\n旧行：\n{old.to_string(index=False) if not old.empty else '（无）'}")

    with engine.begin() as conn:
        n = conn.execute(text("DELETE FROM account_snapshot WHERE snapshot_date=:d"),
                         {"d": d}).rowcount
    print(f"\n已删除 {n} 行（snapshot_date={d}）")

    from faas.jobs import _persist_account_snapshot_daily
    res = _persist_account_snapshot_daily()
    print(f"重估返回：{res}")

    new = repo.read_sql(
        "SELECT snapshot_date, cash, market_value, total_asset FROM account_snapshot "
        "WHERE snapshot_date=:d", {"d": d})
    print(f"\n新行：\n{new.to_string(index=False) if not new.empty else '（未重建！）'}")


if __name__ == "__main__":
    main()
