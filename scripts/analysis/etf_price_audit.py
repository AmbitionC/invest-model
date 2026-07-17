"""ETF 价格审计（2026-07-17 用户质疑通信ETF价格·只读）。

对持仓三只 ETF：并排打印 库内 stock_daily（前复权归一后）近 12 日 vs
tushare fund_daily 原始近 12 日 vs fund_adj 因子 vs fund_div 分红记录 vs
券商快照价——定位价格差异来自 行情源错误/复权归一错误/分红除息/纯下跌。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402

CODES = ["515050.SH", "516120.SH", "588010.SH"]


def main() -> None:
    repo = BaseRepository(make_engine())
    cli = TushareClient()
    for code in CODES:
        print(f"\n{'=' * 30} {code} {'=' * 30}")
        db = repo.read_sql(
            "SELECT trade_date, close FROM stock_daily WHERE code=:c "
            "ORDER BY trade_date DESC LIMIT 12", {"c": code}).sort_values("trade_date")
        try:
            raw = cli.get_etf_daily(code, "20260701", "20260717")
        except Exception as e:  # noqa: BLE001
            print(f"WARN 原始 fund_daily 拉取失败：{e}")
            raw = None
        print("库内(复权归一) vs 原始 fund_daily：")
        rawmap = {}
        if raw is not None and not raw.empty:
            rawmap = dict(zip(raw["trade_date"].astype(str),
                              pd.to_numeric(raw["close"], errors="coerce")))
        for _, r in db.iterrows():
            d = str(r["trade_date"])
            rv = rawmap.get(d)
            diff = "" if rv is None else f"  diff={float(r['close']) - float(rv):+.4f}"
            print(f"  {d}  库内={float(r['close']):.4f}  原始={rv if rv is None else f'{float(rv):.4f}'}{diff}")
        try:
            adj = cli.pro.query("fund_adj", ts_code=code,
                                start_date="20260601", end_date="20260717")
            if adj is not None and len(adj):
                a = adj.sort_values("trade_date")
                uniq = a.drop_duplicates("adj_factor")
                print(f"fund_adj 6月以来因子变化点：\n{uniq[['trade_date','adj_factor']].to_string(index=False)}")
            else:
                print("fund_adj: 无记录")
        except Exception as e:  # noqa: BLE001
            print(f"WARN fund_adj 失败：{e}")
        try:
            div = cli.pro.query("fund_div", ts_code=code)
            if div is not None and len(div):
                cols = [c for c in ("ann_date", "ex_date", "div_cash", "base_date") if c in div.columns]
                print(f"fund_div 分红记录：\n{div[cols].head(8).to_string(index=False)}")
            else:
                print("fund_div: 无分红记录")
        except Exception as e:  # noqa: BLE001
            print(f"WARN fund_div 失败：{e}")
        snap = repo.read_sql(
            "SELECT snapshot_date, last_price, cost_price FROM holding_snapshot "
            "WHERE code=:c ORDER BY snapshot_date DESC LIMIT 4", {"c": code})
        print(f"券商快照价：\n{snap.to_string(index=False) if not snap.empty else '（无）'}")


if __name__ == "__main__":
    main()
