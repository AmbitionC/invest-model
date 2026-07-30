"""五层能力圈落地取数（一次性 dump·只读不落库）。

导出三份数据供 P21/E16、E23 恐慌分支联合回测与近五年 sleeve 回测（本地复现）：
  1) results/fear_daily_dump.csv       —— fear_daily 全历史（trade_date,score，DB）
  2) results/fund_share_dump.csv       —— 国家队常用宽基 ETF 份额（tushare fund_share，2018 起）
  3) results/fund_close_dump.csv       —— 同篮子 ETF 收盘（tushare fund_daily，算净申购额用）
  python scripts/analysis/five_layer_data_dump.py [--out-dir results]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402

BASKET = ["510300.SH", "510050.SH", "510500.SH", "159919.SZ",
          "159915.SZ", "588000.SH", "512100.SH", "159949.SZ"]
START = "20180101"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="results")
    args = ap.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    repo = BaseRepository(make_engine())
    fear = repo.read_sql("SELECT trade_date, score FROM fear_daily ORDER BY trade_date")
    fear.to_csv(out / "fear_daily_dump.csv", index=False)
    print(f"fear_daily: {len(fear)} 行 {fear['trade_date'].min()} ~ {fear['trade_date'].max()}")

    cli = TushareClient()
    shares, closes = [], []
    end = pd.Timestamp.utcnow().strftime("%Y%m%d")
    for code in BASKET:
        for y0 in range(2018, 2031, 4):
            s = cli.get_fund_share(code, f"{y0}0101", f"{y0 + 3}1231")
            if s is not None and not s.empty:
                shares.append(s[["code", "trade_date", "fd_share"]])
            c = cli.get_etf_daily(code, f"{y0}0101", f"{y0 + 3}1231")
            if c is not None and not c.empty:
                closes.append(c[["code", "trade_date", "close"]])
    sh = pd.concat(shares, ignore_index=True).drop_duplicates(["code", "trade_date"])
    cl = pd.concat(closes, ignore_index=True).drop_duplicates(["code", "trade_date"])
    sh.to_csv(out / "fund_share_dump.csv", index=False)
    cl.to_csv(out / "fund_close_dump.csv", index=False)
    print(f"fund_share: {len(sh)} 行 | fund_close: {len(cl)} 行 | 篮子 {len(BASKET)} 只 截至 {end}")


if __name__ == "__main__":
    main()
