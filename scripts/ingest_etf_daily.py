"""拉持仓/观察 ETF 的前复权日线，写入 stock_daily（与股票同表），
让操作计划对 ETF 也能算 MA20 / 硬止损，跟股票一套规则。

ETF 代码来源：最新持仓快照(asset_type=etf) + config/watch_etf.txt。
前复权：fund_daily × (fund_adj 因子 / 最新因子)，使最新价==原始价、历史按
拆分/折算校正——否则均线会被价格台阶污染（与 live_check 的 ETF 处理一致）。

  python scripts/ingest_etf_daily.py --start 20230101
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402

_COLS = ["code", "trade_date", "open", "high", "low", "close",
         "pre_close", "change", "pct_chg", "volume", "amount"]


def _client(retries: int = 6):
    """创建 TushareClient；卖家 token 单 IP 限制('ip超限')时退避重试。"""
    import time
    for i in range(retries):
        try:
            return TushareClient()
        except Exception as e:  # noqa: BLE001
            if any(k in str(e) for k in ("ip超限", "多个ip", "多个IP")):
                wait = 20 * (i + 1)
                print(f"Tushare IP 争用，等待 {wait}s 重试 {i + 1}/{retries} …")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Tushare 客户端多次创建失败（IP 争用，稍后由定时任务重试）")


def _etf_codes(repo: BaseRepository) -> list[str]:
    codes: set[str] = set()
    if repo.table_exists("holding_snapshot"):
        df = repo.read_sql(
            "SELECT code FROM holding_snapshot "
            "WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM holding_snapshot) "
            "AND LOWER(asset_type)='etf'")
        if not df.empty:
            codes |= {str(c) for c in df["code"]}
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "config", "watch_etf.txt")
    if os.path.exists(path):
        for ln in open(path, encoding="utf-8"):
            head = ln.split("#")[0].strip()
            if head:
                codes.add(head.split()[0])
    return sorted(c for c in codes if c)


def main() -> None:
    ap = argparse.ArgumentParser(description="ETF 前复权日线 → stock_daily")
    ap.add_argument("--start", default="20230101")
    ap.add_argument("--end", default=None)
    args = ap.parse_args()
    end = args.end or _dt.datetime.now().strftime("%Y%m%d")

    repo = BaseRepository(make_engine())
    cli = _client()
    codes = _etf_codes(repo)
    print(f"ETF 待拉({len(codes)}): {codes}")
    total = 0
    for code in codes:
        try:
            df = cli.get_etf_daily(code, args.start, end)   # fund_daily，列已 rename code/volume
        except Exception as e:  # noqa: BLE001
            print(f"WARN {code} fund_daily 失败：{e}")
            continue
        if df is None or df.empty:
            print(f"WARN {code} 无日线")
            continue
        try:
            adj = cli.pro.query("fund_adj", ts_code=code, start_date=args.start, end_date=end)
        except Exception:  # noqa: BLE001
            adj = None
        df = df.sort_values("trade_date").reset_index(drop=True)
        if adj is not None and len(adj):
            adj = adj.sort_values("trade_date")
            fac = dict(zip(adj["trade_date"], pd.to_numeric(adj["adj_factor"], errors="coerce")))
            last_fac = float(pd.to_numeric(adj["adj_factor"], errors="coerce").iloc[-1])
            f = (df["trade_date"].map(fac).astype(float).ffill().bfill().fillna(last_fac) / last_fac)
            adj_ok = True
        else:
            print(f"WARN {code} 无 fund_adj，用原始价（趋势可能受拆分污染）")
            f = pd.Series(1.0, index=df.index)
            adj_ok = False
        out = pd.DataFrame({"code": code, "trade_date": df["trade_date"].astype(str)})
        for col in ("open", "high", "low", "close"):
            out[col] = pd.to_numeric(df[col], errors="coerce") * f.values
        out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
        out["pre_close"] = out["close"].shift(1)
        out["change"] = out["close"] - out["pre_close"]
        out["pct_chg"] = (out["change"] / out["pre_close"] * 100).round(4)
        out = out.dropna(subset=["close"])[_COLS]
        n = repo.upsert("stock_daily", out, ["code", "trade_date"])
        total += n
        print(f"{code}: upsert {n} 行（前复权={'是' if adj_ok else '否'}）")
    print(f"ETF 日线入库完成，共 {total} 行")


if __name__ == "__main__":
    main()
