"""持仓行情体检：查 stock_daily 里每只持仓「更到哪一天 + 原始/前复权收盘 + MA20/MA60 + 破位」。

用途：核对计划里的现价是否为最新、以及是否跌破 MA20/MA60（破位）。只读、不落库、不改生产。
前复权口径与 live_check._levels 一致（有 stock_adj 则折算，最新价==原始价）。

用法（Actions，读生产 stock_daily）：
  python scripts/analysis/quote_check.py [--db ...] [--codes 002851.SZ,600160.SH]
      # 不传 --codes 时默认查 current_holding 全部持仓
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


def _codes(repo: BaseRepository, arg: str | None) -> list[tuple[str, str]]:
    if arg:
        return [(c.strip(), "") for c in arg.split(",") if c.strip()]
    try:
        df = repo.read_sql("SELECT code, name FROM current_holding ORDER BY code")
        return [(str(r["code"]), str(r.get("name") or "")) for _, r in df.iterrows()]
    except Exception:  # noqa: BLE001
        return []


def _qfq(repo: BaseRepository, code: str) -> pd.DataFrame:
    """取一只的 stock_daily（近 200 交易日），前复权后返回 [trade_date, raw, close]。"""
    df = repo.read_sql(
        "SELECT trade_date, close FROM stock_daily WHERE code=:c "
        "ORDER BY trade_date DESC LIMIT 200", {"c": code})
    if df.empty:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["trade_date"] = df["trade_date"].astype(str)
    df["raw"] = pd.to_numeric(df["close"], errors="coerce")
    df["close"] = df["raw"]
    try:
        if repo.table_exists("stock_adj"):
            adj = repo.read_sql(
                "SELECT trade_date, adj_factor FROM stock_adj WHERE code=:c "
                "ORDER BY trade_date DESC LIMIT 200", {"c": code})
            if not adj.empty:
                adj["trade_date"] = adj["trade_date"].astype(str)
                f = pd.to_numeric(adj.set_index("trade_date")["adj_factor"], errors="coerce")
                fac = df["trade_date"].map(f).ffill().bfill()
                lastf = pd.to_numeric(fac.iloc[-1], errors="coerce")
                if fac.notna().all() and pd.notna(lastf) and float(lastf) > 0:
                    df["close"] = df["raw"] * fac.astype(float) / float(lastf)
    except Exception:  # noqa: BLE001 — 复权失败退回原始价
        pass
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="持仓行情体检（最新价 + MA20/MA60 + 破位）")
    ap.add_argument("--db", default=None)
    ap.add_argument("--codes", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db))

    dbmax = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]
    print(f"stock_daily 全库最新交易日：{dbmax}\n", flush=True)

    rows = []
    for code, name in _codes(repo, args.codes):
        g = _qfq(repo, code)
        if g.empty or len(g) < 21:
            rows.append((code, name, "—", "样本不足/无数据", "", "", "", "", ""))
            continue
        cl = g["close"]
        last_date = g["trade_date"].iloc[-1]
        last = float(cl.iloc[-1])
        raw = float(g["raw"].iloc[-1])
        ma20 = float(cl.tail(20).mean())
        ma60 = float(cl.tail(60).mean()) if len(cl) >= 60 else float("nan")
        ma60_prev = float(cl.tail(65).head(60).mean()) if len(cl) >= 65 else ma60
        prev_below20 = bool(cl.iloc[-2] < cl.tail(21).head(20).mean())
        d20 = last / ma20 - 1
        d60 = last / ma60 - 1 if ma60 == ma60 else float("nan")
        # 破位判读
        st20 = "破" if last < ma20 else "上方"
        st60 = "破" if (ma60 == ma60 and last < ma60) else "上方"
        fresh = "" if last >= ma20 else ("非新鲜(昨已破)" if prev_below20 else "★新鲜破位")
        rows.append((code, name, last_date, f"{raw:.2f}", f"{last:.2f}",
                     f"{ma20:.2f}({st20}{d20:+.1%})",
                     f"{ma60:.2f}({st60}{d60:+.1%})" if ma60 == ma60 else "—",
                     "↑" if ma60 >= ma60_prev else "↓", fresh))

    hdr = ["代码", "名称", "库内最新日", "原始收盘", "前复权", "MA20(状态/距离)",
           "MA60(状态/距离)", "MA60趋势", "MA20破位性质"]
    print(" | ".join(hdr))
    print("-|-".join(["--"] * len(hdr)))
    for r in rows:
        print(" | ".join(str(x) for x in r), flush=True)


if __name__ == "__main__":
    main()
