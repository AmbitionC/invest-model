"""近五年 创业板指(399006.SZ) − 上证综指(000001.SH) 点差序列导出（用户命题·只读不落库）。

用户要画「近五年两指数差值曲线 + 上下包络辅助线（连最高/最低点拟合）」。沙箱取不到行情，
故本脚本在有 DB 的环境（Actions/FC）读 index_daily 两指数收盘、按 trade_date 内连，
输出 results/index_spread_5y.csv（trade_date,sse,chinext,diff）供 Claude 本地渲染。

口径：diff = 创业板指收盘 − 上证综指收盘（原始点差，两指基点不同、看相对强弱走势用）。
不做前复权（指数无复权概念）。近五年 = 截至最新交易日往前 5 自然年。
  python scripts/analysis/index_spread_dump.py [--db ...] [--out results/index_spread_5y.csv]
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

SSE = "000001.SH"       # 上证综指
CHINEXT = "399006.SZ"   # 创业板指


def _load(repo: BaseRepository, code: str, start: str) -> pd.DataFrame:
    df = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c AND trade_date>=:s "
        "ORDER BY trade_date",
        {"c": code, "s": start})
    if df.empty:
        return df
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna(subset=["close"]).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default="results/index_spread_5y.csv")
    ap.add_argument("--years", type=int, default=5)
    args = ap.parse_args()

    repo = BaseRepository(make_engine(args.db))
    start = (datetime.now(timezone.utc) - timedelta(days=args.years * 365 + 5)).strftime("%Y%m%d")
    sse = _load(repo, SSE, start)
    cn = _load(repo, CHINEXT, start)
    print(f"上证 {SSE}: {len(sse)} 行 {sse['trade_date'].min() if not sse.empty else '-'}"
          f"~{sse['trade_date'].max() if not sse.empty else '-'}", flush=True)
    print(f"创业板指 {CHINEXT}: {len(cn)} 行 {cn['trade_date'].min() if not cn.empty else '-'}"
          f"~{cn['trade_date'].max() if not cn.empty else '-'}", flush=True)
    if sse.empty or cn.empty:
        print("⚠️ index_daily 缺指数样本，先 bump ops/index-backfill.trigger 回填 000001.SH/399006.SZ",
              flush=True)
        raise SystemExit(1)

    m = sse.merge(cn, on="trade_date", suffixes=("_sse", "_cn"))
    m = m.rename(columns={"close_sse": "sse", "close_cn": "chinext"})
    m["diff"] = (m["chinext"] - m["sse"]).round(2)
    m = m[["trade_date", "sse", "chinext", "diff"]].sort_values("trade_date").reset_index(drop=True)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    m.to_csv(out, index=False)
    hi = m.loc[m["diff"].idxmax()]
    lo = m.loc[m["diff"].idxmin()]
    print(f"写出 {out} 共 {len(m)} 行；差值范围 [{lo['diff']}@{lo['trade_date']}, "
          f"{hi['diff']}@{hi['trade_date']}]", flush=True)


if __name__ == "__main__":
    main()
