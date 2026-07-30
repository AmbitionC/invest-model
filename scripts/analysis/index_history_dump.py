"""指数全历史日线导出（通用工具·只读不落库）。

从 tushare index_daily 拉指定指数的全历史收盘，输出 results/index_dump_<code>.csv
（trade_date,close），供一次性分析/验证复现（如 life-teachers 博主主张核验）。
代码列表从命令行传入；workflow 从 ops/index-dump.trigger 首行读逗号分隔代码。
  python scripts/analysis/index_history_dump.py --codes 000300.SH[,399006.SZ] [--out-dir results]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.sources.tushare_client import TushareClient  # noqa: E402


def fetch_full(client: TushareClient, code: str, y0: int = 2000) -> pd.DataFrame:
    frames = []
    for y in range(y0, 2031, 5):
        seg = client.get_index_daily(code, f"{y}0101", f"{y + 4}1231")
        if seg is not None and not seg.empty:
            frames.append(seg[["trade_date", "close"]])
    df = pd.concat(frames, ignore_index=True).drop_duplicates("trade_date")
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna(subset=["close"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", required=True)
    ap.add_argument("--out-dir", default="results")
    args = ap.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    client = TushareClient()
    for code in [c.strip() for c in args.codes.split(",") if c.strip()]:
        df = fetch_full(client, code)
        p = out / f"index_dump_{code.replace('.', '_')}.csv"
        df.to_csv(p, index=False)
        print(f"{code}: {df['trade_date'].min()} ~ {df['trade_date'].max()} 共 {len(df)} 行 → {p}")


if __name__ == "__main__":
    main()
