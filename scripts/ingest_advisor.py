"""投顾信号录入：把提炼好的结构化 CSV upsert 进 advisor_reco / advisor_theme。

校验：① 个股 code 必须在 stock_info 命中；② grade/direction/source_type 合法；
③ 早午盘(intraday)缺 valid_until 时按 rec_date + N 个交易日自动补失效日。

示例：
  python scripts/ingest_advisor.py --db sqlite:///./data/real.db \
      --kind reco --csv config/advisor_signal_template.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.advisor_repo import (  # noqa: E402
    VALID_DIRECTIONS,
    VALID_GRADES,
    VALID_SOURCE_TYPES,
    AdvisorRepo,
)
from invest_model.repositories.base import BaseRepository  # noqa: E402


def _next_trading_day(repo: BaseRepository, rec_date: str, n: int) -> str | None:
    df = repo.read_sql(
        "SELECT cal_date FROM trade_calendar WHERE cal_date>:d AND is_open=1 "
        "ORDER BY cal_date LIMIT :n", {"d": rec_date, "n": n})
    if df.empty:
        return None
    return str(df["cal_date"].iloc[-1])


def ingest_reco(engine, df: pd.DataFrame, intraday_valid_days: int = 3) -> int:
    repo = BaseRepository(engine)
    # 校验合法值
    bad_g = set(df["grade"].dropna()) - VALID_GRADES
    bad_d = set(df["direction"].dropna()) - VALID_DIRECTIONS
    bad_s = set(df["source_type"].dropna()) - VALID_SOURCE_TYPES
    errs = []
    if bad_g:
        errs.append(f"非法 grade: {bad_g}（应∈{VALID_GRADES}）")
    if bad_d:
        errs.append(f"非法 direction: {bad_d}（应∈{VALID_DIRECTIONS}）")
    if bad_s:
        errs.append(f"非法 source_type: {bad_s}（应∈{VALID_SOURCE_TYPES}）")
    # 校验代码命中
    codes = list(dict.fromkeys(df["code"].astype(str)))
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    known = set(repo.read_sql(
        f"SELECT ts_code FROM stock_info WHERE ts_code IN ({ph})", params)["ts_code"])
    missing = [c for c in codes if c not in known]
    if missing:
        errs.append(f"以下 code 在 stock_info 未命中（可能代码错/缺数据）: {missing}")
    if errs:
        raise SystemExit("录入校验失败：\n  - " + "\n  - ".join(errs))

    # 补默认失效日
    df = df.copy()
    if "valid_until" not in df.columns:
        df["valid_until"] = None
    for i, r in df.iterrows():
        if (pd.isna(r.get("valid_until")) or not str(r.get("valid_until")).strip()) \
                and r["source_type"] == "intraday":
            df.at[i, "valid_until"] = _next_trading_day(repo, str(r["rec_date"]), intraday_valid_days)
    return AdvisorRepo(engine).save_reco(df)


def ingest_theme(engine, df: pd.DataFrame) -> int:
    return AdvisorRepo(engine).save_theme(df)


def main() -> None:
    ap = argparse.ArgumentParser(description="投顾信号录入")
    ap.add_argument("--db", default=None)
    ap.add_argument("--kind", choices=["reco", "theme"], default="reco")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--intraday-valid-days", type=int, default=3)
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)
    df = pd.read_csv(args.csv, dtype=str).where(lambda x: x.notna(), None)
    if args.kind == "reco":
        n = ingest_reco(engine, df, args.intraday_valid_days)
    else:
        n = ingest_theme(engine, df)
    print(f"已录入 {n} 条 {args.kind} 信号。")


if __name__ == "__main__":
    main()
