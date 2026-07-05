"""三水表 / 盲区 α 信号录入：把人工提炼的结构化 CSV upsert 进
watermeter_signal / alpha_candidate。

三水表（信贷/财政/政策资本）与盲区 α 是文档《套利方案 v2》里无法自动化的部分，
沿用投顾信号（ingest_advisor.py）的人在环路 curated feed 约定：可审计、留痕。

校验：meter ∈ {credit,fiscal,policy_capital}；dimension ∈ {sector,theme}；
direction ∈ {in,out,flat}；α 必须带 falsification_rule（剥离股价·只看产业侧资金）。

示例：
  python scripts/ingest_watermeter.py --db sqlite:///./data/real.db \
      --kind watermeter --csv config/watermeter_template.csv
  python scripts/ingest_watermeter.py --db sqlite:///./data/real.db \
      --kind alpha --csv config/alpha_template.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.arb_repo import (  # noqa: E402
    VALID_DIMENSIONS,
    VALID_FLOW_DIRECTIONS,
    VALID_METERS,
    AlphaRepo,
    WaterMeterRepo,
)

_WATERMETER_COLS = ["as_of_date", "meter", "dimension", "key", "flow_score",
                    "direction", "evidence", "source", "valid_until", "raw_excerpt"]
_ALPHA_COLS = ["as_of_date", "code", "version", "theme", "thesis",
               "falsification_rule", "falsified", "water_meter", "grade",
               "valid_until", "evidence"]


def ingest_watermeter(engine, df: pd.DataFrame) -> int:
    errs = []
    bad_m = set(df["meter"].dropna()) - VALID_METERS
    bad_dim = set(df["dimension"].dropna()) - VALID_DIMENSIONS
    if "direction" in df.columns:
        bad_dir = set(df["direction"].dropna()) - VALID_FLOW_DIRECTIONS
        if bad_dir:
            errs.append(f"非法 direction: {bad_dir}（应∈{VALID_FLOW_DIRECTIONS}）")
    if bad_m:
        errs.append(f"非法 meter: {bad_m}（应∈{VALID_METERS}）")
    if bad_dim:
        errs.append(f"非法 dimension: {bad_dim}（应∈{VALID_DIMENSIONS}）")
    if errs:
        raise SystemExit("水表录入校验失败：\n  - " + "\n  - ".join(errs))
    df = df.copy()
    for c in _WATERMETER_COLS:
        if c not in df.columns:
            df[c] = None
    return WaterMeterRepo(engine).save(df[_WATERMETER_COLS])


def ingest_alpha(engine, df: pd.DataFrame) -> int:
    df = df.copy()
    # 证伪标准是盲区 α 的唯一护栏，缺失即拒绝（防止"我希望它是红利"）。
    if "falsification_rule" not in df.columns or df["falsification_rule"].isna().all():
        raise SystemExit("盲区 α 录入必须带 falsification_rule（剥离股价·只看产业侧资金到没到）。")
    if "version" not in df.columns:
        df["version"] = "arb_alpha_v1"
    if "falsified" not in df.columns:
        df["falsified"] = -1     # 未知
    for c in _ALPHA_COLS:
        if c not in df.columns:
            df[c] = None
    return AlphaRepo(engine).save(df[_ALPHA_COLS])


def main() -> None:
    ap = argparse.ArgumentParser(description="三水表 / 盲区 α 信号录入")
    ap.add_argument("--db", default=None)
    ap.add_argument("--kind", choices=["watermeter", "alpha"], default="watermeter")
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()

    engine = make_engine(args.db)
    create_schema(engine)
    df = pd.read_csv(args.csv, dtype=str).where(lambda x: x.notna(), None)
    if args.kind == "watermeter":
        n = ingest_watermeter(engine, df)
    else:
        n = ingest_alpha(engine, df)
    print(f"已录入 {n} 条 {args.kind} 信号。")


if __name__ == "__main__":
    main()
