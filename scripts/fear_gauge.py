"""A 股恐慌指数（0–100，越高越恐慌）。按日收盘计算，慢变量、非实时。

  python scripts/fear_gauge.py --db sqlite:///./data/real.db [--date YYYYMMDD]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.signals.fear import fear_gauge, format_fear  # noqa: E402


def persist_fear(engine, g: dict) -> None:
    """恐慌指数按日落库 fear_daily（仪表盘历史曲线用）。"""
    import pandas as pd

    from invest_model.data import create_schema
    from invest_model.repositories.base import BaseRepository

    create_schema(engine)
    BaseRepository(engine).upsert("fear_daily", pd.DataFrame([{
        "trade_date": str(g["date"]), "score": g["score"], "level": g["level"],
        "components": json.dumps(g.get("components", {}), ensure_ascii=False),
        "raw": json.dumps(g.get("raw", {}), ensure_ascii=False),
    }]), ["trade_date"])


def main() -> None:
    ap = argparse.ArgumentParser(description="A股恐慌指数")
    ap.add_argument("--db", default="sqlite:///./data/real.db")
    ap.add_argument("--date", default=None)
    ap.add_argument("--benchmark", default="000300.SH")
    ap.add_argument("--persist", action="store_true", help="结果落库 fear_daily")
    args = ap.parse_args()
    engine = make_engine(args.db)
    g = fear_gauge(engine, dt=args.date, benchmark=args.benchmark)
    print(format_fear(g))
    if args.persist:
        try:
            persist_fear(engine, g)
            print(f"已落库 fear_daily（{g['date']}）")
        except Exception as e:  # noqa: BLE001
            print(f"WARN fear_daily 落库失败：{e}")


if __name__ == "__main__":
    main()
