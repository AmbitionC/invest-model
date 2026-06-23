"""A 股恐慌指数（0–100，越高越恐慌）。按日收盘计算，慢变量、非实时。

  python scripts/fear_gauge.py --db sqlite:///./data/real.db [--date YYYYMMDD]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.signals.fear import fear_gauge, format_fear  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="A股恐慌指数")
    ap.add_argument("--db", default="sqlite:///./data/real.db")
    ap.add_argument("--date", default=None)
    ap.add_argument("--benchmark", default="000300.SH")
    args = ap.parse_args()
    g = fear_gauge(make_engine(args.db), dt=args.date, benchmark=args.benchmark)
    print(format_fear(g))


if __name__ == "__main__":
    main()
