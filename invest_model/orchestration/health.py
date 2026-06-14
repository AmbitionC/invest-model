"""系统健康指标：因子 IC 衰减、universe 覆盖、近期合成 IC。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.repositories.factor_repo import FactorRepository


def compute_health(engine, version: str, method: str = "alla", recent: int = 6) -> dict:
    repo = BaseRepository(engine)
    frepo = FactorRepository(engine)

    health: dict = {}

    # 因子 IC（近 recent 期均值，按 |mean| 排序的前若干）
    ic = frepo.get_ic_log()
    if not ic.empty:
        ic["rank_ic"] = pd.to_numeric(ic["rank_ic"], errors="coerce")
        recent_dates = sorted(ic["trade_date"].unique())[-recent:]
        rec = ic[ic["trade_date"].isin(recent_dates)]
        m = rec.groupby("factor_name")["rank_ic"].mean().sort_values(key=abs, ascending=False)
        health["recent_factor_ic"] = {k: round(float(v), 4) for k, v in m.items()}
        all_m = ic.groupby("factor_name")["rank_ic"].mean()
        health["factor_ic_full"] = {k: round(float(v), 4) for k, v in all_m.items()}

    # universe 覆盖
    uni = repo.read_sql(
        "SELECT trade_date, COUNT(*) AS n FROM universe_snapshot WHERE method=:m GROUP BY trade_date",
        {"m": method},
    )
    if not uni.empty:
        health["universe_avg_size"] = round(float(uni["n"].mean()), 1)
        health["universe_periods"] = int(len(uni))

    # 组合覆盖
    pf = repo.read_sql(
        "SELECT trade_date, COUNT(*) AS n FROM portfolio_target WHERE version=:v GROUP BY trade_date",
        {"v": version},
    )
    if not pf.empty:
        health["portfolio_avg_holdings"] = round(float(pf["n"].mean()), 1)

    return health
