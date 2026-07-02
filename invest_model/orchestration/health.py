"""系统健康指标：因子 IC 衰减、universe 覆盖、近期合成 IC。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.repositories.factor_repo import FactorRepository


def compute_health(engine, version: str, method: str = "alla", recent: int = 6,
                   min_universe: int = 50) -> dict:
    repo = BaseRepository(engine)
    frepo = FactorRepository(engine)

    health: dict = {}
    warnings: list[str] = []

    # 因子 IC（近 recent 期均值，按 |mean| 排序的前若干）；候选因子单列展示——
    # 它们只影子观察不参与打分，IC 攒够即可评估是否晋升（见 factors/library.py）。
    from invest_model.factors.library import CANDIDATE_FACTORS
    ic = frepo.get_ic_log()
    if not ic.empty:
        ic["rank_ic"] = pd.to_numeric(ic["rank_ic"], errors="coerce")
        recent_dates = sorted(ic["trade_date"].unique())[-recent:]
        rec = ic[ic["trade_date"].isin(recent_dates)]
        m = rec.groupby("factor_name")["rank_ic"].mean().sort_values(key=abs, ascending=False)
        cand = set(CANDIDATE_FACTORS)
        health["recent_factor_ic"] = {k: round(float(v), 4) for k, v in m.items()
                                      if k not in cand}
        health["candidate_factor_ic"] = {k: round(float(v), 4) for k, v in m.items()
                                         if k in cand}
        all_m = ic.groupby("factor_name")["rank_ic"].mean()
        health["factor_ic_full"] = {k: round(float(v), 4) for k, v in all_m.items()}
        # 候选因子晋升观测：IC 期数（≥12 期才够评估）
        n_periods = ic[ic["factor_name"].isin(cand)].groupby("factor_name")["trade_date"].nunique()
        if not n_periods.empty:
            health["candidate_ic_periods"] = {k: int(v) for k, v in n_periods.items()}

    # universe 覆盖
    uni = repo.read_sql(
        "SELECT trade_date, COUNT(*) AS n FROM universe_snapshot WHERE method=:m GROUP BY trade_date",
        {"m": method},
    )
    if not uni.empty:
        avg_size = round(float(uni["n"].mean()), 1)
        health["universe_avg_size"] = avg_size
        health["universe_periods"] = int(len(uni))
        health["universe_min_size"] = int(uni["n"].min())
        if avg_size < min_universe:
            warnings.append(
                f"universe 平均仅 {avg_size} 只(<{min_universe})：截面过小，因子 IC 与回测"
                f"结果不可信，组合退化为集中持仓。请先 `--mode update` 补全全市场行情后重跑。"
            )

    # 组合覆盖
    pf = repo.read_sql(
        "SELECT trade_date, COUNT(*) AS n FROM portfolio_target WHERE version=:v GROUP BY trade_date",
        {"v": version},
    )
    if not pf.empty:
        health["portfolio_avg_holdings"] = round(float(pf["n"].mean()), 1)

    health["warnings"] = warnings
    health["trustworthy"] = len(warnings) == 0
    return health
