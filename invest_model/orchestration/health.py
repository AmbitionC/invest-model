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

    # ── 套利模块健康（观察态也可见）──
    try:
        _arb_health(repo, health, warnings)
    except Exception:  # noqa: BLE001 — 套利健康失败不影响主健康
        pass

    # ── 排雷影子健康（提案 P7 观测面）──
    try:
        _quality_health(repo, health, version)
    except Exception:  # noqa: BLE001 — 影子健康失败不影响主健康
        pass

    health["warnings"] = warnings
    health["trustworthy"] = len(warnings) == 0
    return health


def _quality_health(repo: BaseRepository, health: dict, version: str) -> None:
    """财务排雷影子观测：最新一期红旗分布 + 当期目标组合命中数。

    影子只观察不动仓；组合命中数持续偏高时是 P7 晋升（硬过滤）的讨论信号。
    """
    if not repo.table_exists("quality_flag"):
        return
    latest = repo.read_sql("SELECT MAX(trade_date) AS d FROM quality_flag")
    d = latest["d"].iloc[0] if not latest.empty else None
    if not d:
        return
    dist = repo.read_sql(
        "SELECT n_flags, COUNT(*) AS n FROM quality_flag WHERE trade_date=:d GROUP BY n_flags",
        {"d": d})
    q: dict = {"asof": str(d)}
    if not dist.empty:
        q["dist"] = {int(r["n_flags"]): int(r["n"]) for _, r in dist.iterrows()}
        q["flagged2plus"] = int(sum(n for k, n in q["dist"].items() if k >= 2))
    hit = repo.read_sql(
        "SELECT COUNT(*) AS n FROM quality_flag qf "
        "JOIN portfolio_target pt ON pt.code=qf.code AND pt.trade_date=qf.trade_date "
        "WHERE qf.trade_date=:d AND pt.version=:v AND qf.n_flags>=2",
        {"d": d, "v": version})
    if not hit.empty:
        q["portfolio_hits_2plus"] = int(hit["n"].iloc[0])
    health["quality_screen"] = q


def _arb_health(repo: BaseRepository, health: dict, warnings: list[str]) -> None:
    """套利模块可观测指标：水表影子 IC 期数 / α 命中 / sleeve 越界。"""
    arb: dict = {}
    # 水表影子晋升观测：flow_score 攒了多少期（≥12 期才够评估晋升）
    if repo.table_exists("flow_score"):
        fp = repo.read_sql("SELECT COUNT(DISTINCT trade_date) n FROM flow_score")
        if not fp.empty and fp["n"].iloc[0]:
            arb["watermeter_ic_periods"] = int(fp["n"].iloc[0])
    # 盲区 α 证伪率
    if repo.table_exists("alpha_candidate"):
        ac = repo.read_sql(
            "SELECT falsified, COUNT(*) n FROM alpha_candidate "
            "WHERE as_of_date=(SELECT MAX(as_of_date) FROM alpha_candidate) GROUP BY falsified")
        if not ac.empty:
            m = {int(r["falsified"]): int(r["n"]) for _, r in ac.iterrows()
                 if r["falsified"] is not None}
            total = sum(m.values())
            arb["alpha_total"] = total
            arb["alpha_falsified"] = m.get(1, 0)
    # sleeve 越界（零杠杆红线）：最近一次实盘账本 ledger_ok
    if repo.table_exists("action_plan_account"):
        aa = repo.read_sql(
            "SELECT ledger_ok FROM action_plan_account "
            "WHERE plan_date=(SELECT MAX(plan_date) FROM action_plan_account)")
        if not aa.empty and aa["ledger_ok"].iloc[0] is not None:
            ok = int(aa["ledger_ok"].iloc[0])
            arb["ledger_ok"] = ok
            if ok == 0:
                warnings.append("套利资金账本 Σ>100%（零杠杆红线被触发并已收缩）——请核对 sleeve 分配。")
    if arb:
        health["arb"] = arb
