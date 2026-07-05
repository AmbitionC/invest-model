"""统一资金账本回测：把各 sleeve 净值按零杠杆权重合成一条组合净值。

port_ret_t = Σ_sleeve w_sleeve * sleeve_ret_t；现金 sleeve 吸收 1-Σ 的空档。
每个 sleeve 与合成结果都落 backtest_run/nav/trades（按 version），下游零改动。
"""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import text

from invest_model.arb.config import ArbConfig
from invest_model.arb.ledger import allocate_sleeves
from invest_model.backtest.cs_engine import CSBacktestConfig, CSBacktestResult
from invest_model.backtest.metrics import compute_metrics
from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


def compose_ledger(sleeve_navs: dict[str, pd.DataFrame], cfg: ArbConfig,
                   start: str, end: str,
                   fear_map: dict[str, float] | None = None) -> CSBacktestResult:
    """合成账本净值。sleeve_navs: {'defense_A'|'offense_B'|'alpha': nav_df}。

    zero-leverage：权重来自 allocate_sleeves（Σ 三 sleeve ≤1，余为现金）。
    fear_map 提供则逐日恐慌驱动权重，否则用静态中点分配。
    """
    # 各 sleeve 的日收益序列（对齐到并集日期，缺失日 ret=0）
    ret_frames = {}
    all_dates: set[str] = set()
    for sleeve, nav in sleeve_navs.items():
        if nav is None or nav.empty:
            continue
        s = nav.set_index("trade_date")["ret"].astype(float)
        ret_frames[sleeve] = s
        all_dates |= set(s.index)
    dates = sorted(all_dates)
    if not dates:
        flat = pd.DataFrame({"trade_date": [start], "nav": [1.0], "ret": [0.0],
                             "turnover": [0.0], "position_count": [0], "invested": [0.0]})
        return CSBacktestResult(CSBacktestConfig(name="arb_ledger", start_date=start,
                                                 end_date=end), flat, [],
                                compute_metrics(flat).to_dict())

    static_w = allocate_sleeves(cfg, fear_score=None)
    nav_vals, rets = [], []
    prev = 1.0
    for d in dates:
        w = static_w
        if fear_map is not None and d in fear_map:
            w = allocate_sleeves(cfg, fear_score=fear_map[d])
        port_ret = 0.0
        for sleeve in ("defense_A", "offense_B", "alpha"):
            s = ret_frames.get(sleeve)
            if s is not None and d in s.index:
                port_ret += float(w.get(sleeve, 0.0)) * float(s.loc[d])
        # 现金 sleeve ret=0
        prev = prev * (1.0 + port_ret)
        nav_vals.append(prev); rets.append(port_ret)
    nav = pd.DataFrame({
        "trade_date": dates, "nav": nav_vals, "ret": rets,
        "turnover": 0.0, "position_count": len(ret_frames),
        "invested": sum(static_w.get(s, 0.0) for s in ("defense_A", "offense_B", "alpha")),
    })
    conf = CSBacktestConfig(name="arb_ledger", strategy="arb_capital_ledger",
                            start_date=start, end_date=end)
    return CSBacktestResult(conf, nav, [], compute_metrics(nav).to_dict())


def persist_arb_result(engine, res: CSBacktestResult, version: str,
                       top_k: int = 0) -> int:
    """独立落库（不依赖 ClosedLoop），镜像 _persist_backtest。返回 run_id。"""
    repo = BaseRepository(engine)
    run_row = {
        "name": res.config.name, "strategy": res.config.strategy,
        "start_date": res.config.start_date, "end_date": res.config.end_date,
        "rebalance_days": 0, "top_k": top_k,
        "params": json.dumps({"version": version}, ensure_ascii=False),
        "metrics": json.dumps(res.metrics, ensure_ascii=False),
    }
    with repo.engine.begin() as conn:
        cur = conn.execute(text(
            "INSERT INTO backtest_run (name,strategy,start_date,end_date,"
            "rebalance_days,top_k,params,metrics) VALUES "
            "(:name,:strategy,:start_date,:end_date,:rebalance_days,:top_k,:params,:metrics)"
        ), run_row)
        run_id = int(cur.lastrowid)
    nav = res.nav_df.copy()
    nav.insert(0, "run_id", run_id)
    repo.bulk_insert("backtest_nav",
                     nav[["run_id", "trade_date", "nav", "ret", "turnover", "position_count"]])
    if res.trades:
        tr = pd.DataFrame(res.trades)
        tr.insert(0, "run_id", run_id)
        repo.bulk_insert("backtest_trades",
                         tr[["run_id", "trade_date", "code", "action", "weight", "price"]])
    logger.info(f"套利回测落库 run_id={run_id} version={version}: "
                f"{json.dumps(res.metrics, ensure_ascii=False)}")
    return run_id
