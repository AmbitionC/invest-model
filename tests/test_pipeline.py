"""端到端冒烟测试：在临时 SQLite 上灌入小规模合成数据，跑完整闭环，
断言核心机制成立（空仓陷阱已消除、近满仓、约 top_n 持仓、正 alpha、合理 beta）。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine
from invest_model.factors import FactorPipeline
from invest_model.model import CSPredictor
from invest_model.model.dataset import compute_factor_ic, rebalance_dates
from invest_model.orchestration import ClosedLoop, LoopConfig
from invest_model.portfolio import PortfolioConfig
from invest_model.universe import UniverseConfig
from scripts.gen_synthetic_sample import generate

START, END = "20210101", "20251231"


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    db = tmp_path_factory.mktemp("db") / "t.db"
    url = f"sqlite:///{db}"
    generate(url, n_stocks=120, start=START, end=END, seed=7)
    eng = make_engine(url)
    create_schema(eng)
    return eng


def test_factor_signal_present(engine):
    """动量/质量因子应呈正 rank-IC（合成嵌入信号可被侦测）。"""
    rebs = rebalance_dates(engine, START, END, "monthly")
    cfg = LoopConfig(start=START, end=END)
    loop = ClosedLoop(engine, cfg)
    loop.build_universe()
    loop.build_factors()
    ic = compute_factor_ic(engine, rebs)
    mean_ic = ic.groupby("factor_name")["rank_ic"].mean()
    # 嵌入信号方向应可侦测（动量/质量为正）
    assert mean_ic.get("mom_120", 0) > 0.005
    assert mean_ic.get("roe", 0) > 0.005


def test_full_loop_fixes_cash_trap(engine):
    """完整闭环：beta、仓位、持仓数、alpha 均达标（对比旧系统 beta=0.13/空仓）。"""
    cfg = LoopConfig(
        start=START, end=END, version="test_v1",
        universe=UniverseConfig(method="alla"),
        portfolio=PortfolioConfig(top_n=30, max_weight=0.08),
    )
    m = ClosedLoop(engine, cfg).run("all")
    assert m["beta"] > 0.5, f"beta 应回到 0.5+（空仓陷阱已消除），实际 {m['beta']}"
    assert m["avg_invested"] > 0.7, f"应近满仓，实际 {m['avg_invested']}"
    assert 20 <= m["avg_position_count"] <= 40, f"应约 30 只，实际 {m['avg_position_count']}"
    assert m["alpha"] > 0, f"应有正 alpha，实际 {m['alpha']}"


def test_predictions_monotonic(engine):
    """合成分应单调：top 五分位前瞻收益 > bottom 五分位。"""
    import pandas as pd
    from invest_model.model.dataset import forward_returns, next_rebalance_map

    rebs = rebalance_dates(engine, START, END, "monthly")
    nxt = next_rebalance_map(rebs)
    pred = CSPredictor(engine, version="mono_v1")
    spreads = []
    for t in rebs:
        if t not in nxt:
            continue
        p = pred.predict(t)
        if len(p) < 20:
            continue
        fwd = forward_returns(engine, t, nxt[t], p["code"].tolist())
        p = p.set_index("code")
        common = p.index.intersection(fwd.index)
        if len(common) < 20:
            continue
        q = pd.qcut(p.loc[common, "score"], 5, labels=False, duplicates="drop")
        spreads.append(fwd.loc[common][q == q.max()].mean() - fwd.loc[common][q == q.min()].mean())
    assert pd.Series(spreads).mean() > 0, "多空价差应为正"
