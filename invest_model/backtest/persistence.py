"""回测结果持久化：写入 backtest_run / backtest_nav / backtest_trades 三表。"""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.backtest.engine import BacktestResult
from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


def save_backtest_result(engine: Engine, result: BacktestResult) -> int:
    """落库回测结果，返回 run_id。"""
    cfg = result.config

    # 1) backtest_run
    params = {
        "fee_rate": cfg.fee_rate,
        "stamp_tax": cfg.stamp_tax,
        "slippage": cfg.slippage,
        "min_position_change": cfg.min_position_change,
        "max_position_per_stock": cfg.max_position_per_stock,
        "benchmark_code": cfg.benchmark_code,
        "initial_capital": cfg.initial_capital,
    }
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                INSERT INTO backtest_run
                (name, strategy, start_date, end_date, rebalance_days, top_k, params, metrics)
                VALUES (:name, :strategy, :s, :e, :rb, NULL, :params, :metrics)
            """),
            {
                "name": cfg.name,
                "strategy": cfg.strategy,
                "s": cfg.start_date,
                "e": cfg.end_date,
                "rb": cfg.rebalance_days,
                "params": json.dumps(params, ensure_ascii=False),
                "metrics": json.dumps(result.metrics, ensure_ascii=False),
            },
        )
        run_id = res.lastrowid

    # 2) backtest_nav
    nav_rows = result.nav_df.copy()
    nav_rows["run_id"] = run_id
    nav_rows = nav_rows[["run_id", "trade_date", "nav", "ret", "turnover", "position_count"]]
    repo = BaseRepository(engine)
    repo.upsert("backtest_nav", nav_rows, unique_keys=["run_id", "trade_date"])

    # 3) backtest_trades（清空已有 run_id 的记录则非必需，因为 INSERT IGNORE）
    trade_rows = []
    for t in result.trades:
        trade_rows.append({
            "run_id": run_id,
            "trade_date": t.trade_date,
            "code": t.code,
            "action": t.action,
            "weight": round(t.weight_after, 6),
            "price": t.price,
        })
    if trade_rows:
        df = pd.DataFrame(trade_rows)
        repo.bulk_insert("backtest_trades", df)

    logger.info(f"回测落库完成 run_id={run_id}: nav={len(nav_rows)} trades={len(trade_rows)}")
    return int(run_id)


def load_backtest_nav(engine: Engine, run_id: int) -> pd.DataFrame:
    """读取某次回测的 NAV 序列。"""
    repo = BaseRepository(engine)
    return repo.read_sql(
        "SELECT * FROM backtest_nav WHERE run_id = :r ORDER BY trade_date",
        {"r": run_id},
    )


def list_backtest_runs(engine: Engine, name: str | None = None) -> pd.DataFrame:
    """列出回测运行记录。"""
    repo = BaseRepository(engine)
    if name:
        return repo.read_sql(
            "SELECT * FROM backtest_run WHERE name = :n ORDER BY created_at DESC",
            {"n": name},
        )
    return repo.read_sql(
        "SELECT * FROM backtest_run ORDER BY created_at DESC LIMIT 20", {}
    )
