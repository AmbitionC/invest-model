"""可移植 schema（SQLAlchemy Core）—— MySQL / SQLite 通用。

包含两类表：
1. **行情底座**（新系统读写，需在 SQLite 路径下也能建）：trade_calendar / stock_info /
   stock_daily / stock_fundamental / stock_fina_indicator / index_daily /
   stock_northbound_flow / stock_margin。列与旧 ``models/ddl.py`` 对齐，
   故在生产 MySQL 上 ``create_all(checkfirst=True)`` 会跳过这些已存在表。
2. **新模型表**：universe_snapshot / factor_exposure / model_prediction /
   portfolio_target / model_registry / factor_ic_log。

factor_exposure 采用长表（trade_date, code, factor, value），新增因子无需改表结构。
"""

from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

_NOW = text("CURRENT_TIMESTAMP")


def _created_at() -> Column:
    return Column("created_at", DateTime, server_default=_NOW)


# ── 行情底座 ────────────────────────────────────────────────

trade_calendar = Table(
    "trade_calendar", metadata,
    Column("cal_date", String(8), primary_key=True),
    Column("is_open", Integer, nullable=False, server_default=text("0")),
    Column("pretrade_date", String(8)),
)

stock_info = Table(
    "stock_info", metadata,
    Column("ts_code", String(16), primary_key=True),
    Column("symbol", String(10)),
    Column("name", String(32)),
    Column("area", String(16)),
    Column("industry", String(32)),
    Column("market", String(16)),
    Column("list_date", String(8)),
)

stock_daily = Table(
    "stock_daily", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("open", Numeric(12, 3)),
    Column("high", Numeric(12, 3)),
    Column("low", Numeric(12, 3)),
    Column("close", Numeric(12, 3)),
    Column("pre_close", Numeric(12, 3)),
    Column("change", Numeric(12, 3)),
    Column("pct_chg", Numeric(12, 4)),
    Column("volume", Numeric(20, 2)),
    Column("amount", Numeric(20, 3)),
)

stock_fundamental = Table(
    "stock_fundamental", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("pe_ttm", Numeric(20, 6)),
    Column("pb", Numeric(20, 6)),
    Column("ps_ttm", Numeric(20, 6)),
    Column("total_mv", Numeric(20, 4)),
    Column("circ_mv", Numeric(20, 4)),
    Column("turnover_rate", Numeric(12, 4)),
    Column("turnover_rate_f", Numeric(12, 4)),
)

stock_fina_indicator = Table(
    "stock_fina_indicator", metadata,
    Column("code", String(16), primary_key=True),
    Column("report_date", String(8), primary_key=True),
    Column("ann_date", String(8)),
    Column("eps", Numeric(14, 4)),
    Column("bps", Numeric(14, 4)),
    Column("roe", Numeric(12, 4)),
    Column("roa", Numeric(12, 4)),
    Column("gross_margin", Numeric(12, 4)),
    Column("debt_to_asset", Numeric(12, 4)),
    Column("revenue_yoy", Numeric(12, 4)),
    Column("profit_yoy", Numeric(12, 4)),
    Column("revenue", Numeric(20, 4)),
    Column("net_profit", Numeric(20, 4)),
    Column("ocfps", Numeric(14, 4)),
)

index_daily = Table(
    "index_daily", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("open", Numeric(14, 3)),
    Column("high", Numeric(14, 3)),
    Column("low", Numeric(14, 3)),
    Column("close", Numeric(14, 3)),
    Column("pre_close", Numeric(14, 3)),
    Column("change", Numeric(14, 3)),
    Column("pct_chg", Numeric(12, 4)),
    Column("volume", Numeric(22, 2)),
    Column("amount", Numeric(22, 3)),
)

stock_northbound_flow = Table(
    "stock_northbound_flow", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("ggt_ss", Numeric(18, 4)),
    Column("ggt_sz", Numeric(18, 4)),
    Column("hgt", Numeric(18, 4)),
    Column("sgt", Numeric(18, 4)),
    Column("north_money", Numeric(18, 4)),
    Column("south_money", Numeric(18, 4)),
)

stock_namechange = Table(
    "stock_namechange", metadata,
    Column("ts_code", String(16), primary_key=True),
    Column("start_date", String(8), primary_key=True),   # 该名称启用日
    Column("name", String(32)),
    Column("end_date", String(8)),                        # 该名称停用日（最新名为空）
    Column("change_reason", String(64)),
)

stock_hk_hold = Table(
    "stock_hk_hold", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("vol", Numeric(20, 2)),      # 北向持股量(股)
    Column("ratio", Numeric(10, 4)),    # 占流通股本比例(%)
)

stock_margin = Table(
    "stock_margin", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("rzye", Numeric(20, 4)),
    Column("rqye", Numeric(20, 4)),
    Column("rzmre", Numeric(20, 4)),
    Column("rqyl", Numeric(18, 2)),
    Column("rzche", Numeric(20, 4)),
    Column("rqchl", Numeric(18, 2)),
)

# ── 新模型表 ────────────────────────────────────────────────

universe_snapshot = Table(
    "universe_snapshot", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("method", String(32), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("name", String(32)),
    Column("industry", String(32)),
    Column("circ_mv", Numeric(20, 4)),
    Column("amount", Numeric(20, 3)),
    _created_at(),
)

factor_exposure = Table(
    "factor_exposure", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("factor", String(32), primary_key=True),
    Column("value", Numeric(16, 8)),
    _created_at(),
)

model_prediction = Table(
    "model_prediction", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("version", String(32), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("score", Numeric(16, 8)),
    Column("rank_pct", Numeric(10, 6)),
    _created_at(),
)

portfolio_target = Table(
    "portfolio_target", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("version", String(32), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("weight", Numeric(12, 6)),
    Column("rank", Integer),
    Column("gross_exposure", Numeric(8, 4)),
    Column("grade", String(2)),          # 投顾分级 A/B/C；量化补充为空
    Column("source", String(16)),        # advisor | quant
    _created_at(),
)

model_registry = Table(
    "model_registry", metadata,
    Column("version", String(48), primary_key=True),
    Column("model_type", String(24)),
    Column("train_start", String(8)),
    Column("train_end", String(8)),
    Column("n_samples", Integer),
    Column("n_factors", Integer),
    Column("factor_cols", Text),
    Column("cv_ic_mean", Numeric(10, 6)),
    Column("cv_ic_ir", Numeric(10, 6)),
    Column("cv_hit_rate", Numeric(8, 4)),
    Column("model_path", String(255)),
    _created_at(),
)

factor_ic_log = Table(
    "factor_ic_log", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("factor_name", String(32), primary_key=True),
    Column("horizon", Integer, primary_key=True),
    Column("ic", Numeric(10, 6)),
    Column("rank_ic", Numeric(10, 6)),
    _created_at(),
)

# 回测结果表（沿用旧 backtest/persistence.py 的 backtest_run/nav/trades；
# 这里也给出可移植定义，便于 SQLite 路径下回测落库）。
backtest_run = Table(
    "backtest_run", metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(128), nullable=False),
    Column("strategy", String(64), nullable=False),
    Column("start_date", String(8), nullable=False),
    Column("end_date", String(8), nullable=False),
    Column("rebalance_days", Integer),
    Column("top_k", Integer),
    Column("params", Text),
    Column("metrics", Text),
    _created_at(),
)

backtest_nav = Table(
    "backtest_nav", metadata,
    Column("run_id", Integer, primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("nav", Numeric(18, 6), nullable=False),
    Column("ret", Numeric(12, 6)),
    Column("turnover", Numeric(12, 6)),
    Column("position_count", Integer),
)

backtest_trades = Table(
    "backtest_trades", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, nullable=False),
    Column("trade_date", String(8), nullable=False),
    Column("code", String(16), nullable=False),
    Column("action", String(8), nullable=False),
    Column("weight", Numeric(12, 6)),
    Column("price", Numeric(14, 4)),
)

# ── 投顾融合 / 实盘决策表 ────────────────────────────────────

# 投顾个股信号（研报=中期有效 research / 早午盘=短期 intraday）。
advisor_reco = Table(
    "advisor_reco", metadata,
    Column("rec_date", String(8), primary_key=True),       # 生效起始日 YYYYMMDD
    Column("code", String(16), primary_key=True),
    Column("source_type", String(12), primary_key=True),   # research | intraday
    Column("grade", String(2)),                            # A | B | C
    Column("direction", String(8)),                        # long | reduce | avoid | exit
    Column("catalyst", Text),                              # 催化剂/推荐逻辑
    Column("valid_until", String(8)),                      # 失效日（空=按 source_type 默认）
    Column("source", String(64)),                          # 团队/分析师标识
    Column("raw_excerpt", Text),                           # 原文留痕（可审计）
    _created_at(),
)

# 投顾行业/主题信号。
advisor_theme = Table(
    "advisor_theme", metadata,
    Column("rec_date", String(8), primary_key=True),
    Column("theme", String(64), primary_key=True),
    Column("source_type", String(12), primary_key=True),
    Column("direction", String(8)),
    Column("thesis", Text),
    Column("valid_until", String(8)),
    _created_at(),
)

# 当前持仓（实盘，股数+成本价）。
current_holding = Table(
    "current_holding", metadata,
    Column("code", String(16), primary_key=True),
    Column("shares", Numeric(20, 2)),
    Column("cost_price", Numeric(14, 4)),
    Column("entry_date", String(8)),
    Column("updated_at", DateTime, server_default=_NOW),
)

# 操作计划存证（每次 build_action_plan 落一批）。
action_plan = Table(
    "action_plan", metadata,
    Column("plan_date", String(8), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("name", String(32)),
    Column("action", String(12)),                          # buy|add|trim|sell|hold
    Column("cur_weight", Numeric(12, 6)),
    Column("tgt_weight", Numeric(12, 6)),
    Column("shares_delta", Numeric(20, 2)),
    Column("reason", String(64)),
    Column("stop_price", Numeric(14, 4)),
    Column("ref_price", Numeric(14, 4)),
    Column("grade", String(2)),
    _created_at(),
)

# 每日持仓快照（时间序列，供复盘/净值曲线；逐持仓一行）。
holding_snapshot = Table(
    "holding_snapshot", metadata,
    Column("snapshot_date", String(8), primary_key=True),   # YYYYMMDD
    Column("code", String(16), primary_key=True),           # 含 ETF/转债，不校验 stock_info
    Column("name", String(32)),
    Column("asset_type", String(8)),                        # stock | etf | bond
    Column("shares", Numeric(20, 2)),
    Column("available", Numeric(20, 2)),
    Column("cost_price", Numeric(16, 4)),
    Column("last_price", Numeric(16, 4)),
    Column("market_value", Numeric(20, 2)),
    Column("pnl", Numeric(20, 2)),
    Column("pnl_pct", Numeric(12, 4)),
    _created_at(),
)

# 每日账户快照（现金 + 总市值 + 总资产）。
account_snapshot = Table(
    "account_snapshot", metadata,
    Column("snapshot_date", String(8), primary_key=True),
    Column("cash", Numeric(20, 2)),
    Column("market_value", Numeric(20, 2)),
    Column("total_asset", Numeric(20, 2)),
    _created_at(),
)

# 关键列补丁：老库已存在的表按需补列（create_all 不会改已存在表）。
_COLUMN_PATCHES: dict[str, dict[str, str]] = {
    "portfolio_target": {"grade": "VARCHAR(2)", "source": "VARCHAR(16)"},
}


def _ensure_columns(engine: Engine) -> None:
    repo_exists_cols: dict[str, set[str]] = {}
    dialect = engine.dialect.name
    with engine.begin() as conn:
        for table, cols in _COLUMN_PATCHES.items():
            try:
                if dialect == "sqlite":
                    rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
                    existing = {r[1] for r in rows}
                else:
                    rows = conn.exec_driver_sql(
                        "SELECT column_name FROM information_schema.columns "
                        f"WHERE table_schema=DATABASE() AND table_name='{table}'"
                    ).fetchall()
                    existing = {r[0] for r in rows}
            except Exception:  # noqa: BLE001 — 表不存在时交给 create_all
                continue
            repo_exists_cols[table] = existing
            for col, ddl in cols.items():
                if col not in existing:
                    conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def create_schema(engine: Engine) -> list[str]:
    """在给定引擎上创建全部新系统表（已存在则跳过）。返回本次新建/确保的表名。"""
    metadata.create_all(engine, checkfirst=True)
    _ensure_columns(engine)
    return list(metadata.tables.keys())
