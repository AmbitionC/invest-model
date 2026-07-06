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
    UniqueConstraint,
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
    Column("dv_ratio", Numeric(12, 6)),      # 股息率(%)，红利 carry 用
    Column("dv_ttm", Numeric(12, 6)),        # TTM 股息率(%)
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
    # 单季同比（增速二阶导因子 growth_accel 原料；财报跟踪法：只盯单季增速同比）
    Column("q_sales_yoy", Numeric(12, 4)),
    Column("q_profit_yoy", Numeric(12, 4)),
)

# 三大报表扩展项（排雷打分器 + 扣商誉估值原料）：balancesheet/income/cashflow
# 按报告期合并为一行。金额单位：元（Tushare 原始口径）。
stock_fina_ext = Table(
    "stock_fina_ext", metadata,
    Column("code", String(16), primary_key=True),
    Column("report_date", String(8), primary_key=True),
    Column("ann_date", String(8)),
    Column("goodwill", Numeric(20, 4)),                 # 商誉
    Column("minority_int", Numeric(20, 4)),             # 少数股东权益
    Column("eq_exc_min", Numeric(20, 4)),               # 归母股东权益（净资产）
    Column("accounts_receiv", Numeric(20, 4)),          # 应收账款
    Column("revenue", Numeric(20, 4)),                  # 营业收入
    Column("n_income", Numeric(20, 4)),                 # 净利润（含少数股东损益）
    Column("n_income_attr_p", Numeric(20, 4)),          # 归母净利润
    Column("sell_exp", Numeric(20, 4)),                 # 销售费用
    Column("admin_exp", Numeric(20, 4)),                # 管理费用
    Column("fin_exp", Numeric(20, 4)),                  # 财务费用
    Column("n_cashflow_act", Numeric(20, 4)),           # 经营活动现金流净额
)

# 业绩预告/快报（时效层：预报<快报<定期报告，越快越有效——财报跟踪法）。
# profit_yoy 统一为「累计净利同比(%)」口径：express 取 yoy_net_profit，
# forecast 取 (p_change_min+p_change_max)/2 —— 与 stock_fina_indicator.profit_yoy 可比。
fina_express = Table(
    "fina_express", metadata,
    Column("code", String(16), primary_key=True),
    Column("report_date", String(8), primary_key=True),
    Column("kind", String(8), primary_key=True),        # express | forecast
    Column("ann_date", String(8)),
    Column("revenue", Numeric(20, 4)),
    Column("n_income", Numeric(20, 4)),
    Column("profit_yoy", Numeric(12, 4)),               # 累计净利同比(%)
    Column("forecast_type", String(16)),                # 预增/预减/扭亏/首亏…（forecast）
)

# 重要股东/高管增减持（跟庄信号：信号可信度=押注÷身家，高管≈20×大股东）。
holder_trade = Table(
    "holder_trade", metadata,
    Column("code", String(16), primary_key=True),
    Column("ann_date", String(8), primary_key=True),
    Column("holder_name", String(64), primary_key=True),
    Column("in_de", String(4), primary_key=True),       # IN 增持 | DE 减持
    Column("holder_type", String(4)),                   # G 高管 | P 个人 | C 公司
    Column("change_vol", Numeric(20, 2)),               # 变动数量(股)
    Column("change_ratio", Numeric(12, 4)),             # 占流通比例(%)
    Column("after_ratio", Numeric(12, 4)),
    Column("avg_price", Numeric(14, 4)),
)

# 财务排雷影子打分（7 规则红旗；影子观察：只记录不剔除，晋升见提案 P7）。
quality_flag = Table(
    "quality_flag", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("n_flags", Integer),
    Column("flags", Text),                              # json 数组：每面红旗的人话理由
    _created_at(),
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

# 个股两融明细（融资余额 by 股票 → 聚合行业得信贷水表；margin_detail）。
stock_margin_detail = Table(
    "stock_margin_detail", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("rzye", Numeric(20, 4)),      # 融资余额(元)
    Column("rqye", Numeric(20, 4)),      # 融券余额
    Column("rzmre", Numeric(20, 4)),     # 融资买入额
    Column("rzche", Numeric(20, 4)),     # 融资偿还额
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
    Column("trigger_hint", String(64)),                    # 买点挂单提示（trigger 为 MySQL 保留字）
    Column("model_rank", Numeric(10, 6)),                  # 全市场因子分位
    Column("model_view", String(32)),                      # 模型研判：看好 前8% ★★★
    _created_at(),
)

# 操作计划账户层元数据（build_action_plan 的 account 块，供仪表盘展示）。
action_plan_account = Table(
    "action_plan_account", metadata,
    Column("plan_date", String(8), primary_key=True),
    Column("equity", Numeric(20, 2)),
    Column("invested_pct", Numeric(12, 6)),
    Column("cash_pct", Numeric(12, 6)),
    Column("n_holdings", Integer),
    Column("unrealized_pnl_pct", Numeric(12, 6)),
    Column("gross_target", Numeric(12, 6)),
    Column("risk_off", Integer),                           # 0/1
    Column("model_ic_mean", Numeric(10, 6)),
    Column("model_ic_ir", Numeric(10, 6)),
    Column("model_hit", Numeric(8, 4)),
    Column("model_conf_label", String(32)),
    Column("risk_hints", String(512)),                      # 执行对账/集中度/仓位偏离等账户级提示
    _created_at(),
)

# 研报速通影子验证：fast(信号次日收盘直入) vs gate(旧严格闸门) 两条虚拟净值，
# 供上线 4~6 周后复核「研报直入」政策；RESEARCH_FAST_ENTRY=0 可随时回退。
policy_shadow = Table(
    "policy_shadow", metadata,
    Column("signal_date", String(8), primary_key=True),    # 信号 rec_date
    Column("code", String(16), primary_key=True),
    Column("grade", String(2)),
    Column("d0_date", String(8)),                           # fast 入场日（信号次一交易日）
    Column("d0_close", Numeric(18, 4)),
    Column("gate_date", String(8)),                         # 严格闸门首次触发日（可为空=从未触发）
    Column("gate_close", Numeric(18, 4)),
    Column("last_date", String(8)),
    Column("last_close", Numeric(18, 4)),
    Column("fast_ret", Numeric(12, 6)),
    Column("gate_ret", Numeric(12, 6)),
    _created_at(),
)

# 投顾信号实战战绩记分卡：按「来源×等级」统计信号买入后的真实前瞻收益。
# 这才是"跟着系统到底有没有用"的真答案（量化回测衡量的是参谋、不是实盘）。
signal_scorecard = Table(
    "signal_scorecard", metadata,
    Column("as_of", String(8), primary_key=True),          # 统计基准日 YYYYMMDD
    Column("bucket", String(24), primary_key=True),        # research/A、intraday、ALL 等
    Column("label", String(32)),                           # 展示名（研报A级 等）
    Column("n", Integer),                                  # 样本信号数
    Column("win_rate", Numeric(8, 4)),                     # 买入→最新 收益为正的比例
    Column("mean_ret", Numeric(12, 6)),                    # 平均前瞻收益（信号次日买→最新）
    Column("median_ret", Numeric(12, 6)),
    Column("mean_excess", Numeric(12, 6)),                 # 相对沪深300同窗口的超额均值
    Column("mean_hold_days", Numeric(10, 2)),              # 平均持有交易日（信号至今）
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

# 盯盘预警历史（live_check 推 GitHub 的同时落库，供仪表盘消息流展示）。
watch_alert = Table(
    "watch_alert", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("alert_date", String(8), nullable=False),       # 北京日期 YYYYMMDD
    Column("alert_time", DateTime),                        # 触发时刻（CST naive）
    Column("code", String(16)),                            # 自检类可空
    Column("kind", String(12)),                            # hold|watch|etf|selfcheck
    Column("severity", String(8)),                         # crit | batch
    Column("message", Text),                               # 完整展示行
    Column("dedup_key", String(160), nullable=False),
    UniqueConstraint("alert_date", "dedup_key", name="uq_watch_alert_day_key"),
    _created_at(),
)

# 复盘报告存证（review.py 输出的 Markdown）。
review_report = Table(
    "review_report", metadata,
    Column("report_date", String(8), primary_key=True),    # asof 数据日
    Column("period", String(8), primary_key=True),         # weekly | daily | adhoc
    Column("version", String(32)),
    Column("markdown", Text),
    Column("meta", Text),                                  # json: {generated_at, ...}
    _created_at(),
)

# 恐慌指数按日存证（fear_gauge 为即时计算，落库供历史曲线）。
fear_daily = Table(
    "fear_daily", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("score", Numeric(6, 2)),
    Column("level", String(32)),
    Column("components", Text),                            # json {动量,波动率,宽度,涨跌停,新高新低}
    Column("raw", Text),                                   # json fear_gauge()["raw"]
    _created_at(),
)

# ── 套利模块（arbitrage）表 ──────────────────────────────────
# 说明：套利与交易是同一资金池的一体两面。以下表全部按 (code|id, trade_date) /
# version 命名空间落库，回测/复盘/看板复用既有骨架。数据缺失时对应 sleeve 预算
# 划入现金（绝不加杠杆），零杠杆红线无条件成立。

# 国债逆回购日行情（GC001/GC007/… close=年化利率）。
reverse_repo_daily = Table(
    "reverse_repo_daily", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("rate", Numeric(10, 4)),            # 年化利率(%)，交易所逆回购 close 即为此
    Column("close", Numeric(12, 4)),
    Column("pre_close", Numeric(12, 4)),
    Column("amount", Numeric(20, 3)),
    Column("interest_days", Integer),          # 计息天数（周四 GC001=3）
    _created_at(),
)

# 可转债基础信息（静态）。
cb_basic = Table(
    "cb_basic", metadata,
    Column("ts_code", String(16), primary_key=True),
    Column("bond_short_name", String(48)),
    Column("stk_code", String(16)),            # 正股代码
    Column("list_date", String(8)),
    Column("delist_date", String(8)),
    Column("conv_price", Numeric(14, 4)),      # 最新转股价
    Column("maturity_date", String(8)),
    Column("remain_size", Numeric(20, 4)),     # 剩余规模(万元)
    Column("call_status", String(16)),         # 强赎状态
    _created_at(),
)

# 可转债日行情。
cb_daily = Table(
    "cb_daily", metadata,
    Column("code", String(16), primary_key=True),
    Column("trade_date", String(8), primary_key=True),
    Column("open", Numeric(14, 4)),
    Column("high", Numeric(14, 4)),
    Column("low", Numeric(14, 4)),
    Column("close", Numeric(14, 4)),
    Column("pre_close", Numeric(14, 4)),
    Column("pct_chg", Numeric(12, 4)),
    Column("vol", Numeric(20, 2)),
    Column("amount", Numeric(20, 3)),
    Column("cb_value", Numeric(14, 4)),        # 纯债价值（可空）
    Column("cb_over_rate", Numeric(12, 6)),    # 纯债溢价率（可空）
    _created_at(),
)

# 分红/除权事件（红利 carry 用）。
dividend_event = Table(
    "dividend_event", metadata,
    Column("code", String(16), primary_key=True),
    Column("ex_date", String(8), primary_key=True),     # 除权除息日
    Column("ann_date", String(8)),
    Column("end_date", String(8)),
    Column("div_proc", String(16)),            # 实施 | 预案 | ...
    Column("cash_div", Numeric(14, 6)),        # 税前每股现金分红
    Column("cash_div_tax", Numeric(14, 6)),
    Column("record_date", String(8)),
    Column("pay_date", String(8)),
    Column("dv_ratio", Numeric(12, 6)),        # 股息率(%)（主取自 daily_basic）
    _created_at(),
)

# 三水表信号（人工 CSV 录入：信贷/财政/政策资本）。
watermeter_signal = Table(
    "watermeter_signal", metadata,
    Column("as_of_date", String(8), primary_key=True),
    Column("meter", String(16), primary_key=True),       # credit|fiscal|policy_capital
    Column("dimension", String(12), primary_key=True),   # sector|theme
    Column("key", String(64), primary_key=True),         # 行业/主题名
    Column("flow_score", Numeric(8, 4)),                 # -100..100
    Column("direction", String(8)),                      # in|out|flat
    Column("evidence", Text),                            # 信贷/财政/资本开支/订单锚点=证伪抓手
    Column("source", String(64)),
    Column("valid_until", String(8)),
    Column("raw_excerpt", Text),
    _created_at(),
)

# 三水表聚合后的资金流分（派生，按 version）。
flow_score = Table(
    "flow_score", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("dimension", String(12), primary_key=True),
    Column("key", String(64), primary_key=True),
    Column("version", String(32), primary_key=True),
    Column("credit", Numeric(10, 6)),
    Column("fiscal", Numeric(10, 6)),
    Column("policy", Numeric(10, 6)),
    Column("composite", Numeric(10, 6)),
    Column("z", Numeric(10, 6)),
    _created_at(),
)

# carry 信号（逆回购/红利/可转债，派生，按 version）。
carry_signal = Table(
    "carry_signal", metadata,
    Column("trade_date", String(8), primary_key=True),
    Column("sleeve", String(16), primary_key=True),      # reverse_repo|dividend_carry|convertible
    Column("code", String(16), primary_key=True),
    Column("version", String(32), primary_key=True),
    Column("expected_carry", Numeric(12, 6)),            # 年化预期 carry
    Column("horizon_days", Integer),
    Column("rank", Integer),
    Column("metric", Text),                              # json: premium/net_dv/spike_flag 等
    _created_at(),
)

# 盲区 α 候选（CSV + 派生，带证伪标记）。
alpha_candidate = Table(
    "alpha_candidate", metadata,
    Column("as_of_date", String(8), primary_key=True),
    Column("code", String(16), primary_key=True),
    Column("version", String(32), primary_key=True),
    Column("theme", String(64)),
    Column("thesis", Text),
    Column("falsification_rule", Text),                  # 剥离股价·只看产业侧资金到没到
    Column("falsified", Integer),                        # 0 否 | 1 是 | -1 未知
    Column("water_meter", String(16)),
    Column("grade", String(2)),
    Column("valid_until", String(8)),
    Column("evidence", Text),
    _created_at(),
)

# 统一资金账本：单一资金池按 sleeve 分配（plan_date/sleeve/version）。
sleeve_target = Table(
    "sleeve_target", metadata,
    Column("plan_date", String(8), primary_key=True),
    Column("sleeve", String(16), primary_key=True),      # defense_A|offense_B|alpha|cash
    Column("version", String(32), primary_key=True),
    Column("target_pct", Numeric(12, 6)),
    Column("actual_pct", Numeric(12, 6)),
    Column("min_pct", Numeric(12, 6)),
    Column("max_pct", Numeric(12, 6)),
    Column("nav", Numeric(18, 6)),                       # 回测 sleeve 净值（实盘可空）
    Column("note", String(128)),
    _created_at(),
)

# 套利战绩记分卡（结构对齐 signal_scorecard，按 sleeve/meter 分桶）。
arb_scorecard = Table(
    "arb_scorecard", metadata,
    Column("as_of", String(8), primary_key=True),
    Column("bucket", String(24), primary_key=True),      # defense_A/alpha/credit 等
    Column("label", String(32)),
    Column("n", Integer),
    Column("win_rate", Numeric(8, 4)),
    Column("mean_ret", Numeric(12, 6)),
    Column("median_ret", Numeric(12, 6)),
    Column("mean_excess", Numeric(12, 6)),
    Column("mean_hold_days", Numeric(10, 2)),
    _created_at(),
)


# 关键列补丁：老库已存在的表按需补列（create_all 不会改已存在表）。
_COLUMN_PATCHES: dict[str, dict[str, str]] = {
    "portfolio_target": {"grade": "VARCHAR(2)", "source": "VARCHAR(16)"},
    "stock_fundamental": {"dv_ratio": "DECIMAL(12,6)", "dv_ttm": "DECIMAL(12,6)"},
    "stock_fina_indicator": {"q_sales_yoy": "DECIMAL(12,4)",
                             "q_profit_yoy": "DECIMAL(12,4)"},
    # 因子层可解释归因：top3 贡献因子（如 "ep+0.8|mom_60+1.2|roe+0.5"）
    "model_prediction": {"top_factors": "VARCHAR(128)"},
    "action_plan": {
        "trigger_hint": "VARCHAR(64)",
        "model_rank": "DECIMAL(10,6)",
        "model_view": "VARCHAR(32)",
        "sleeve": "VARCHAR(16)",                          # 套利：一张表容纳 A/B/α/可转债
    },
    "action_plan_account": {
        "risk_hints": "VARCHAR(512)",
        "defense_pct": "DECIMAL(12,6)",
        "offense_pct": "DECIMAL(12,6)",
        "alpha_pct": "DECIMAL(12,6)",
        "sleeve_gross": "DECIMAL(12,6)",
        "ledger_ok": "INT",
        "carry_expected": "DECIMAL(12,6)",
        "fear_score": "DECIMAL(6,2)",
    },
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
