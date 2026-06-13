"""回测引擎：基于目标仓位制的逐日换仓模拟。

设计：
- 每个交易日 t 取上一交易日特征 → advisor 输出目标仓位 → 在 t 日开盘价换仓
  （回测使用 close-to-close 简化：以 t 日 close 作为换仓价格，假设在 t 日收盘前完成）
- 持仓追踪：positions = {code: weight}，weight 为占组合权重
- 现金留存：1 - sum(positions.values())
- 收益计算：每日重估持仓市值，含滑点 + 手续费

成本模型（简化）：
- 双边手续费 0.0003（万 3）
- 印花税 0.001（仅卖出）
- 滑点 0.0005
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.advisor import StockAdvisor
from invest_model.advisor.advisor import AdvisorSignal
from invest_model.advisor.decision import DecisionConfig
from invest_model.logger import get_logger
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.repositories.etf_repo import ETFRepository
from invest_model.repositories.market_repo import MarketRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository

logger = get_logger()


@dataclass
class BacktestConfig:
    """回测配置。"""
    name: str = "ml_advisor_v1"
    strategy: str = "ml_target_position"
    start_date: str = ""
    end_date: str = ""
    initial_capital: float = 1_000_000.0
    fee_rate: float = 0.0003     # 双边手续费
    stamp_tax: float = 0.001     # 卖出印花税
    slippage: float = 0.0005     # 滑点
    rebalance_days: int = 1      # 1=每日检查
    min_position_change: float = 0.05  # 与 DecisionConfig.min_trade_size 对齐
    max_position_per_stock: float = 0.5
    benchmark_code: str | None = None  # 可选基准（如 000300.SH）

    # ── 决策层配置（可选：未传则 advisor 自带默认） ──
    # 如果同时配置 advisor 上的 decision_config 和这里，以 BacktestEngine 启动时为准（透传给 advisor）
    decision_config: DecisionConfig | None = None
    execution_tier: str | None = None  # 快捷参数：覆盖 decision_config.execution_tier


@dataclass
class TradeRecord:
    """单笔成交。"""
    trade_date: str
    code: str
    action: str
    weight_before: float
    weight_after: float
    weight_delta: float
    price: float
    cost: float                  # 成本占组合比例


@dataclass
class BacktestResult:
    """回测结果汇总。"""
    config: BacktestConfig
    nav_df: pd.DataFrame                       # trade_date, nav, ret, turnover, position_count
    trades: list[TradeRecord] = field(default_factory=list)
    daily_signals: list[AdvisorSignal] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


# ── 引擎 ──────────────────────────────────────────


class BacktestEngine:
    """目标仓位制回测引擎。"""

    def __init__(
        self,
        engine: Engine,
        advisor: StockAdvisor,
        config: BacktestConfig,
        codes: list[str],
        code_name_map: dict[str, str] | None = None,
    ):
        self.engine = engine
        self.advisor = advisor
        self.cfg = config
        self.codes = codes
        self.code_name_map = code_name_map or {}

        self.daily_repo = StockDailyRepository(engine)
        self.etf_repo = ETFRepository(engine)
        self.market_repo = MarketRepository(engine)
        self.cal_repo = CalendarRepository(engine)

        self._price_cache: dict[str, pd.DataFrame] = {}

        # 把 BacktestConfig 上的 decision_config / execution_tier 同步到 advisor
        # 优先级：cfg.execution_tier > cfg.decision_config > advisor.decision_config
        if self.cfg.decision_config is not None:
            self.advisor.decision_config = self.cfg.decision_config
        if self.cfg.execution_tier is not None:
            base = self.advisor.decision_config
            self.advisor.decision_config = DecisionConfig(
                execution_tier=self.cfg.execution_tier,
                min_trade_size=base.min_trade_size,
                score_to_position_scale=base.score_to_position_scale,
                sell_score_threshold=base.sell_score_threshold,
                min_holding_days=base.min_holding_days,
                min_flat_days=base.min_flat_days,
                take_profit_min_conditions=base.take_profit_min_conditions,
                buy_threshold=base.buy_threshold,
            )

        # 保持 cfg.min_position_change 与决策层 min_trade_size 一致（避免双重过滤）
        if self.cfg.min_position_change != self.advisor.decision_config.min_trade_size:
            self.cfg.min_position_change = self.advisor.decision_config.min_trade_size

    # ── 价格 ──────────────────────────────────────

    def _load_prices(self) -> None:
        """一次性加载全区间所有标的的日线收盘价，加速回测。"""
        for code in self.codes:
            df = self.daily_repo.get_daily(code, self.cfg.start_date, self.cfg.end_date)
            if df.empty:
                df = self.etf_repo.get_daily(code, self.cfg.start_date, self.cfg.end_date)
            if df.empty:
                logger.warning(f"回测加载价格为空: {code}")
                continue
            df = df[["trade_date", "close"]].copy()
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.set_index("trade_date").sort_index()
            self._price_cache[code] = df

    def _price(self, code: str, date: str) -> float | None:
        """取 (code, date) 的收盘价。"""
        df = self._price_cache.get(code)
        if df is None or date not in df.index:
            return None
        v = df.loc[date, "close"]
        try:
            return float(v) if v is not None and np.isfinite(float(v)) else None
        except (TypeError, ValueError):
            return None

    # ── 主循环 ────────────────────────────────────

    def run(self) -> BacktestResult:
        """执行回测。"""
        if not self.cfg.start_date or not self.cfg.end_date:
            raise ValueError("BacktestConfig 必须设置 start_date 与 end_date")

        # 加载交易日历 + 价格缓存
        trade_dates = self.cal_repo.get_trade_dates(self.cfg.start_date, self.cfg.end_date)
        if not trade_dates:
            raise ValueError(f"区间无交易日: {self.cfg.start_date}~{self.cfg.end_date}")

        self._load_prices()

        # 一次性预加载 advisor 所需特征数据（日线/技术/信号/composite/fundamental），
        # 避免主循环中每个交易日 × 每只标的反复查 DB（性能瓶颈优化，提速 5-10 倍）。
        if self.advisor.feature_builder is not None:
            logger.info(
                f"FeatureBuilder warmup: {len(self.codes)} 只标的 "
                f"× {self.cfg.start_date}~{self.cfg.end_date}"
            )
            self.advisor.feature_builder.warmup(
                self.codes, self.cfg.start_date, self.cfg.end_date
            )

        # 状态：weights 是仓位字典（占组合的权重）；nav 是单位净值
        weights: dict[str, float] = {c: 0.0 for c in self.codes}
        cash_weight = 1.0
        nav = 1.0

        nav_rows: list[dict] = []
        trades: list[TradeRecord] = []
        all_signals: list[AdvisorSignal] = []

        prev_prices: dict[str, float] = {}

        # 冷却跟踪：每只票最近一次 "开仓"（从 0 到 >0）/ "清仓"（从 >0 到 0）的交易日
        last_action_dates: dict[str, dict[str, str | None]] = {
            c: {"open": None, "clear": None} for c in self.codes
        }

        for i, dt in enumerate(trade_dates):
            day_ret = 0.0
            if i > 0:
                for c, w in weights.items():
                    if w <= 0:
                        continue
                    p_prev = prev_prices.get(c)
                    p_now = self._price(c, dt)
                    if p_prev is None or p_now is None or p_prev <= 0:
                        continue
                    asset_ret = (p_now - p_prev) / p_prev
                    day_ret += w * asset_ret
                nav *= 1.0 + day_ret

                weights = self._reprice_weights(weights, prev_prices, dt)
                cash_weight = max(0.0, 1.0 - sum(weights.values()))

            if (i % self.cfg.rebalance_days) == 0:
                signals = self.advisor.advise_batch(
                    self.codes, dt, self.code_name_map,
                    current_positions=dict(weights),
                    last_action_dates=last_action_dates,
                    trade_dates=trade_dates,
                )
                all_signals.extend(signals)

                turnover = 0.0
                for s in signals:
                    new_w = float(s.target_position)
                    new_w = max(0.0, min(new_w, self.cfg.max_position_per_stock))
                    prev_w = weights.get(s.code, 0.0)
                    delta = new_w - prev_w
                    if abs(delta) < self.cfg.min_position_change:
                        continue
                    p = self._price(s.code, dt)
                    if p is None or p <= 0:
                        continue

                    fee = self.cfg.fee_rate
                    if delta < 0:
                        fee += self.cfg.stamp_tax
                    fee += self.cfg.slippage
                    cost = abs(delta) * fee
                    nav *= 1.0 - cost
                    turnover += abs(delta)

                    weights[s.code] = new_w

                    # 维护 open/clear 日期
                    if prev_w <= 1e-6 and new_w > 1e-6:
                        last_action_dates[s.code]["open"] = dt
                    if prev_w > 1e-6 and new_w <= 1e-6:
                        last_action_dates[s.code]["clear"] = dt

                    trades.append(TradeRecord(
                        trade_date=dt,
                        code=s.code,
                        action=s.action,
                        weight_before=round(prev_w, 6),
                        weight_after=round(new_w, 6),
                        weight_delta=round(delta, 6),
                        price=p,
                        cost=cost,
                    ))
            else:
                turnover = 0.0

            cash_weight = max(0.0, 1.0 - sum(weights.values()))
            n_pos = sum(1 for w in weights.values() if w > 1e-4)

            nav_rows.append({
                "trade_date": dt,
                "nav": round(nav, 6),
                "ret": round(day_ret, 6) if i > 0 else 0.0,
                "turnover": round(turnover, 6),
                "position_count": n_pos,
            })

            # 更新 prev_prices
            for c in self.codes:
                p = self._price(c, dt)
                if p is not None:
                    prev_prices[c] = p

        nav_df = pd.DataFrame(nav_rows)

        from invest_model.backtest.metrics import compute_metrics
        metrics = compute_metrics(nav_df, benchmark_nav=self._load_benchmark_nav(trade_dates))

        return BacktestResult(
            config=self.cfg,
            nav_df=nav_df,
            trades=trades,
            daily_signals=all_signals,
            metrics=metrics.to_dict(),
        )

    # ── 辅助 ─────────────────────────────────────

    def _reprice_weights(
        self,
        weights: dict[str, float],
        prev_prices: dict[str, float],
        dt: str,
    ) -> dict[str, float]:
        """根据当日价格变动重新计算权重（保证 sum + cash = 1）。

        简化模型：对每只持仓票，新权重 = 旧权重 × (1 + 当日票收益)，然后整体（含现金）归一化。
        现金部分不增长（无利息）。
        """
        new_weights: dict[str, float] = {}
        cash_w = max(0.0, 1.0 - sum(weights.values()))
        for c, w in weights.items():
            if w <= 0:
                new_weights[c] = 0.0
                continue
            p_prev = prev_prices.get(c)
            p_now = self._price(c, dt)
            if p_prev is None or p_now is None or p_prev <= 0:
                new_weights[c] = w
                continue
            r = (p_now - p_prev) / p_prev
            new_weights[c] = w * (1.0 + r)
        total = sum(new_weights.values()) + cash_w
        if total <= 0:
            return weights
        return {c: w / total for c, w in new_weights.items()}

    def _load_benchmark_nav(self, trade_dates: list[str]) -> pd.DataFrame | None:
        """加载基准 NAV。依次尝试 stock_daily / etf_daily / index_daily 三个来源。

        000300.SH / 000905.SH 等指数实际存储在 ``index_daily`` 表，旧版只查
        前两个表，导致基准始终加载失败。
        """
        if not self.cfg.benchmark_code:
            return None

        df = self.daily_repo.get_daily(
            self.cfg.benchmark_code, self.cfg.start_date, self.cfg.end_date
        )
        if df.empty:
            df = self.etf_repo.get_daily(
                self.cfg.benchmark_code, self.cfg.start_date, self.cfg.end_date
            )
        if df.empty:
            df = self.market_repo.get_index_daily(
                self.cfg.benchmark_code, self.cfg.start_date, self.cfg.end_date
            )
        if df.empty:
            logger.warning(
                f"基准 {self.cfg.benchmark_code} 在 stock_daily/etf_daily/index_daily 均无数据，"
                f"alpha/beta 无法计算"
            )
            return None
        df = df[["trade_date", "close"]].copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.set_index("trade_date").reindex(trade_dates).ffill()
        if df["close"].iloc[0] is None or not np.isfinite(df["close"].iloc[0]):
            return None
        df["nav"] = df["close"] / df["close"].iloc[0]
        return df.reset_index()[["trade_date", "nav"]]
