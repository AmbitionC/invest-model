"""回测绩效指标。

支持：
- 总收益、年化收益、年化波动、Sharpe、Sortino、MaxDD、Calmar
- 胜率、盈亏比、平均持仓天数
- 与基准的 alpha/beta（可选）
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

ANNUAL_TRADING_DAYS = 250


@dataclass
class PortfolioMetrics:
    total_return: float = 0.0
    annual_return: float = 0.0
    annual_vol: float = 0.0
    sharpe: float = 0.0
    sortino: float = 0.0
    max_drawdown: float = 0.0
    calmar: float = 0.0
    win_rate: float = 0.0
    avg_position_count: float = 0.0
    turnover_total: float = 0.0
    n_days: int = 0
    benchmark_total_return: float | None = None
    benchmark_annual_return: float | None = None
    alpha: float | None = None
    beta: float | None = None

    def to_dict(self) -> dict:
        return {k: round(v, 6) if isinstance(v, float) else v
                for k, v in self.__dict__.items() if v is not None or k.startswith("benchmark")}


def compute_metrics(
    nav_df: pd.DataFrame,
    benchmark_nav: pd.DataFrame | None = None,
) -> PortfolioMetrics:
    """从 nav_df 计算组合绩效指标。

    nav_df 必须包含 trade_date, nav, ret, turnover, position_count 列。
    """
    m = PortfolioMetrics()
    if nav_df is None or nav_df.empty:
        return m

    nav = nav_df["nav"].astype(float).values
    rets = nav_df["ret"].astype(float).values

    m.n_days = len(nav)
    m.total_return = float(nav[-1] - 1.0)
    if m.n_days > 0:
        years = m.n_days / ANNUAL_TRADING_DAYS
        m.annual_return = float((1.0 + m.total_return) ** (1.0 / max(years, 1e-9)) - 1.0)

    if len(rets) > 1:
        std = float(np.std(rets, ddof=1))
        m.annual_vol = std * np.sqrt(ANNUAL_TRADING_DAYS)
        if m.annual_vol > 0:
            m.sharpe = m.annual_return / m.annual_vol
        # Sortino: downside std
        neg = rets[rets < 0]
        if len(neg) > 1:
            downside = float(np.std(neg, ddof=1)) * np.sqrt(ANNUAL_TRADING_DAYS)
            if downside > 0:
                m.sortino = m.annual_return / downside

    # MaxDD
    peak = -np.inf
    max_dd = 0.0
    for v in nav:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > max_dd:
                max_dd = dd
    m.max_drawdown = float(max_dd)
    if max_dd > 0:
        m.calmar = m.annual_return / max_dd

    # Win rate（日胜率）
    if len(rets) > 1:
        nonzero = rets[rets != 0]
        if len(nonzero) > 0:
            m.win_rate = float((nonzero > 0).sum() / len(nonzero))

    if "position_count" in nav_df.columns:
        m.avg_position_count = float(nav_df["position_count"].mean())
    if "turnover" in nav_df.columns:
        m.turnover_total = float(nav_df["turnover"].sum())

    # 基准对比
    if benchmark_nav is not None and not benchmark_nav.empty:
        merged = nav_df.merge(
            benchmark_nav.rename(columns={"nav": "bench_nav"}),
            on="trade_date", how="inner",
        )
        if not merged.empty:
            bench = merged["bench_nav"].astype(float).values
            m.benchmark_total_return = float(bench[-1] - 1.0)
            years = len(merged) / ANNUAL_TRADING_DAYS
            m.benchmark_annual_return = float(
                (1.0 + m.benchmark_total_return) ** (1.0 / max(years, 1e-9)) - 1.0
            )
            # 简单线性回归得 beta
            br = pd.Series(bench).pct_change().fillna(0).values
            pr = merged["ret"].astype(float).values
            if len(br) > 5 and np.std(br) > 0:
                cov = np.cov(pr, br)[0, 1]
                var = np.var(br)
                if var > 0:
                    m.beta = float(cov / var)
                    # alpha 用与基准相同的重叠窗口年化策略收益，避免两边窗口不一致
                    pnav = merged["nav"].astype(float).values
                    strat_ann = float(
                        (pnav[-1] / pnav[0]) ** (1.0 / max(years, 1e-9)) - 1.0
                    )
                    m.alpha = strat_ann - (m.beta * (m.benchmark_annual_return or 0.0))

    return m
