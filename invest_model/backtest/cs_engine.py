"""截面回测引擎：逐日 close-to-close 重估 + 调仓日按外部目标权重换仓。

与旧 ``BacktestEngine`` 的区别：universe 逐期变化、目标权重由外部 target_provider
给出（来自 portfolio 层），不绑定单票 advisor。成本模型沿用旧引擎：
双边手续费 0.0003 + 卖出印花税 0.001 + 滑点 0.0005。
A 股拟真：涨停不买 / 跌停不卖 / 停牌不可交易，月频收盘换仓天然满足 T+1。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

from invest_model.backtest.metrics import compute_metrics
from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

TargetProvider = Callable[[str, dict[str, float]], dict[str, float]]


@dataclass
class CSBacktestConfig:
    name: str = "cs_multifactor_v1"
    strategy: str = "cross_sectional_multifactor"
    start_date: str = ""
    end_date: str = ""
    fee_rate: float = 0.0003
    stamp_tax: float = 0.001
    slippage: float = 0.0005
    min_trade: float = 0.01          # 换手带：权重变动 < 1% 跳过
    benchmark_code: str = "000300.SH"
    limit_pct: float = 9.8           # 涨跌停近似阈值（|pct_chg|）
    exec_lag: int = 1                # 成交滞后：1=调仓日出信号、次一交易日收盘成交（避免同日前视）；0=同日收盘成交


@dataclass
class CSBacktestResult:
    config: CSBacktestConfig
    nav_df: pd.DataFrame
    trades: list[dict] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


class CSBacktestEngine:
    def __init__(self, engine, config: CSBacktestConfig, target_provider: TargetProvider,
                 rebalance_dates: list[str]):
        self.engine = engine
        self.cfg = config
        self.target_provider = target_provider
        self.reb_set = set(rebalance_dates)
        self.repo = BaseRepository(engine)
        self._close: pd.DataFrame = pd.DataFrame()
        self._pct: pd.DataFrame = pd.DataFrame()

    def _load_prices(self) -> list[str]:
        df = self.repo.read_sql(
            "SELECT code, trade_date, close, pct_chg FROM stock_daily "
            "WHERE trade_date>=:s AND trade_date<=:d",
            {"s": self.cfg.start_date, "d": self.cfg.end_date},
        )
        if df.empty:
            return []
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
        self._close = df.pivot(index="trade_date", columns="code", values="close").sort_index()
        self._pct = df.pivot(index="trade_date", columns="code", values="pct_chg").sort_index()
        return self._close.index.tolist()

    def _tradable(self, code: str, dt: str, buying: bool) -> bool:
        try:
            p = self._close.at[dt, code]
            pc = self._pct.at[dt, code]
        except KeyError:
            return False
        if p is None or not np.isfinite(p) or p <= 0:
            return False
        if pd.notna(pc):
            if buying and pc >= self.cfg.limit_pct:
                return False           # 涨停不买
            if (not buying) and pc <= -self.cfg.limit_pct:
                return False           # 跌停不卖
        return True

    def run(self) -> CSBacktestResult:
        dates = self._load_prices()
        if not dates:
            raise ValueError(f"回测区间无行情：{self.cfg.start_date}~{self.cfg.end_date}")

        weights: dict[str, float] = {}
        nav = 1.0
        nav_rows, trades = [], []
        prev_close = {}
        pending: dict[str, float] | None = None   # 待次日成交的目标权重

        for i, dt in enumerate(dates):
            day_ret = 0.0
            if i > 0 and weights:
                for c, w in list(weights.items()):
                    p0 = prev_close.get(c)
                    p1 = self._close.at[dt, c] if c in self._close.columns else None
                    if p0 and p1 and np.isfinite(p1) and p0 > 0:
                        day_ret += w * (p1 / p0 - 1.0)
                nav *= 1.0 + day_ret
                # 重估权重
                new_w = {}
                for c, w in weights.items():
                    p0 = prev_close.get(c)
                    p1 = self._close.at[dt, c] if c in self._close.columns else None
                    new_w[c] = w * (p1 / p0) if (p0 and p1 and np.isfinite(p1) and p0 > 0) else w
                tot = sum(new_w.values())
                cash = max(0.0, 1.0 - sum(weights.values()))
                denom = tot + cash
                if denom > 0:
                    weights = {c: v / denom for c, v in new_w.items()}

            # 成交：exec_lag=1 时，调仓日只出信号（pending），次一交易日收盘成交，
            # 杜绝「用当日收盘信号在当日收盘成交」的同日前视。
            turnover = 0.0
            if pending is not None:
                turnover, nav = self._apply_targets(pending, dt, weights, nav, trades)
                pending = None
            if dt in self.reb_set:
                signal = self.target_provider(dt, dict(weights)) or {}
                if self.cfg.exec_lag <= 0:
                    turnover, nav = self._apply_targets(signal, dt, weights, nav, trades)
                else:
                    pending = signal

            n_pos = sum(1 for w in weights.values() if w > 1e-4)
            invested = sum(weights.values())
            nav_rows.append({"trade_date": dt, "nav": round(nav, 6),
                             "ret": round(day_ret, 6) if i > 0 else 0.0,
                             "turnover": round(turnover, 6),
                             "position_count": n_pos,
                             "invested": round(invested, 4)})

            for c in self._close.columns:
                p = self._close.at[dt, c]
                if p is not None and np.isfinite(p):
                    prev_close[c] = float(p)

        nav_df = pd.DataFrame(nav_rows)
        metrics = compute_metrics(nav_df, benchmark_nav=self._benchmark_nav(dates))
        md = metrics.to_dict()
        md["avg_invested"] = round(float(nav_df["invested"].mean()), 4)
        return CSBacktestResult(self.cfg, nav_df, trades, md)

    def _apply_targets(self, targets: dict[str, float], dt: str,
                       weights: dict[str, float], nav: float, trades: list) -> tuple[float, float]:
        """在 dt 收盘把组合调向 targets，扣成本、记录成交。返回 (turnover, nav)。"""
        turnover = 0.0
        for c in set(weights) | set(targets):
            cur = weights.get(c, 0.0)
            tgt = targets.get(c, 0.0)
            delta = tgt - cur
            if abs(delta) < self.cfg.min_trade:
                continue
            if not self._tradable(c, dt, buying=delta > 0):
                continue
            fee = self.cfg.fee_rate + self.cfg.slippage + (self.cfg.stamp_tax if delta < 0 else 0.0)
            nav *= 1.0 - abs(delta) * fee
            turnover += abs(delta)
            weights[c] = tgt
            trades.append({"trade_date": dt, "code": c,
                           "action": "buy" if delta > 0 else "sell",
                           "weight": round(tgt, 6),
                           "price": float(self._close.at[dt, c])})
        # 清理空仓
        for c in [c for c, w in weights.items() if w <= 1e-6]:
            weights.pop(c, None)
        return turnover, nav

    def _benchmark_nav(self, dates: list[str]) -> pd.DataFrame | None:
        df = self.repo.read_sql(
            "SELECT trade_date, close FROM index_daily "
            "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
            {"c": self.cfg.benchmark_code, "s": self.cfg.start_date, "d": self.cfg.end_date},
        )
        if df.empty:
            return None
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.set_index("trade_date").reindex(dates).ffill().dropna()
        if df.empty:
            return None
        df["nav"] = df["close"] / df["close"].iloc[0]
        return df.reset_index().rename(columns={"index": "trade_date"})[["trade_date", "nav"]]
