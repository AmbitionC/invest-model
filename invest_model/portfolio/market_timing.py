"""轻度指数择时：把组合总仓位 gross 在 [floor, 1.0] 间线性调节。

信号融合三项（各 [0,1]，等权平均）：
  1) 指数收盘是否在 MA60 上方（趋势）
  2) 宽度：universe 中收盘价在各自 MA20 上方的占比
  3) 低波：指数 20 日年化波动的反向打分

关键设计：线性、有下限（默认 0.5）、**不连乘、不设死区** —— 这是对旧系统
「regime×safety×tanh 连乘压成 0」空仓陷阱的针对性修复。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


class MarketTiming:
    def __init__(self, engine, benchmark: str = "000300.SH",
                 floor: float = 0.5, cap: float = 1.0, enabled: bool = True):
        self.engine = engine
        self.benchmark = benchmark
        self.floor = floor
        self.cap = cap
        self.enabled = enabled
        self.repo = BaseRepository(engine)

    def gross_exposure(self, trade_date: str, universe: list[str] | None = None) -> float:
        if not self.enabled:
            return self.cap
        trend = self._trend_signal(trade_date)
        vol = self._vol_signal(trade_date)
        breadth = self._breadth_signal(trade_date, universe) if universe else 0.5
        signal = np.nanmean([trend, breadth, vol])
        if not np.isfinite(signal):
            signal = 0.5
        gross = self.floor + (self.cap - self.floor) * float(np.clip(signal, 0.0, 1.0))
        return float(np.clip(gross, self.floor, self.cap))

    def _index_close(self, trade_date: str, lookback_days: int = 200) -> pd.Series:
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=lookback_days)).strftime("%Y%m%d")
        df = self.repo.read_sql(
            "SELECT trade_date, close FROM index_daily "
            "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
            {"c": self.benchmark, "s": start, "d": trade_date},
        )
        if df.empty:
            return pd.Series(dtype=float)
        return pd.to_numeric(df.set_index("trade_date")["close"], errors="coerce")

    def _trend_signal(self, trade_date: str) -> float:
        c = self._index_close(trade_date)
        if len(c) < 60:
            return 0.5
        ma60 = c.tail(60).mean()
        last = c.iloc[-1]
        # 距 MA60 的相对偏离，映射到 [0,1]（±8% 饱和）
        dev = (last - ma60) / ma60
        return float(np.clip(0.5 + dev / 0.16, 0.0, 1.0))

    def _vol_signal(self, trade_date: str) -> float:
        c = self._index_close(trade_date)
        if len(c) < 21:
            return 0.5
        vol = c.pct_change().tail(20).std() * np.sqrt(250)
        # 年化波动 15% 记 1，35% 记 0
        return float(np.clip((0.35 - vol) / 0.20, 0.0, 1.0))

    def _breadth_signal(self, trade_date: str, universe: list[str]) -> float:
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=60)).strftime("%Y%m%d")
        df = self.repo.read_sql(
            "SELECT code, trade_date, close FROM stock_daily "
            "WHERE trade_date>=:s AND trade_date<=:d",
            {"s": start, "d": trade_date},
        )
        if df.empty:
            return 0.5
        df = df[df["code"].isin(set(universe))]
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        wide = df.pivot(index="trade_date", columns="code", values="close").sort_index()
        if len(wide) < 20:
            return 0.5
        ma20 = wide.tail(20).mean()
        last = wide.iloc[-1]
        above = (last > ma20).mean()
        return float(np.clip(above, 0.0, 1.0))
