"""轻度指数择时：把组合总仓位 gross 在 [floor, 1.0] 间线性调节。

信号融合三项（各 [0,1]，等权平均）：
  1) 指数收盘是否在 MA60 上方（趋势）
  2) 宽度：universe 中收盘价在各自 MA20 上方的占比
  3) 低波：指数 20 日年化波动的反向打分

关键设计：线性、有下限（默认 0.5）、**不连乘、不设死区** —— 这是对旧系统
「regime×safety×tanh 连乘压成 0」空仓陷阱的针对性修复。

P12（默认关闭）：指数 MA60 **事件闸**——线性融合缺"趋势断裂"的事件语义
（指数有效跌破 MA60 时 gross 只是渐降而非一次性降到位）。开启后：收盘
< MA60×(1−break_pct) 视为有效跌破 → gross 直接取 floor（与线性结果取 min，
不连乘、不改 floor 本身）；收盘站回 MA60 上方即解除。出处：life-teachers
重远投资观 2026-07-10《散户亏损失控》；晋升前置：E9 验证过关
（scripts/validation/e9_index_ma60.py）+ 影子期，见 model_change_proposals.md P12。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


def ma60_gate_active(closes: pd.Series, break_pct: float = 0.01) -> bool:
    """P12 事件闸纯函数：最新收盘是否处于「有效跌破 MA60」状态。

    有效跌破 = 收盘 < MA60×(1−break_pct)；收盘 ≥ MA60 即解除（中间带为迟滞区，
    维持前一状态由调用方决定——本函数只判当日，保守口径：迟滞区不算跌破）。
    样本不足 60 根返回 False（数据不够不触发，宁松勿误伤）。
    """
    s = pd.to_numeric(closes, errors="coerce").dropna()
    if len(s) < 60:
        return False
    ma60 = float(s.tail(60).mean())
    last = float(s.iloc[-1])
    return bool(np.isfinite(ma60) and ma60 > 0 and last < ma60 * (1.0 - break_pct))


class MarketTiming:
    def __init__(self, engine, benchmark: str = "000300.SH",
                 floor: float = 0.5, cap: float = 1.0, enabled: bool = True,
                 ma60_gate: bool = False, ma60_break_pct: float = 0.01):
        self.engine = engine
        self.benchmark = benchmark
        self.floor = floor
        self.cap = cap
        self.enabled = enabled
        self.ma60_gate = ma60_gate            # P12 事件闸（默认关=现状逐字一致）
        self.ma60_break_pct = ma60_break_pct
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
        gross = float(np.clip(gross, self.floor, self.cap))
        # P12：有效跌破 MA60 → 与线性结果取 min（事件降到位；不连乘、不破 floor）
        if self.ma60_gate and ma60_gate_active(self._index_close(trade_date),
                                               self.ma60_break_pct):
            logger.info("P12 MA60 事件闸触发（%s 有效跌破）：gross %.2f → floor %.2f",
                        self.benchmark, gross, self.floor)
            gross = min(gross, self.floor)
        return gross

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
