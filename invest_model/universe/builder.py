"""UniverseBuilder：按交易日构建全 A 流动性投资域并落 universe_snapshot。"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.universe_repo import UniverseRepository
from invest_model.universe import filters

logger = get_logger()


@dataclass
class UniverseConfig:
    method: str = "alla"               # 投资域口径标识（落库用）
    min_list_days: int = 365           # 上市满 1 年
    liquidity_pct: float = 0.20        # 剔除流动性最差 20%
    size_pct: float = 0.10             # 剔除市值最小 10%
    min_amount: float = 0.0            # 绝对成交额下限（千元，生产可设）
    min_circ_mv: float = 0.0           # 绝对流通市值下限（万元，生产可设）
    amount_window: int = 20            # 流动性均额窗口（交易日）
    max_size: int | None = None        # 可选：仅保留市值最大的前 N 只


class UniverseBuilder:
    def __init__(self, engine, config: UniverseConfig | None = None):
        self.engine = engine
        self.cfg = config or UniverseConfig()
        self.repo = BaseRepository(engine)
        self.uni_repo = UniverseRepository(engine)

    def build(self, trade_date: str, persist: bool = True) -> list[str]:
        """返回 trade_date 当日的可投股票代码列表。"""
        cross = self._load_cross_section(trade_date)
        if cross.empty:
            logger.warning(f"universe[{trade_date}]：当日无行情数据")
            return []

        n0 = len(cross)
        cross = filters.exclude_st(cross)
        cross = filters.exclude_new_listings(cross, trade_date, self.cfg.min_list_days)
        cross = filters.exclude_suspended(cross)
        cross = filters.liquidity_filter(
            cross, self.cfg.liquidity_pct, self.cfg.size_pct,
            self.cfg.min_amount, self.cfg.min_circ_mv,
        )
        if self.cfg.max_size:
            cross = cross.nlargest(self.cfg.max_size, "circ_mv")

        codes = sorted(cross["code"].tolist())
        logger.info(f"universe[{trade_date}]：{n0} → {len(codes)} 只 (method={self.cfg.method})")

        if persist and codes:
            snap = cross[["code", "name", "industry", "circ_mv", "amount_20d"]].copy()
            snap = snap.rename(columns={"amount_20d": "amount"})
            snap.insert(0, "trade_date", trade_date)
            snap.insert(1, "method", self.cfg.method)
            self.uni_repo.save_snapshot(snap)
        return codes

    def _load_cross_section(self, trade_date: str) -> pd.DataFrame:
        """组装当日截面：行情(成交/量) + 元信息(名称/行业/上市日) + 估值(circ_mv) + 20日均额。"""
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=self.cfg.amount_window * 2 + 10)).strftime("%Y%m%d")

        # 当日行情（决定可交易/停牌）
        today = self.repo.read_sql(
            "SELECT code, volume, amount FROM stock_daily WHERE trade_date=:d",
            {"d": trade_date},
        )
        if today.empty:
            return pd.DataFrame()

        # 窗口成交额 → 20 日均额
        win = self.repo.read_sql(
            "SELECT code, trade_date, amount FROM stock_daily "
            "WHERE trade_date>=:s AND trade_date<=:d",
            {"s": start, "d": trade_date},
        )
        win["amount"] = pd.to_numeric(win["amount"], errors="coerce")
        amt20 = (win.sort_values("trade_date")
                 .groupby("code")["amount"]
                 .apply(lambda s: s.tail(self.cfg.amount_window).mean())
                 .rename("amount_20d").reset_index())

        # 元信息
        info = self.repo.read_sql(
            "SELECT ts_code AS code, name, industry, list_date FROM stock_info"
        )
        # 估值（流通市值）
        fund = self.repo.read_sql(
            "SELECT code, circ_mv FROM stock_fundamental WHERE trade_date=:d",
            {"d": trade_date},
        )

        df = (today.merge(info, on="code", how="left")
                   .merge(fund, on="code", how="left")
                   .merge(amt20, on="code", how="left"))
        df["trade_date"] = trade_date
        return df
