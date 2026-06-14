"""ETF 新标的发现 — 基于板块轮动的 ETF 候选扫描。

与 strategy/etf_rotation.py（管理已入池 ETF 的轮动）不同：
本模块扫描**全市场**行业 ETF，发现值得纳入 stock_pool 的新 ETF。

筛选逻辑：
  1. 从 etf_daily 取全部 ETF（成交额 > 10 亿，规模过滤）
  2. 计算相对强弱 RS = (ETF 20日收益) / (沪深300 20日收益)
  3. 成交额放量确认（5日均额/60日均额 > 1.2）
  4. 北向资金同向（5日净流入 > 0）
  5. 排除已在 stock_pool 的 ETF
  6. 输出 ETF 候选（有效期 20 交易日）
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

_IDX_HS300 = "000300.SH"


@dataclass
class ETFCandidate:
    code: str
    name: str
    source: str          # 'etf_rotation'
    score: float
    reason: str
    scan_date: str
    expire_date: str


class ETFDiscoveryScanner:
    """全市场 ETF 候选发现扫描器（周频）。"""

    def __init__(
        self,
        engine: Engine,
        rs_threshold: float = 1.08,          # RS 相对沪深300 跑赢 8%
        vol_ratio_threshold: float = 1.20,   # 成交额放量
        north_positive: bool = True,         # 要求北向5日净流入 > 0
        lookback_days: int = 20,
        vol_short: int = 5,
        vol_long: int = 60,
        min_amount_yi: float = 1.0,          # 日均成交额最低 1 亿元
        top_n: int = 5,                      # 最多输出 N 只候选
    ):
        self.engine = engine
        self.repo = BaseRepository(engine)
        self.rs_threshold = rs_threshold
        self.vol_ratio_threshold = vol_ratio_threshold
        self.north_positive = north_positive
        self.lookback_days = lookback_days
        self.vol_short = vol_short
        self.vol_long = vol_long
        self.min_amount_yi = min_amount_yi
        self.top_n = top_n

    def scan(
        self,
        trade_date: str,
        exclude_codes: list[str] | None = None,
    ) -> list[ETFCandidate]:
        """扫描市场中符合轮动信号的 ETF，返回候选列表。"""
        exclude = set(exclude_codes or [])
        expire = _expire_date(trade_date, 20)

        # 1. 沪深300参照收益
        hs300_ret = self._load_hs300_return(trade_date)

        # 2. 北向5日净流入
        north_5d = self._load_northbound_5d(trade_date)
        north_ok = (not self.north_positive) or (north_5d > 0)

        # 3. 获取活跃 ETF 列表（成交额过滤）
        etf_codes = self._list_active_etfs(trade_date, exclude)
        if not etf_codes:
            logger.info(f"[Discovery/ETF] {trade_date} 无活跃 ETF")
            return []

        # 4. 计算每只 ETF 的信号
        candidates: list[ETFCandidate] = []
        for code in etf_codes:
            cand = self._evaluate_etf(
                code=code,
                trade_date=trade_date,
                hs300_ret=hs300_ret,
                north_5d=north_5d,
                north_ok=north_ok,
                expire=expire,
            )
            if cand is not None:
                candidates.append(cand)

        # 5. 按综合分降序，取 top_n
        candidates.sort(key=lambda c: -c.score)
        candidates = candidates[: self.top_n]

        logger.info(f"[Discovery/ETF] {trade_date} 发现 {len(candidates)} 只 ETF 候选")
        return candidates

    # ── 数据加载 ──────────────────────────────────────

    def _load_hs300_return(self, trade_date: str) -> float:
        try:
            df = self.repo.read_sql(
                """
                SELECT close FROM index_daily
                WHERE code = :code AND trade_date <= :td
                ORDER BY trade_date DESC LIMIT :n
                """,
                {"code": _IDX_HS300, "td": trade_date, "n": self.lookback_days + 1},
            )
            if len(df) < 2:
                return 0.0
            closes = pd.to_numeric(df["close"], errors="coerce").dropna().values
            return float(closes[0]) / float(closes[-1]) - 1.0
        except Exception as e:
            logger.debug(f"[Discovery/ETF] 加载沪深300失败: {e}")
            return 0.0

    def _load_northbound_5d(self, trade_date: str) -> float:
        try:
            df = self.repo.read_sql(
                """
                SELECT north_money FROM stock_northbound_flow
                WHERE trade_date <= :td
                ORDER BY trade_date DESC LIMIT 5
                """,
                {"td": trade_date},
            )
            if df.empty:
                return 0.0
            return float(pd.to_numeric(df["north_money"], errors="coerce").fillna(0).sum())
        except Exception as e:
            logger.debug(f"[Discovery/ETF] 加载北向资金失败: {e}")
            return 0.0

    def _list_active_etfs(self, trade_date: str, exclude: set[str]) -> list[str]:
        """取近 60 日日均成交额 > min_amount_yi 亿元的 ETF 代码。"""
        try:
            df = self.repo.read_sql(
                """
                SELECT code, AVG(amount) AS avg_amount
                FROM etf_daily
                WHERE trade_date <= :td
                  AND trade_date >= DATE_FORMAT(
                        DATE_SUB(STR_TO_DATE(:td,'%%Y%%m%%d'), INTERVAL 90 DAY),
                        '%%Y%%m%%d')
                GROUP BY code
                HAVING AVG(amount) >= :min_amt
                """,
                {
                    "td": trade_date,
                    "min_amt": self.min_amount_yi * 1e8,  # 亿元 → 元
                },
            )
        except Exception as e:
            logger.debug(f"[Discovery/ETF] 列表查询失败: {e}")
            return []

        if df.empty:
            return []
        return [c for c in df["code"].tolist() if c not in exclude]

    def _evaluate_etf(
        self,
        code: str,
        trade_date: str,
        hs300_ret: float,
        north_5d: float,
        north_ok: bool,
        expire: str,
    ) -> ETFCandidate | None:
        try:
            n_need = max(self.lookback_days, self.vol_long) + 1
            df = self.repo.read_sql(
                """
                SELECT trade_date, close, amount
                FROM etf_daily
                WHERE code = :code AND trade_date <= :td
                ORDER BY trade_date DESC LIMIT :n
                """,
                {"code": code, "td": trade_date, "n": n_need},
            )
        except Exception:
            return None

        if df.empty or len(df) < self.lookback_days + 1:
            return None

        df = df.sort_values("trade_date").reset_index(drop=True)
        closes = pd.to_numeric(df["close"], errors="coerce")
        amounts = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

        if closes.iloc[-1] == 0 or len(closes) < self.lookback_days + 1:
            return None

        # RS
        etf_ret = float(closes.iloc[-1]) / float(closes.iloc[-self.lookback_days - 1]) - 1.0
        if abs(hs300_ret) < 0.005:
            rs = 1.0 + etf_ret
        else:
            rs = (1.0 + etf_ret) / (1.0 + hs300_ret)

        # 放量
        vol_r = float(amounts.tail(self.vol_short).mean()) / (
            float(amounts.tail(self.vol_long).mean()) + 1e-9
        )

        if rs < self.rs_threshold:
            return None
        if vol_r < self.vol_ratio_threshold:
            return None
        if not north_ok:
            return None

        # 综合评分（RS 超额权重 0.6，放量 0.4）
        rs_excess = rs - 1.0
        score = round(min(rs_excess / 0.20, 1.0) * 0.6 + min((vol_r - 1.0) / 1.0, 1.0) * 0.4, 4)

        reasons: list[str] = [
            f"RS={rs:.3f}(>{self.rs_threshold:.2f})",
            f"放量{vol_r:.2f}x(>{self.vol_ratio_threshold:.2f}x)",
        ]
        if north_5d > 0:
            reasons.append(f"北向净流入{north_5d:.0f}亿")

        return ETFCandidate(
            code=code,
            name="",
            source="etf_rotation",
            score=score,
            reason=" | ".join(reasons),
            scan_date=trade_date,
            expire_date=expire,
        )


def _expire_date(trade_date: str, days: int) -> str:
    dt = datetime.strptime(trade_date, "%Y%m%d")
    expire_dt = dt + timedelta(days=int(days * 1.4))
    return expire_dt.strftime("%Y%m%d")
