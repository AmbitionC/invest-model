"""ETF 板块轮动策略。

逻辑：计算各行业 ETF 相对于沪深300的相对强弱（RS），结合成交量放大信号，
识别当前资金共识最强的行业方向，辅助组合中 ETF 仓位的配置决策。

输出 ETFRotationSignal，供 advisor 或 daily_pipeline 调用。

设计原则：
  - 纯数据查询，无副作用，可独立调用
  - ETF 数据来自 etf_daily；指数来自 index_daily
  - 北向资金用市场整体净流入（stock_northbound_flow）作为大方向参考
  - RS = (ETF 20日涨跌幅) / (沪深300 20日涨跌幅)，> 1 表示跑赢
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

_IDX_HS300 = "000300.SH"


@dataclass
class ETFRotationSignal:
    """单只 ETF 的轮动评分结果。"""
    code: str
    name: str
    rs_score: float       # 相对强弱得分（vs沪深300，20日）
    vol_ratio: float      # 成交额5日均值 / 60日均值（放量信号）
    north_5d: float       # 北向5日净流入（市场整体，亿）
    composite_rank: float  # 综合排名分（0-1，越高越强）
    reason: str = ""      # 触发原因简述
    recommend: bool = False  # 是否推荐（RS > 阈值 且 放量）


class ETFRotationScanner:
    """ETF 板块轮动扫描器。

    每次调用 ``scan()`` 返回当前 ETF 池内各 ETF 的轮动信号，按综合强度排序。
    """

    def __init__(
        self,
        engine: Engine,
        rs_threshold: float = 1.05,     # RS > 105% 才算"跑赢"
        vol_ratio_threshold: float = 1.15,  # 成交额放量阈值
        lookback_days: int = 20,         # 相对强弱计算窗口
        vol_lookback_short: int = 5,     # 近期量窗口
        vol_lookback_long: int = 60,     # 基准量窗口
    ):
        self.engine = engine
        self.repo = BaseRepository(engine)
        self.rs_threshold = rs_threshold
        self.vol_ratio_threshold = vol_ratio_threshold
        self.lookback_days = lookback_days
        self.vol_lookback_short = vol_lookback_short
        self.vol_lookback_long = vol_lookback_long

    def scan(
        self,
        etf_codes: list[str],
        trade_date: str,
        code_name_map: dict[str, str] | None = None,
    ) -> list[ETFRotationSignal]:
        """扫描 ETF 池，返回按综合强度排序的轮动信号列表。

        Parameters
        ----------
        etf_codes : list[str]
            要扫描的 ETF 代码列表（来自 stock_pool 的 etf 组）。
        trade_date : str
            基准日期 YYYYMMDD，使用该日期前的数据。
        code_name_map : dict | None
            代码 → 名称映射。
        """
        if not etf_codes:
            return []
        code_name_map = code_name_map or {}

        # 1. 加载沪深300近期收益（用于相对强弱计算）
        hs300_ret = self._load_index_return(trade_date, self.lookback_days)

        # 2. 北向资金5日净流入
        north_5d = self._load_northbound_5d(trade_date)

        # 3. 逐 ETF 计算指标
        results: list[ETFRotationSignal] = []
        for code in etf_codes:
            sig = self._compute_etf_signal(
                code=code,
                name=code_name_map.get(code, code),
                trade_date=trade_date,
                hs300_ret=hs300_ret,
                north_5d=north_5d,
            )
            if sig is not None:
                results.append(sig)

        if not results:
            return []

        # 4. 归一化综合排名分（RS + 放量各占权重）
        results = self._rank_signals(results)

        # 按综合排名降序
        results.sort(key=lambda s: s.composite_rank, reverse=True)

        for s in results:
            tag = "★推荐" if s.recommend else ""
            logger.info(
                f"[ETFRotation] {s.code} {s.name:8s} "
                f"RS={s.rs_score:+.3f} Vol={s.vol_ratio:.2f} "
                f"rank={s.composite_rank:.3f} {tag}"
            )
        return results

    # ── 指标计算 ─────────────────────────────────────

    def _load_index_return(self, trade_date: str, days: int) -> float:
        """加载沪深300近 days 个交易日的累计收益率。"""
        try:
            df = self.repo.read_sql(
                """
                SELECT close
                FROM index_daily
                WHERE code = :code AND trade_date <= :td
                ORDER BY trade_date DESC
                LIMIT :n
                """,
                {"code": _IDX_HS300, "td": trade_date, "n": days + 1},
            )
            if len(df) < 2:
                return 0.0
            closes = pd.to_numeric(df["close"], errors="coerce").dropna().values
            if len(closes) < 2:
                return 0.0
            return float(closes[0]) / float(closes[-1]) - 1.0
        except Exception as e:
            logger.debug(f"[ETFRotation] 加载指数收益失败: {e}")
            return 0.0

    def _load_northbound_5d(self, trade_date: str) -> float:
        """近5日北向资金合计净流入（亿元）。"""
        try:
            df = self.repo.read_sql(
                """
                SELECT north_money
                FROM stock_northbound_flow
                WHERE trade_date <= :td
                ORDER BY trade_date DESC
                LIMIT 5
                """,
                {"td": trade_date},
            )
            if df.empty:
                return 0.0
            return float(pd.to_numeric(df["north_money"], errors="coerce").fillna(0).sum())
        except Exception as e:
            logger.debug(f"[ETFRotation] 加载北向资金失败: {e}")
            return 0.0

    def _compute_etf_signal(
        self,
        code: str,
        name: str,
        trade_date: str,
        hs300_ret: float,
        north_5d: float,
    ) -> ETFRotationSignal | None:
        """计算单只 ETF 的轮动指标。"""
        try:
            n_need = max(self.lookback_days, self.vol_lookback_long) + 1
            df = self.repo.read_sql(
                """
                SELECT trade_date, close, amount
                FROM etf_daily
                WHERE code = :code AND trade_date <= :td
                ORDER BY trade_date DESC
                LIMIT :n
                """,
                {"code": code, "td": trade_date, "n": n_need},
            )
        except Exception as e:
            logger.debug(f"[ETFRotation] 加载 {code} 数据失败: {e}")
            return None

        if df.empty or len(df) < self.lookback_days + 1:
            logger.debug(f"[ETFRotation] {code} 数据不足 (需要{self.lookback_days+1}行，有{len(df)}行)")
            return None

        df = df.sort_values("trade_date").reset_index(drop=True)
        closes = pd.to_numeric(df["close"], errors="coerce")
        amounts = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

        # ETF 近 lookback_days 日收益率
        if len(closes) < self.lookback_days + 1 or closes.iloc[-1] == 0:
            return None
        etf_ret = float(closes.iloc[-1]) / float(closes.iloc[-self.lookback_days - 1]) - 1.0

        # 相对强弱：ETF 收益 / HS300 收益（若 HS300 几乎不变，直接用 ETF 收益）
        if abs(hs300_ret) < 0.005:
            rs = 1.0 + etf_ret
        else:
            rs = (1.0 + etf_ret) / (1.0 + hs300_ret)

        # 成交额放量：近5日均额 / 近60日均额
        vol_short = float(amounts.tail(self.vol_lookback_short).mean())
        vol_long = float(amounts.tail(self.vol_lookback_long).mean())
        vol_ratio = vol_short / vol_long if vol_long > 1 else 1.0

        # 是否推荐：RS 超越阈值 且 成交量放大
        reasons: list[str] = []
        if rs > self.rs_threshold:
            reasons.append(f"RS={rs:.3f}(>{self.rs_threshold:.2f})")
        if vol_ratio > self.vol_ratio_threshold:
            reasons.append(f"放量{vol_ratio:.2f}x")
        if north_5d > 0:
            reasons.append(f"北向净流入{north_5d:.0f}亿")

        recommend = rs > self.rs_threshold and vol_ratio > self.vol_ratio_threshold

        return ETFRotationSignal(
            code=code,
            name=name,
            rs_score=round(rs, 4),
            vol_ratio=round(vol_ratio, 3),
            north_5d=round(north_5d, 1),
            composite_rank=0.0,  # 在 _rank_signals 中赋值
            reason=" | ".join(reasons) if reasons else "无明显信号",
            recommend=recommend,
        )

    def _rank_signals(self, signals: list[ETFRotationSignal]) -> list[ETFRotationSignal]:
        """归一化 RS 和放量信号，计算综合排名分（0-1）。"""
        if not signals:
            return signals

        rs_vals = [s.rs_score for s in signals]
        vr_vals = [s.vol_ratio for s in signals]

        rs_min, rs_max = min(rs_vals), max(rs_vals)
        vr_min, vr_max = min(vr_vals), max(vr_vals)

        for s in signals:
            rs_norm = (s.rs_score - rs_min) / (rs_max - rs_min + 1e-9)
            vr_norm = (s.vol_ratio - vr_min) / (vr_max - vr_min + 1e-9)
            # RS 权重 0.6，放量 0.4
            s.composite_rank = round(rs_norm * 0.6 + vr_norm * 0.4, 4)

        return signals
