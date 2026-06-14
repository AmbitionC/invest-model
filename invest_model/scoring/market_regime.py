"""A 股市场状态检测。

识别五种市场状态（Regime），用于调整信号仓位乘数：
  TECH_DOMINANT  : 科技/成长超强，资金虹吸，其他板块被动流出（AI 吸血行情）
  VALUE_ROTATION : 价值轮动，成长显著落后，化工/制造业相对占优
  BROAD_BULL     : 全面牛市，市场整体上涨且小盘强于大盘
  BEAR           : 熊市，大盘显著下跌
  NEUTRAL        : 无明显方向

检测指标（全部来自已有表，无需新采集）：
  - market_trend    : 沪深300（000300.SH） 20日收益
  - tech_momentum   : 创业板指（399006.SZ）20日超额 vs 沪深300
  - size_spread     : 中证500（000905.SH）20日超额 vs 沪深300
  - northbound_5d   : 近5日北向资金合计净流入（亿，正=流入）
  - m1m2_spread     : M1同比 - M2同比（剪刀差，正值=实体活跃）
  - pmi_mfg         : 制造业PMI（>50扩张，<50收缩）

仓位乘数参考（实际仓位 = advisor 输出 × multiplier，再截断到 max_single_position）：
  TECH_DOMINANT  : 0.55  （非科技标的降仓，信号可靠性下降）
  VALUE_ROTATION : 1.10  （化工/制造业可适当放大）
  BROAD_BULL     : 1.15  （普涨行情放大仓位）
  BEAR           : 0.30  （熊市大幅缩仓）
  NEUTRAL        : 1.00  （不调整）
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

REGIME_MULTIPLIER: dict[str, float] = {
    "TECH_DOMINANT":  0.55,
    "VALUE_ROTATION": 1.10,
    "BROAD_BULL":     1.15,
    "BEAR":           0.30,
    "NEUTRAL":        1.00,
}

# 指数代码
_IDX_HS300  = "000300.SH"   # 沪深300
_IDX_GEM    = "399006.SZ"   # 创业板指
_IDX_CSI500 = "000905.SH"   # 中证500


class MarketRegimeDetector:
    """市场状态检测器。"""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._repo = BaseRepository(engine)

    def detect(self, trade_date: str) -> dict:
        """返回 {regime, multiplier, indicators} 字典。

        Parameters
        ----------
        trade_date : str
            当前交易日 YYYYMMDD，检测使用该日期前 60 个交易日数据。
        """
        indicators = self._load_indicators(trade_date)
        regime = self._classify(indicators)
        multiplier = REGIME_MULTIPLIER.get(regime, 1.0)

        logger.info(
            f"[MarketRegime] {trade_date} → {regime} (×{multiplier:.2f}) | "
            f"trend={indicators.get('market_trend', 0):+.2%} "
            f"tech_mom={indicators.get('tech_momentum', 0):+.2%} "
            f"size_spread={indicators.get('size_spread', 0):+.2%} "
            f"nb5d={indicators.get('northbound_5d', 0):.1f}亿 "
            f"m1m2={indicators.get('m1m2_spread', float('nan')):.2f} "
            f"pmi={indicators.get('pmi_mfg', float('nan')):.1f}"
        )
        return {"regime": regime, "multiplier": multiplier, "indicators": indicators}

    # ── 指标计算 ──────────────────────────────────────────

    def _load_indicators(self, trade_date: str) -> dict:
        """从 index_daily 和 stock_northbound_flow 计算四个检测指标。"""
        index_df = self._repo.read_sql(
            """
            SELECT code, trade_date, close
            FROM index_daily
            WHERE code IN (:hs300, :gem, :csi500)
              AND trade_date <= :td
            ORDER BY trade_date DESC
            LIMIT 240
            """,
            {"hs300": _IDX_HS300, "gem": _IDX_GEM, "csi500": _IDX_CSI500, "td": trade_date},
        )

        market_trend   = 0.0
        tech_momentum  = 0.0
        size_spread    = 0.0

        if not index_df.empty:
            index_df["close"] = pd.to_numeric(index_df["close"], errors="coerce")
            for code in [_IDX_HS300, _IDX_GEM, _IDX_CSI500]:
                sub = (
                    index_df[index_df["code"] == code]
                    .sort_values("trade_date")
                    .dropna(subset=["close"])
                )
                if len(sub) >= 21:
                    r20 = float(sub["close"].iloc[-1]) / float(sub["close"].iloc[-21]) - 1
                    if code == _IDX_HS300:
                        market_trend = r20
                    elif code == _IDX_GEM:
                        tech_momentum = r20 - market_trend
                    elif code == _IDX_CSI500:
                        size_spread = r20 - market_trend

        # 北向资金：近5个交易日净流入合计
        northbound_5d = self._load_northbound_5d(trade_date)

        # 宏观补充指标（可选，数据缺失时不影响核心判断）
        m1m2_spread, pmi_mfg = self._load_macro_indicators(trade_date)

        return {
            "market_trend":   market_trend,
            "tech_momentum":  tech_momentum,
            "size_spread":    size_spread,
            "northbound_5d":  northbound_5d,
            "m1m2_spread":    m1m2_spread,
            "pmi_mfg":        pmi_mfg,
        }

    def _load_macro_indicators(self, trade_date: str) -> tuple[float, float]:
        """从 macro_money_supply 和 macro_pmi 加载 M1-M2 剪刀差和制造业PMI。

        返回 (m1m2_spread, pmi_mfg)，数据缺失时返回 (nan, nan)。
        """
        import math
        m1m2_spread = math.nan
        pmi_mfg = math.nan

        # 取当月或上月（trade_date YYYYMMDD → 近两个月 YYYYMM）
        period = trade_date[:6]  # YYYYMM
        prev_period = f"{int(period) - 1:06d}" if period[-2:] != "01" else \
            f"{int(period[:4]) - 1}12"

        try:
            ms_df = self._repo.read_sql(
                """
                SELECT period_month, m1_yoy, m2_yoy
                FROM macro_money_supply
                WHERE period_month IN (:cur, :prev)
                ORDER BY period_month DESC
                LIMIT 1
                """,
                {"cur": period, "prev": prev_period},
            )
            if not ms_df.empty:
                row = ms_df.iloc[0]
                m1 = pd.to_numeric(row.get("m1_yoy"), errors="coerce")
                m2 = pd.to_numeric(row.get("m2_yoy"), errors="coerce")
                if pd.notna(m1) and pd.notna(m2):
                    m1m2_spread = float(m1) - float(m2)
        except Exception as e:
            logger.debug(f"[MarketRegime] M1/M2 读取失败: {e}")

        try:
            pmi_df = self._repo.read_sql(
                """
                SELECT period_month, pmi_mfg
                FROM macro_pmi
                WHERE period_month IN (:cur, :prev)
                ORDER BY period_month DESC
                LIMIT 1
                """,
                {"cur": period, "prev": prev_period},
            )
            if not pmi_df.empty:
                val = pd.to_numeric(pmi_df.iloc[0].get("pmi_mfg"), errors="coerce")
                if pd.notna(val):
                    pmi_mfg = float(val)
        except Exception as e:
            logger.debug(f"[MarketRegime] PMI 读取失败: {e}")

        return m1m2_spread, pmi_mfg

    def _load_northbound_5d(self, trade_date: str) -> float:
        """取最近5个交易日北向合计净流入（亿元）。"""
        try:
            df = self._repo.read_sql(
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
            logger.debug(f"[MarketRegime] 读取北向资金失败: {e}")
            return 0.0

    # ── 分类逻辑（按优先级）──────────────────────────────────

    @staticmethod
    def _classify(ind: dict) -> str:
        import math
        market_trend  = ind.get("market_trend", 0.0)
        tech_momentum = ind.get("tech_momentum", 0.0)
        size_spread   = ind.get("size_spread", 0.0)
        m1m2_spread   = ind.get("m1m2_spread", math.nan)
        pmi_mfg       = ind.get("pmi_mfg", math.nan)

        # 1. 熊市（最优先）— 大盘跌 >5% 或 PMI 持续收缩（<49）且大盘也偏弱
        pmi_contraction = (not math.isnan(pmi_mfg)) and pmi_mfg < 49.0
        if market_trend < -0.05:
            return "BEAR"
        if market_trend < -0.02 and pmi_contraction:
            return "BEAR"

        # 2. AI / 科技虹吸：创业板20日超额 > +10%
        if tech_momentum > 0.10:
            return "TECH_DOMINANT"

        # 3. 价值轮动：创业板显著落后，且 M1-M2 剪刀差改善（实体经济活跃）
        if tech_momentum < -0.10:
            return "VALUE_ROTATION"

        # 4. 普涨牛市：大盘上涨且小盘跑赢，宏观数据也支持
        macro_supportive = (
            (math.isnan(m1m2_spread) or m1m2_spread > -2.0)  # M1-M2 剪刀差不极端悲观
            and (math.isnan(pmi_mfg) or pmi_mfg >= 50.0)     # PMI 扩张区间
        )
        if market_trend > 0.03 and size_spread > 0 and macro_supportive:
            return "BROAD_BULL"
        if market_trend > 0.03 and size_spread > 0:
            return "BROAD_BULL"  # 即使宏观数据缺失，技术面支持也判断为 BROAD_BULL

        return "NEUTRAL"
