"""个股标的发现 — 两条并行通道。

通道 A（日频）：大单异动追踪
  从 stock_cashflow 取超大单净流入 / 流通市值 > 1.5% 的标的，
  叠加量价突破验证（价格 > MA20 且成交量 > 60日均量 1.3 倍）。

通道 B（周频）：板块补涨发现
  取近 5 日涨幅最大的热点行业，在该行业内找涨幅垫底（最可能补涨）
  的个股，叠加融资余额增加确认机构认可。

两条通道的结果均写入 discovery_candidates 表（status='pending'），
有效期 20 个交易日，由人工确认后 promote 到 stock_pool。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


@dataclass
class StockCandidate:
    code: str
    name: str
    source: str          # 'cashflow_spike' | 'sector_laggard'
    score: float
    reason: str
    scan_date: str
    expire_date: str


class StockScreener:
    """个股标的发现扫描器（通道 A + 通道 B）。"""

    def __init__(
        self,
        engine: Engine,
        # 通道 A 参数
        cashflow_min_ratio: float = 0.015,  # 主力净流入 / 流通市值 > 1.5%
        price_ma_window: int = 20,           # 均线窗口
        vol_confirm_ratio: float = 1.3,      # 成交量需超过 60 日均量 1.3 倍
        vol_long_window: int = 60,
        # 通道 B 参数
        sector_top_n: int = 5,               # 取涨幅最大的前 N 个行业
        sector_laggard_pct: float = 0.30,    # 行业内涨幅最低的 30% 视为补涨候选
        min_circ_mv: float = 30.0,           # 流通市值最低 30 亿元
        margin_confirm_pct: float = 0.05,    # 融资余额环比增加 5% 为机构认可信号
    ):
        self.engine = engine
        self.repo = BaseRepository(engine)
        self.cashflow_min_ratio = cashflow_min_ratio
        self.price_ma_window = price_ma_window
        self.vol_confirm_ratio = vol_confirm_ratio
        self.vol_long_window = vol_long_window
        self.sector_top_n = sector_top_n
        self.sector_laggard_pct = sector_laggard_pct
        self.min_circ_mv = min_circ_mv           # 亿元
        self.margin_confirm_pct = margin_confirm_pct

    # ── 公开接口 ──────────────────────────────────────

    def scan_daily(
        self,
        trade_date: str,
        exclude_codes: list[str] | None = None,
    ) -> list[StockCandidate]:
        """通道 A：大单异动扫描（日频）。"""
        exclude = set(exclude_codes or [])
        expire = self._expire_date(trade_date, 20)

        try:
            candidates = self._channel_a(trade_date, exclude, expire)
        except Exception as e:
            logger.error(f"[Discovery/A] 扫描失败: {e}")
            candidates = []

        logger.info(f"[Discovery/A] {trade_date} 发现 {len(candidates)} 只大单异动候选")
        return candidates

    def scan_weekly(
        self,
        trade_date: str,
        exclude_codes: list[str] | None = None,
    ) -> list[StockCandidate]:
        """通道 B：板块补涨扫描（周频）。"""
        exclude = set(exclude_codes or [])
        expire = self._expire_date(trade_date, 20)

        try:
            candidates = self._channel_b(trade_date, exclude, expire)
        except Exception as e:
            logger.error(f"[Discovery/B] 扫描失败: {e}")
            candidates = []

        logger.info(f"[Discovery/B] {trade_date} 发现 {len(candidates)} 只补涨候选")
        return candidates

    # ── 通道 A：大单净流入异动 ──────────────────────

    def _channel_a(
        self,
        trade_date: str,
        exclude: set[str],
        expire: str,
    ) -> list[StockCandidate]:
        # 近 5 日超大单净流入汇总（buy_elg_vol - sell_elg_vol）
        # JOIN daily_basic 取 circ_mv（流通市值，万元）
        cf = self.repo.read_sql(
            """
            SELECT c.code,
                   SUM(c.buy_elg_vol - c.sell_elg_vol) AS net5d,
                   MAX(b.circ_mv) AS circ_mv
            FROM stock_cashflow c
            LEFT JOIN stock_fundamental b ON b.code = c.code AND b.trade_date = c.trade_date
            WHERE c.trade_date <= :td
              AND c.trade_date >= DATE_FORMAT(
                    DATE_SUB(STR_TO_DATE(:td,'%%Y%%m%%d'), INTERVAL 7 DAY),
                    '%%Y%%m%%d')
            GROUP BY c.code
            HAVING SUM(c.buy_elg_vol - c.sell_elg_vol) IS NOT NULL
            """,
            {"td": trade_date},
        )
        if cf.empty:
            return []

        cf["net5d"] = pd.to_numeric(cf["net5d"], errors="coerce").fillna(0)
        cf["circ_mv"] = pd.to_numeric(cf["circ_mv"], errors="coerce").fillna(0)

        # circ_mv 在 DB 中单位为万元，转亿元
        cf["circ_mv_yi"] = cf["circ_mv"] / 10_000
        cf = cf[cf["circ_mv_yi"] >= self.min_circ_mv]

        # 过滤已在池内
        cf = cf[~cf["code"].isin(exclude)]

        # 净流入 / 流通市值 > 阈值（net5d 单位为手×100，circ_mv 为万元，需对齐）
        cf["flow_ratio"] = cf["net5d"].abs() / (cf["circ_mv"] + 1e-6)
        hot = cf[cf["flow_ratio"] > self.cashflow_min_ratio].copy()
        if hot.empty:
            return []

        # 量价技术验证
        results: list[StockCandidate] = []
        for _, row in hot.iterrows():
            code = row["code"]
            ok, reason_tech = self._verify_price_volume(code, trade_date)
            if not ok:
                continue

            score = float(min(row["flow_ratio"] / 0.03, 1.0))  # 归一到 [0,1]
            reason = (
                f"超大单5日净流入占比{row['flow_ratio']:.1%}>{self.cashflow_min_ratio:.1%}; "
                + reason_tech
            )
            results.append(
                StockCandidate(
                    code=code,
                    name="",
                    source="cashflow_spike",
                    score=round(score, 4),
                    reason=reason,
                    scan_date=trade_date,
                    expire_date=expire,
                )
            )

        return results

    def _verify_price_volume(self, code: str, trade_date: str) -> tuple[bool, str]:
        """价格需站上 MA20，成交量需超过 60 日均量 1.3 倍。"""
        try:
            df = self.repo.read_sql(
                """
                SELECT trade_date, close, volume
                FROM stock_daily
                WHERE code = :code AND trade_date <= :td
                ORDER BY trade_date DESC
                LIMIT :n
                """,
                {"code": code, "td": trade_date, "n": self.vol_long_window + 5},
            )
        except Exception:
            return False, ""

        if df.empty or len(df) < self.price_ma_window:
            return False, ""

        df = df.sort_values("trade_date").reset_index(drop=True)
        closes = pd.to_numeric(df["close"], errors="coerce")
        volumes = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

        last_close = float(closes.iloc[-1])
        ma20 = float(closes.iloc[-self.price_ma_window:].mean())
        vol_short = float(volumes.iloc[-1])
        vol_long_avg = float(volumes.tail(self.vol_long_window).mean())

        price_ok = last_close > ma20
        vol_ok = vol_long_avg > 0 and vol_short > vol_long_avg * self.vol_confirm_ratio

        if not (price_ok and vol_ok):
            return False, ""

        return True, (
            f"价格{last_close:.2f}>{ma20:.2f}(MA{self.price_ma_window}); "
            f"量比{vol_short/vol_long_avg:.2f}x(>{self.vol_confirm_ratio}x)"
        )

    # ── 通道 B：板块补涨 ──────────────────────────────

    def _channel_b(
        self,
        trade_date: str,
        exclude: set[str],
        expire: str,
    ) -> list[StockCandidate]:
        # 取各行业近 5 日涨幅：JOIN stock_info 取 industry
        sector_perf = self.repo.read_sql(
            """
            SELECT si.industry,
                   AVG((s.close / s2.close) - 1) AS ret5d
            FROM stock_daily s
            JOIN stock_info si ON si.ts_code = s.code
            JOIN stock_daily s2 ON s2.code = s.code
            WHERE s.trade_date = :td
              AND s2.trade_date = (
                    SELECT MAX(d2.trade_date)
                    FROM stock_daily d2
                    WHERE d2.code = s.code
                      AND d2.trade_date < DATE_FORMAT(
                            DATE_SUB(STR_TO_DATE(:td,'%%Y%%m%%d'), INTERVAL 5 DAY),
                            '%%Y%%m%%d')
                  )
              AND si.industry IS NOT NULL AND si.industry != ''
            GROUP BY si.industry
            ORDER BY ret5d DESC
            """,
            {"td": trade_date},
        )
        if sector_perf.empty:
            return []

        sector_perf["ret5d"] = pd.to_numeric(sector_perf["ret5d"], errors="coerce")
        top_sectors = (
            sector_perf.dropna(subset=["ret5d"])
            .head(self.sector_top_n)["industry"]
            .tolist()
        )
        if not top_sectors:
            return []

        results: list[StockCandidate] = []
        for sector in top_sectors:
            candidates = self._find_laggards(
                sector=sector,
                trade_date=trade_date,
                exclude=exclude,
                expire=expire,
            )
            results.extend(candidates)

        return results

    def _find_laggards(
        self,
        sector: str,
        trade_date: str,
        exclude: set[str],
        expire: str,
    ) -> list[StockCandidate]:
        """在热点行业内找涨幅垫底的个股（补涨逻辑）。"""
        try:
            df = self.repo.read_sql(
                """
                SELECT s.code, s.close,
                       (s.close / s2.close - 1) AS ret5d,
                       b.circ_mv
                FROM stock_daily s
                JOIN stock_info si ON si.ts_code = s.code
                JOIN stock_daily s2 ON s2.code = s.code
                LEFT JOIN daily_basic b ON b.code = s.code AND b.trade_date = s.trade_date
                WHERE s.trade_date = :td
                  AND s2.trade_date = (
                        SELECT MAX(d2.trade_date)
                        FROM stock_daily d2
                        WHERE d2.code = s.code
                          AND d2.trade_date < DATE_FORMAT(
                                DATE_SUB(STR_TO_DATE(:td,'%%Y%%m%%d'), INTERVAL 5 DAY),
                                '%%Y%%m%%d')
                      )
                  AND si.industry = :sector
                ORDER BY ret5d ASC
                """,
                {"td": trade_date, "sector": sector},
            )
        except Exception as e:
            logger.debug(f"[Discovery/B] {sector} 查询失败: {e}")
            return []

        if df.empty:
            return []

        df["ret5d"] = pd.to_numeric(df["ret5d"], errors="coerce")
        df["circ_mv"] = pd.to_numeric(df["circ_mv"], errors="coerce").fillna(0)
        df = df.dropna(subset=["ret5d"])
        df = df[df["circ_mv"] / 10_000 >= self.min_circ_mv]  # 转亿元
        df = df[~df["code"].isin(exclude)]

        if df.empty:
            return []

        # 取涨幅最低的 30%（补涨候选）
        cutoff = int(len(df) * self.sector_laggard_pct) + 1
        laggards = df.head(cutoff)

        results: list[StockCandidate] = []
        for _, row in laggards.iterrows():
            code = row["code"]
            # 融资余额确认（机构认可）
            margin_ok, margin_reason = self._check_margin(code, trade_date)
            if not margin_ok:
                continue

            idx = laggards.index.get_loc(row.name)
            score = round(0.5 + (1 - idx / len(laggards)) * 0.5, 4)
            reason = (
                f"行业[{sector}]热点补涨候选; 5日涨幅{row['ret5d']:.1%}偏低; "
                + margin_reason
            )
            results.append(
                StockCandidate(
                    code=code,
                    name="",
                    source="sector_laggard",
                    score=score,
                    reason=reason,
                    scan_date=trade_date,
                    expire_date=expire,
                )
            )

        return results

    def _check_margin(self, code: str, trade_date: str) -> tuple[bool, str]:
        """融资余额近 10 日环比增加 5% 为机构认可信号。"""
        try:
            df = self.repo.read_sql(
                """
                SELECT trade_date, rzye
                FROM margin_detail
                WHERE code = :code AND trade_date <= :td
                ORDER BY trade_date DESC
                LIMIT 12
                """,
                {"code": code, "td": trade_date},
            )
        except Exception:
            return True, "融资数据不可用"

        if df.empty or len(df) < 2:
            return True, "融资数据不足"

        df = df.sort_values("trade_date")
        rzye = pd.to_numeric(df["rzye"], errors="coerce").dropna()
        if len(rzye) < 2 or rzye.iloc[0] <= 0:
            return True, "融资基准为零"

        chg = float(rzye.iloc[-1]) / float(rzye.iloc[0]) - 1.0
        if chg < self.margin_confirm_pct:
            return False, f"融资余额变化{chg:.1%}<{self.margin_confirm_pct:.0%}"

        return True, f"融资余额增加{chg:.1%}(机构认可)"

    # ── 工具 ─────────────────────────────────────────

    @staticmethod
    def _expire_date(trade_date: str, days: int) -> str:
        """从 trade_date 向后推 days 个自然日（近似20个交易日 ≈ 28个自然日）。"""
        dt = datetime.strptime(trade_date, "%Y%m%d")
        expire_dt = dt + timedelta(days=int(days * 1.4))
        return expire_dt.strftime("%Y%m%d")
