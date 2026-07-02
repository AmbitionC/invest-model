"""FactorDataLoader：组装某交易日的原始截面（价格衍生 + 估值 + point-in-time 财务）。

返回 DataFrame，index=code，列含：
  industry, circ_mv, pe_ttm, pb, ps_ttm, turnover_rate,
  roe, roa, gross_margin, revenue_yoy, profit_yoy,
  mom_20, mom_60, mom_120, ret_5, vol_20
缺失数据以 NaN 体现，由后续处理器/合成层稳健处理。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

_PRICE_LOOKBACK_DAYS = 200  # 自然日回看（覆盖 120 交易日动量）


class FactorDataLoader:
    def __init__(self, engine):
        self.engine = engine
        self.repo = BaseRepository(engine)

    def load_cross_section(self, trade_date: str, codes: list[str]) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()
        code_set = set(codes)

        price_feat = self._price_features(trade_date, code_set)
        valuation = self._valuation(trade_date, code_set)
        fina = self._fina_pit(trade_date, code_set)
        info = self.repo.read_sql(
            "SELECT ts_code AS code, industry FROM stock_info"
        ).set_index("code")

        df = pd.DataFrame(index=sorted(code_set))
        df.index.name = "code"
        df = df.join(info[["industry"]])
        df = df.join(valuation)
        df = df.join(fina)
        df = df.join(price_feat)
        return df

    # ── 价格衍生因子 ──
    def _price_features(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=_PRICE_LOOKBACK_DAYS)).strftime("%Y%m%d")
        px = self.repo.read_sql(
            "SELECT code, trade_date, close FROM stock_daily "
            "WHERE trade_date>=:s AND trade_date<=:d",
            {"s": start, "d": trade_date},
        )
        if px.empty:
            return pd.DataFrame()
        px["close"] = pd.to_numeric(px["close"], errors="coerce")
        px = px[px["code"].isin(codes)]
        wide = px.pivot(index="trade_date", columns="code", values="close").sort_index()

        def mom(n: int, skip: int = 0) -> pd.Series:
            """近 n 日动量；skip>0 时跳过最近 skip 日（剔除近端反转段）。"""
            if len(wide) <= n + skip:
                return pd.Series(np.nan, index=wide.columns)
            return wide.iloc[-1 - skip] / wide.iloc[-1 - skip - n] - 1.0

        rets = wide.pct_change()
        feat = pd.DataFrame({
            "mom_20": mom(20),
            # 中期动量跳过最近 5 日：与 reversal_5 解耦（否则两因子在同一段
            # 数据上方向相反地打架），也是经典动量（12-1 型）的标准构造。
            "mom_60": mom(60, skip=5),
            "mom_120": mom(120, skip=5),
            "ret_5": mom(5),
            "vol_20": rets.tail(20).std(),
        })
        feat.index.name = "code"
        return feat

    # ── 估值 ──
    def _valuation(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        v = self.repo.read_sql(
            "SELECT code, pe_ttm, pb, ps_ttm, circ_mv, turnover_rate "
            "FROM stock_fundamental WHERE trade_date=:d",
            {"d": trade_date},
        )
        if v.empty:
            return pd.DataFrame()
        v = v[v["code"].isin(codes)].set_index("code")
        for c in v.columns:
            v[c] = pd.to_numeric(v[c], errors="coerce")
        return v

    # ── point-in-time 财务（ann_date <= t 的最新一期，且不早于 t-540 天）──
    def _fina_pit(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        # 时效下限：超过 ~1.5 年未披露视为失效（停止披露常是退市前兆，
        # 陈年 ROE 不应继续参与打分），顺带避免每次全表扫描。
        stale = (pd.to_datetime(trade_date) - pd.Timedelta(days=540)).strftime("%Y%m%d")
        f = self.repo.read_sql(
            "SELECT code, ann_date, report_date, roe, roa, gross_margin, "
            "revenue_yoy, profit_yoy FROM stock_fina_indicator "
            "WHERE ann_date<=:d AND ann_date>=:lo",
            {"d": trade_date, "lo": stale},
        )
        if f.empty:
            return pd.DataFrame()
        f = f[f["code"].isin(codes)].copy()
        # 每只票取 ann_date 最新一行（同 ann_date 取 report_date 最新）
        f = f.sort_values(["code", "ann_date", "report_date"])
        latest = f.groupby("code").tail(1).set_index("code")
        cols = ["roe", "roa", "gross_margin", "revenue_yoy", "profit_yoy"]
        for c in cols:
            latest[c] = pd.to_numeric(latest[c], errors="coerce")
        return latest[cols]
