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

        def mom(n: int) -> pd.Series:
            if len(wide) <= n:
                return pd.Series(np.nan, index=wide.columns)
            return wide.iloc[-1] / wide.iloc[-1 - n] - 1.0

        rets = wide.pct_change()
        feat = pd.DataFrame({
            "mom_20": mom(20),
            "mom_60": mom(60),
            "mom_120": mom(120),
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

    # ── point-in-time 财务（ann_date <= t 的最新一期）──
    def _fina_pit(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        f = self.repo.read_sql(
            "SELECT code, ann_date, report_date, roe, roa, gross_margin, "
            "revenue_yoy, profit_yoy FROM stock_fina_indicator "
            "WHERE ann_date<=:d",
            {"d": trade_date},
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
