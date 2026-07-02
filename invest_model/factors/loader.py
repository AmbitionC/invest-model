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
        nb = self._northbound(trade_date, code_set)
        if not nb.empty:
            df = df.join(nb)
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

    # ── 北向持股占比变化（候选因子影子观察）──
    def _northbound(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        """nb_ratio_chg_20：北向持股占流通股本比例的 ~20 交易日变化（百分点）。

        无 stock_hk_hold 表/无数据（本地合成库、未回填时）返回空——候选因子
        整列 NaN，影子模式下对打分与组合无任何影响。
        """
        try:
            if not self.repo.table_exists("stock_hk_hold"):
                return pd.DataFrame()
        except Exception:  # noqa: BLE001
            return pd.DataFrame()
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y%m%d")
        df = self.repo.read_sql(
            "SELECT code, trade_date, ratio FROM stock_hk_hold "
            "WHERE trade_date>=:s AND trade_date<=:d",
            {"s": start, "d": trade_date},
        )
        if df.empty:
            return pd.DataFrame()
        df = df[df["code"].isin(codes)]
        df["ratio"] = pd.to_numeric(df["ratio"], errors="coerce")
        wide = df.pivot(index="trade_date", columns="code", values="ratio").sort_index()
        if len(wide) < 15:                      # 窗口内交易日太少，不出信号
            return pd.DataFrame()

        def _chg(s: pd.Series) -> float:
            v = s.dropna()
            # 至少覆盖窗口的 2/3，避免新进标的用极短区间放大变化
            return float(v.iloc[-1] - v.iloc[0]) if len(v) >= 10 else np.nan

        out = wide.apply(_chg).rename("nb_ratio_chg_20").to_frame()
        out.index.name = "code"
        return out

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
