"""FactorDataLoader：组装某交易日的原始截面（价格衍生 + 估值 + point-in-time 财务）。

返回 DataFrame，index=code，列含：
  industry, total_mv, circ_mv, pe_ttm, pb, ps_ttm, turnover_rate, dv_ttm,
  roe, roa, gross_margin, revenue_yoy, profit_yoy, q_profit_yoy, q_profit_accel,
  goodwill, eq_exc_min, insider_conviction,
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
        adv = self._advisor_stance(trade_date, code_set)
        if not adv.empty:
            df = df.join(adv)
        ext = self._fina_ext_pit(trade_date, code_set)
        if not ext.empty:
            df = df.join(ext)
        ins = self._insider(trade_date, code_set)
        if not ins.empty:
            df = df.join(ins)
        return df

    # ── 投顾立场（候选因子，影子观察）──
    def _advisor_stance(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        """adv_stance：投顾立场信号（候选因子）。取 rec_date<=trade_date 且未失效的最新一条
        advisor_reco，按 方向×分级 打分：long A=3/B=2/C=1，reduce=-1，avoid/exit=-2；
        未覆盖标的为 NaN（截面中性）。E6 实测：立场对量化 rank_pct 有独立增量偏 IC（+0.077,
        聚类稳健 t=+2.2，issue #14）。此处作候选因子起累积多期 IC，晋升门槛见 CANDIDATE 说明。
        依赖 advisor_reco；无表/无数据整列缺省（影子模式无下游影响，PIT：只用 rec_date<=当日）。"""
        if not self.repo.table_exists("advisor_reco"):
            return pd.DataFrame()
        df = self.repo.read_sql(
            "SELECT rec_date, code, grade, direction, valid_until FROM advisor_reco "
            "WHERE rec_date<=:d", {"d": trade_date})
        if df.empty:
            return pd.DataFrame()
        df = df[df["code"].isin(codes)]
        if df.empty:
            return pd.DataFrame()

        def _valid(vu: object) -> bool:
            s = str(vu or "").strip()
            return (not s) or (trade_date <= s)

        df = df[df["valid_until"].apply(_valid)]
        if df.empty:
            return pd.DataFrame()
        df = df.sort_values("rec_date").groupby("code", as_index=False).tail(1)

        def _score(direction: object, grade: object) -> float:
            d = str(direction or "").strip().lower()
            g = str(grade or "").strip().upper()
            if d == "long":
                return {"A": 3.0, "B": 2.0, "C": 1.0}.get(g, 1.0)
            return {"reduce": -1.0, "avoid": -2.0, "exit": -2.0}.get(d, 0.0)

        df["adv_stance"] = [_score(d, g) for d, g in zip(df["direction"], df["grade"])]
        out = df.set_index("code")[["adv_stance"]]
        out.index.name = "code"
        return out

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
            "SELECT code, pe_ttm, pb, ps_ttm, total_mv, circ_mv, turnover_rate, dv_ttm "
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

    # ── 报表扩展项 PIT（扣商誉估值原料；候选因子 bp_ex_goodwill）──
    def _fina_ext_pit(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        """goodwill / eq_exc_min（归母净资产）的 point-in-time 最新一期。

        无 stock_fina_ext 表/无数据时返回空——候选因子整列 NaN（影子无下游影响）。
        """
        try:
            if not self.repo.table_exists("stock_fina_ext"):
                return pd.DataFrame()
        except Exception:  # noqa: BLE001
            return pd.DataFrame()
        stale = (pd.to_datetime(trade_date) - pd.Timedelta(days=540)).strftime("%Y%m%d")
        f = self.repo.read_sql(
            "SELECT code, ann_date, report_date, goodwill, eq_exc_min "
            "FROM stock_fina_ext WHERE ann_date<=:d AND ann_date>=:lo",
            {"d": trade_date, "lo": stale},
        )
        if f.empty:
            return pd.DataFrame()
        f = f[f["code"].isin(codes)].copy()
        f = f.sort_values(["code", "ann_date", "report_date"])
        latest = f.groupby("code").tail(1).set_index("code")
        for c in ("goodwill", "eq_exc_min"):
            latest[c] = pd.to_numeric(latest[c], errors="coerce")
        return latest[["goodwill", "eq_exc_min"]]

    # ── 高管增持信号（候选因子 insider_conviction，影子观察）──
    def _insider(self, trade_date: str, codes: set[str], window_days: int = 90) -> pd.DataFrame:
        """近 90 日重要股东/高管净增持强度。

        跟庄方法论核心洞察（已验证）：信号可信度 = 押注 ÷ 身家——高管押的是
        辛苦钱、只为赚钱且最懂公司，约强于大股东 20 倍（大股东增持常另有目的：
        质押护盘/增发保驾/争控股权）。实现：Σ 方向(增+/减−) × 身份权重(高管 G=20,
        其余=1) × 占流通比例(%)，PIT 只取 ann_date<=当日。
        无 holder_trade 表/无数据返回空（影子模式无下游影响）。
        """
        try:
            if not self.repo.table_exists("holder_trade"):
                return pd.DataFrame()
        except Exception:  # noqa: BLE001
            return pd.DataFrame()
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=window_days)).strftime("%Y%m%d")
        df = self.repo.read_sql(
            "SELECT code, ann_date, holder_type, in_de, change_ratio FROM holder_trade "
            "WHERE ann_date>=:s AND ann_date<=:d",
            {"s": start, "d": trade_date},
        )
        if df.empty:
            return pd.DataFrame()
        df = df[df["code"].isin(codes)].copy()
        if df.empty:
            return pd.DataFrame()
        ratio = pd.to_numeric(df["change_ratio"], errors="coerce").fillna(0.0)
        sign = df["in_de"].astype(str).str.upper().map({"IN": 1.0, "DE": -1.0}).fillna(0.0)
        w = df["holder_type"].astype(str).str.upper().map(
            lambda t: 20.0 if t == "G" else 1.0)
        df["conv"] = sign * w * ratio
        out = df.groupby("code")["conv"].sum().rename("insider_conviction").to_frame()
        out.index.name = "code"
        return out

    # ── point-in-time 财务（ann_date <= t 的最新一期，且不早于 t-540 天）──
    def _fina_pit(self, trade_date: str, codes: set[str]) -> pd.DataFrame:
        # 时效下限：超过 ~1.5 年未披露视为失效（停止披露常是退市前兆，
        # 陈年 ROE 不应继续参与打分），顺带避免每次全表扫描。
        stale = (pd.to_datetime(trade_date) - pd.Timedelta(days=540)).strftime("%Y%m%d")
        f = self.repo.read_sql(
            "SELECT code, ann_date, report_date, roe, roa, gross_margin, "
            "revenue_yoy, profit_yoy, q_profit_yoy FROM stock_fina_indicator "
            "WHERE ann_date<=:d AND ann_date>=:lo",
            {"d": trade_date, "lo": stale},
        )
        if f.empty:
            return pd.DataFrame()
        f = f[f["code"].isin(codes)].copy()
        # 每只票取 ann_date 最新一行（同 ann_date 取 report_date 最新）
        f = f.sort_values(["code", "ann_date", "report_date"])
        f["q_profit_yoy"] = pd.to_numeric(f["q_profit_yoy"], errors="coerce")
        latest = f.groupby("code").tail(1).set_index("code")
        cols = ["roe", "roa", "gross_margin", "revenue_yoy", "profit_yoy", "q_profit_yoy"]
        for c in cols:
            latest[c] = pd.to_numeric(latest[c], errors="coerce")
        latest = latest[cols].copy()
        # 增速二阶导（候选因子 growth_accel 原料）：最近两期单季净利同比的一阶差分。
        # 财报跟踪法：+50% 若上期 +80% 是减速的坏消息——看增速的变化而非增长本身。
        last2 = f.dropna(subset=["q_profit_yoy"]).groupby("code").tail(2)
        accel = last2.groupby("code")["q_profit_yoy"].agg(
            lambda s: float(s.iloc[-1] - s.iloc[0]) if len(s) == 2 else np.nan)
        latest["q_profit_accel"] = accel.reindex(latest.index)
        return latest
