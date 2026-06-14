"""原始市场特征构造器（Phase 2 重构）。

替代 features.py 中 65% 来自规则信号的旧特征集，改用直接来自市场原始数据的
20 维特征。与旧 FeatureBuilder 并存，供截面训练器 CrossSectionalTrainer 使用；
旧版 FeatureBuilder 继续为 v1/v1_oos 模型服务，无兼容性问题。

特征布局（共 20 维）：
  价格动量（4）: ret_1d / ret_5d / ret_10d / ret_20d
  量价技术（5）: vol_ratio_5d / vol_ratio_20d / rsi_14 / macd_hist / volatility_20
  趋势结构（4）: ma_alignment / price_pos_20d / ma60_bias / vol_trend_5d
  资金流（4）: elg_net_5d_pct / main_net_5d_pct / margin_chg_pct / north_flow_5d
  估值市值（3）: pe_ttm / pb / circ_mv_log

ETF 的资金流和估值列填 0（无个股层面数据）。

设计原则：
  - 全部从 DB 原始表读取，无信号系统依赖
  - 特征维度小（样本/特征比 >> 50:1）
  - 截面联合使用时可比较（同日截面归一化在 CrossSectionalTrainer 中处理）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

# ── 特征列定义 ──────────────────────────────────────────

RAW_FEATURE_COLUMNS: tuple[str, ...] = (
    # 价格动量 (4)
    "ret_1d",           # 1日收益率
    "ret_5d",           # 5日累计收益率
    "ret_10d",          # 10日累计收益率
    "ret_20d",          # 20日累计收益率
    # 量价技术 (5)
    "vol_ratio_5d",     # 5日均量 / 20日均量
    "vol_ratio_20d",    # 20日均量 / 60日均量
    "rsi_14",           # RSI(14)
    "macd_hist",        # MACD 柱状线
    "volatility_20",    # 20日历史波动率（日收益率标准差）
    # 趋势结构 (4)
    "ma_alignment",     # 多均线排列得分 MA5>10>20>60=+1, 反序=-1
    "price_pos_20d",    # 当前价格在近20日 [low, high] 区间内的位置 (0-1)
    "ma60_bias",        # 价格偏离 MA60 的比例
    "vol_trend_5d",     # 近5日均量 / 前5日均量 - 1（量能趋势）
    # 资金流 (4, 个股专属；ETF 填 0)
    "elg_net_5d_pct",   # 5日超大单净流入 / 流通市值
    "main_net_5d_pct",  # 5日主力净流入 / 流通市值
    "margin_chg_pct",   # 5日融资余额变化 / 流通市值
    "north_flow_5d",    # 北向近5日净流入（亿，市场整体，个股/ETF 共用同一值）
    # 估值市值 (3, 个股专属；ETF 填 0)
    "pe_ttm",           # 市盈率TTM（经过 winsorize 处理）
    "pb",               # 市净率（经过 winsorize 处理）
    "circ_mv_log",      # 流通市值对数（log10(亿元)）
)

# ETF 使用的列（省略个股专属的资金流和估值）
RAW_ETF_FEATURE_COLUMNS: tuple[str, ...] = (
    "ret_1d", "ret_5d", "ret_10d", "ret_20d",
    "vol_ratio_5d", "vol_ratio_20d",
    "rsi_14", "macd_hist", "volatility_20",
    "ma_alignment", "price_pos_20d", "ma60_bias", "vol_trend_5d",
    "north_flow_5d",
)


def raw_effective_columns(
    code: str,
    etf_codes: set[str] | None = None,
) -> list[str]:
    """根据标的类型返回应使用的原始特征列。"""
    if etf_codes and code in etf_codes:
        return list(RAW_ETF_FEATURE_COLUMNS)
    return list(RAW_FEATURE_COLUMNS)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        f = float(v)
        return f if np.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """对 Series 做 1%~99% winsorize，减少极端值影响。"""
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)


# ── 主类 ──────────────────────────────────────────────


@dataclass
class RawFeatureBuilder:
    """原始市场特征构造器。

    与 FeatureBuilder 接口兼容（build_single / build_history），
    但特征完全来自原始行情数据，无信号系统依赖。
    """

    engine: Engine
    ts_lookback_days: int = 80     # 技术指标计算所需 K 线数
    _cache: dict[str, dict] = field(default_factory=dict, repr=False, compare=False)

    def warmup(self, codes: list[str], start_date: str, end_date: str) -> None:
        """预加载批量历史数据到内存，加速后续 build_single。"""
        for code in codes:
            try:
                self._cache[code] = self._preload(code, start_date, end_date)
            except Exception as e:
                logger.warning(f"RawFeatureBuilder.warmup 失败 code={code}: {e}")

    def build_single(self, code: str, trade_date: str) -> pd.Series | None:
        """构造单日单票特征向量，返回 pd.Series（索引为 RAW_FEATURE_COLUMNS）。"""
        preload = self._cache.get(code)
        feats = self._build_one(code, trade_date, preload)
        if feats is None:
            return None
        return pd.Series(feats)[list(RAW_FEATURE_COLUMNS)]

    def build_history(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """构造历史特征矩阵，索引为 trade_date，列为 RAW_FEATURE_COLUMNS。"""
        from invest_model.repositories.calendar_repo import CalendarRepository
        cal = CalendarRepository(self.engine)
        dates = cal.get_trade_dates(start_date, end_date)
        if not dates:
            return pd.DataFrame(columns=list(RAW_FEATURE_COLUMNS))

        preload = self._preload(code, start_date, end_date)

        rows: list[dict] = []
        for dt in dates:
            feats = self._build_one(code, dt, preload)
            if feats is None:
                continue
            feats["trade_date"] = dt
            rows.append(feats)

        if not rows:
            return pd.DataFrame(columns=list(RAW_FEATURE_COLUMNS))

        df = pd.DataFrame(rows).set_index("trade_date")
        for c in RAW_FEATURE_COLUMNS:
            if c not in df.columns:
                df[c] = 0.0
        return df[list(RAW_FEATURE_COLUMNS)]

    # ── 内部 ─────────────────────────────────────

    def _preload(self, code: str, start_date: str, end_date: str) -> dict:
        """一次性从 DB 加载该标的历史区间所需的所有表。"""
        ext_start = (
            datetime.strptime(start_date, "%Y%m%d") - timedelta(days=self.ts_lookback_days * 2)
        ).strftime("%Y%m%d")

        repo = BaseRepository(self.engine)

        def safe_query(sql: str, params: dict) -> pd.DataFrame:
            try:
                return repo.read_sql(sql, params)
            except Exception as e:
                logger.debug(f"RawFeatureBuilder 查询失败 code={code}: {e}")
                return pd.DataFrame()

        # 日线（个股 + ETF 均从两个表尝试）
        daily = safe_query(
            "SELECT trade_date, close, volume, amount, pct_chg FROM stock_daily "
            "WHERE code = :c AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"c": code, "s": ext_start, "e": end_date},
        )
        if daily.empty:
            daily = safe_query(
                "SELECT trade_date, close, volume, amount, pct_chg FROM etf_daily "
                "WHERE code = :c AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
                {"c": code, "s": ext_start, "e": end_date},
            )

        # 技术指标（个股）
        technical = safe_query(
            "SELECT trade_date, rsi_14, macd_hist, ma5, ma10, ma20, ma60, ma60_bias, "
            "volatility_20, vol_ratio FROM stock_technical "
            "WHERE code = :c AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"c": code, "s": ext_start, "e": end_date},
        )

        # 资金流（个股）
        cashflow = safe_query(
            "SELECT trade_date, buy_lg_vol, sell_lg_vol, buy_elg_vol, sell_elg_vol "
            "FROM stock_cashflow "
            "WHERE code = :c AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"c": code, "s": ext_start, "e": end_date},
        )

        # 融资余额（个股）
        margin = safe_query(
            "SELECT trade_date, rzye FROM stock_margin "
            "WHERE code = :c AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"c": code, "s": ext_start, "e": end_date},
        )

        # 估值（个股）
        fundamental = safe_query(
            "SELECT trade_date, pe_ttm, pb, circ_mv FROM stock_fundamental "
            "WHERE code = :c AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"c": code, "s": ext_start, "e": end_date},
        )

        # 北向资金（市场整体）
        north = safe_query(
            "SELECT trade_date, north_money FROM stock_northbound_flow "
            "WHERE trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"s": ext_start, "e": end_date},
        )

        return {
            "daily": daily,
            "technical": technical,
            "cashflow": cashflow,
            "margin": margin,
            "fundamental": fundamental,
            "north": north,
        }

    def _build_one(self, code: str, trade_date: str, preload: dict | None) -> dict | None:
        """构造单日特征 dict。"""
        if preload is None:
            preload = self._preload(code, trade_date, trade_date)

        daily: pd.DataFrame = preload.get("daily", pd.DataFrame())
        if daily.empty:
            return None

        daily_until = daily[daily["trade_date"] <= trade_date]
        if daily_until.empty:
            return None
        daily_until = daily_until.sort_values("trade_date").tail(self.ts_lookback_days).reset_index(drop=True)

        feats: dict[str, float] = {c: 0.0 for c in RAW_FEATURE_COLUMNS}

        # 价格动量
        closes = pd.to_numeric(daily_until["close"], errors="coerce").dropna()
        pct_chgs = pd.to_numeric(daily_until.get("pct_chg", pd.Series()), errors="coerce").fillna(0)
        if len(closes) >= 2:
            feats["ret_1d"] = float(pct_chgs.iloc[-1]) / 100.0
        if len(closes) >= 6:
            c0, c5 = float(closes.iloc[-6]), float(closes.iloc[-1])
            feats["ret_5d"] = c5 / c0 - 1.0 if c0 > 0 else 0.0
        if len(closes) >= 11:
            c0, c10 = float(closes.iloc[-11]), float(closes.iloc[-1])
            feats["ret_10d"] = c10 / c0 - 1.0 if c0 > 0 else 0.0
        if len(closes) >= 21:
            c0, c20 = float(closes.iloc[-21]), float(closes.iloc[-1])
            feats["ret_20d"] = c20 / c0 - 1.0 if c0 > 0 else 0.0

        # 量比特征
        vols = pd.to_numeric(daily_until.get("volume", pd.Series()), errors="coerce").fillna(0)
        if len(vols) >= 60:
            v5  = float(vols.tail(5).mean())
            v20 = float(vols.tail(20).mean())
            v60 = float(vols.tail(60).mean())
            feats["vol_ratio_5d"]  = v5 / v20  if v20 > 0 else 1.0
            feats["vol_ratio_20d"] = v20 / v60 if v60 > 0 else 1.0
        elif len(vols) >= 20:
            v5  = float(vols.tail(5).mean())
            v20 = float(vols.tail(20).mean())
            feats["vol_ratio_5d"]  = v5 / v20 if v20 > 0 else 1.0

        # 量能趋势（近5日 vs 前5日）
        if len(vols) >= 10:
            v_new = float(vols.tail(5).mean())
            v_old = float(vols.iloc[-10:-5].mean())
            feats["vol_trend_5d"] = v_new / v_old - 1.0 if v_old > 0 else 0.0

        # 价格位置（20日区间）
        if len(closes) >= 5:
            w20 = closes.tail(20)
            hi, lo = float(w20.max()), float(w20.min())
            c = float(closes.iloc[-1])
            if hi > lo:
                feats["price_pos_20d"] = (c - lo) / (hi - lo)

        # 技术指标（优先从 stock_technical 读，否则从日线现算）
        tech: pd.DataFrame = preload.get("technical", pd.DataFrame())
        if not tech.empty:
            tech_until = tech[tech["trade_date"] <= trade_date].sort_values("trade_date")
            if not tech_until.empty:
                row = tech_until.iloc[-1]
                feats["rsi_14"]       = _safe_float(row.get("rsi_14"))
                feats["macd_hist"]    = _safe_float(row.get("macd_hist"))
                feats["volatility_20"]= _safe_float(row.get("volatility_20"))
                feats["ma60_bias"]    = _safe_float(row.get("ma60_bias"))

                ma5  = _safe_float(row.get("ma5"))
                ma10 = _safe_float(row.get("ma10"))
                ma20 = _safe_float(row.get("ma20"))
                ma60 = _safe_float(row.get("ma60"))
                if all(v > 0 for v in (ma5, ma10, ma20, ma60)):
                    ups = (ma5 > ma10) + (ma10 > ma20) + (ma20 > ma60)
                    dns = (ma5 < ma10) + (ma10 < ma20) + (ma20 < ma60)
                    feats["ma_alignment"] = (ups - dns) / 3.0
        else:
            # ETF 或无技术表的标的：从日线现场简算
            feats.update(self._compute_tech_from_daily(daily_until))

        # 北向资金（市场整体，个股/ETF 共用）
        north: pd.DataFrame = preload.get("north", pd.DataFrame())
        if not north.empty:
            n_until = north[north["trade_date"] <= trade_date].sort_values("trade_date").tail(5)
            if not n_until.empty:
                feats["north_flow_5d"] = float(
                    pd.to_numeric(n_until["north_money"], errors="coerce").fillna(0).sum()
                )

        # 资金流（个股）
        cashflow: pd.DataFrame = preload.get("cashflow", pd.DataFrame())
        if not cashflow.empty:
            cf_until = cashflow[cashflow["trade_date"] <= trade_date].sort_values("trade_date").tail(5)
            if not cf_until.empty and len(cf_until) >= 1:
                for col in ("buy_lg_vol", "sell_lg_vol", "buy_elg_vol", "sell_elg_vol"):
                    cf_until = cf_until.copy()
                    cf_until[col] = pd.to_numeric(cf_until[col], errors="coerce").fillna(0)

                elg_net = float(
                    (cf_until["buy_elg_vol"] - cf_until["sell_elg_vol"]).sum()
                )
                main_net = float(
                    (cf_until["buy_elg_vol"] + cf_until["buy_lg_vol"]
                     - cf_until["sell_elg_vol"] - cf_until["sell_lg_vol"]).sum()
                )

                # 归一化到流通市值
                circ_mv = self._get_circ_mv(preload, trade_date)
                if circ_mv > 0:
                    feats["elg_net_5d_pct"]  = elg_net  / (circ_mv * 1e8)
                    feats["main_net_5d_pct"] = main_net / (circ_mv * 1e8)

        # 融资余额变化（个股）
        margin: pd.DataFrame = preload.get("margin", pd.DataFrame())
        if not margin.empty:
            mg_until = margin[margin["trade_date"] <= trade_date].sort_values("trade_date").tail(6)
            if len(mg_until) >= 2:
                rz_new = _safe_float(pd.to_numeric(mg_until["rzye"], errors="coerce").iloc[-1])
                rz_old = _safe_float(pd.to_numeric(mg_until["rzye"], errors="coerce").iloc[0])
                circ_mv = self._get_circ_mv(preload, trade_date)
                if circ_mv > 0:
                    feats["margin_chg_pct"] = (rz_new - rz_old) / (circ_mv * 1e8)

        # 估值（个股）
        fund: pd.DataFrame = preload.get("fundamental", pd.DataFrame())
        if not fund.empty:
            f_until = fund[fund["trade_date"] <= trade_date].sort_values("trade_date")
            if not f_until.empty:
                row = f_until.iloc[-1]
                pe = _safe_float(row.get("pe_ttm"))
                pb = _safe_float(row.get("pb"))
                circ_mv = _safe_float(row.get("circ_mv"))
                # PE winsorize [-200, 200]，负值保留（亏损公司）
                feats["pe_ttm"] = float(np.clip(pe, -200.0, 200.0)) if pe != 0 else 0.0
                feats["pb"] = float(np.clip(pb, 0.0, 30.0)) if pb > 0 else 0.0
                if circ_mv > 0:
                    feats["circ_mv_log"] = float(np.log10(circ_mv / 1e4 + 1))  # 转换为亿后取log10

        return feats

    def _get_circ_mv(self, preload: dict, trade_date: str) -> float:
        """从 fundamental 表取流通市值（亿元）。"""
        fund: pd.DataFrame = preload.get("fundamental", pd.DataFrame())
        if fund.empty:
            return 0.0
        sub = fund[fund["trade_date"] <= trade_date].sort_values("trade_date")
        if sub.empty:
            return 0.0
        mv = _safe_float(sub.iloc[-1].get("circ_mv"))
        return mv / 1e4 if mv > 0 else 0.0  # circ_mv 单位是万元 → 亿元

    @staticmethod
    def _compute_tech_from_daily(df: pd.DataFrame) -> dict[str, float]:
        """无 stock_technical 数据时，从日线现场计算基础技术指标（ETF 专用）。"""
        out: dict[str, float] = {}
        if df.empty or len(df) < 5:
            return out

        closes = pd.to_numeric(df["close"], errors="coerce").dropna()
        if len(closes) < 5:
            return out

        # MA60 偏离
        if len(closes) >= 60:
            ma60 = float(closes.tail(60).mean())
            c = float(closes.iloc[-1])
            out["ma60_bias"] = (c - ma60) / ma60 if ma60 > 0 else 0.0

        # MA 排列（5/10/20/60）
        for w in (5, 10, 20, 60):
            if len(closes) >= w:
                out[f"_ma{w}"] = float(closes.tail(w).mean())
        ma5  = out.pop("_ma5",  0.0)
        ma10 = out.pop("_ma10", 0.0)
        ma20 = out.pop("_ma20", 0.0)
        ma60_v = out.pop("_ma60", 0.0)
        if all(v > 0 for v in (ma5, ma10, ma20, ma60_v)):
            ups = (ma5 > ma10) + (ma10 > ma20) + (ma20 > ma60_v)
            dns = (ma5 < ma10) + (ma10 < ma20) + (ma20 < ma60_v)
            out["ma_alignment"] = (ups - dns) / 3.0

        # 20日波动率
        if len(closes) >= 21:
            rets = closes.pct_change().dropna().tail(20)
            out["volatility_20"] = float(rets.std()) if len(rets) >= 10 else 0.0

        # RSI-14（简化计算）
        if len(closes) >= 15:
            delta = closes.diff().dropna().tail(14)
            gain = delta.clip(lower=0).mean()
            loss = (-delta.clip(upper=0)).mean()
            rs = gain / loss if loss > 0 else 100.0
            out["rsi_14"] = 100.0 - 100.0 / (1.0 + rs)

        return out
