"""ML 特征工程。

基于现有 signals/registry + advisor.triggers + advisor.trend_context + composite 时序，
为 XGBoost 拼接每日特征矩阵。

特征布局（共约 60 维）：
  1. 信号分（31 维）：从 signals.registry 的 Generator 输出，按 SIGNAL_NAME_ORDER 固定顺序
  2. Trigger 连续量（8 维）：MA60 偏离/局部极值距离/量价背离比例/突破方向
  3. TrendContext 编码（6 维）：方向 ordinal、阶段 ordinal、短期方向 ordinal、MA20 斜率、5日收益、长趋势综合
  4. Composite 时序（8 维）：lag1/3/5、delta_3d/5d、std_5d、动量符号变化、roll_max
  5. 原始技术指标（7 维）：rsi_14、macd_hist、ma60_bias、volatility_20、momentum_20、turnover_rate、vol_ratio

模块对外暴露 FeatureBuilder，可批量构造历史特征矩阵或单日切片。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.repositories.technical_repo import TechnicalRepository
from invest_model.repositories.etf_repo import ETFRepository

logger = get_logger()

# ── 信号名顺序（必须与 signals/registry.py 中的 Generator 输出保持一致）──

SIGNAL_NAME_ORDER: tuple[str, ...] = (
    # tech_trend
    "macd_trend",
    "macd_hist_direction",
    "macd_momentum_decay",
    "momentum_20",
    "vol_ratio",
    "price_momentum_5d",
    "price_momentum_20d",
    "macd_bottom_divergence",
    "macd_top_divergence",
    "rsi_bottom_divergence",
    "rsi_top_divergence",
    "volume_breakout",
    "volume_pullback",
    "volume_dry_up",
    "volume_climax",
    # tech_reversal
    "rsi_extreme",
    "boll_position",
    "ma_bias",
    "volatility_20",
    # fundamental
    "pe_rank",
    "pb_rank",
    "roe_level",
    "revenue_growth",
    "debt_level",
    # money_flow
    "main_inflow_5d",
    "elg_ratio",
    "margin_delta_5d",
    "northbound_net_5d",
    # sentiment
    "turnover_extreme",
    "holder_count_trend",
    "insider_net_30d",
)

TRIGGER_FEATURE_NAMES: tuple[str, ...] = (
    "trig_ma60_bias",          # close 相对 ma60 的偏离比例（带方向）
    "trig_pos_in_20d",         # 价格在 20 日 [low, high] 区间的位置 0-1
    "trig_dist_from_high",     # 距 20 日高点的相对距离 0-1
    "trig_dist_from_low",      # 距 20 日低点的相对距离 0-1
    "trig_vol_change",         # 后半段量比前半段量的变化率
    "trig_break_above",        # 是否突破 ma20/60，1/0
    "trig_break_below",        # 是否跌破 ma20/60，1/0
    "trig_price_trend_10d",    # 10 日价格变化（用于背离判断）
)

TREND_FEATURE_NAMES: tuple[str, ...] = (
    "trend_dir_ord",           # 中期方向 ordinal: -2~+2
    "trend_phase_ord",         # 阶段 ordinal: 0=stable / 1=accel / -1=decel / -2=reversal
    "trend_short_dir_ord",     # 短期方向 ordinal: -1/0/+1
    "trend_ma_slope",          # MA20 斜率（连续）
    "trend_ret_5d",            # 近 5 日累计收益率
    "trend_ma_alignment",      # 多周期 MA 排列得分（综合 MA5/10/20/60）
)

COMPOSITE_TS_NAMES: tuple[str, ...] = (
    "comp_lag1",
    "comp_lag3",
    "comp_lag5",
    "comp_delta_3d",          # 近 3 日 - 之前 3 日
    "comp_delta_5d",          # 近 5 日 - 之前 5 日
    "comp_std_5d",            # 近 5 日 std
    "comp_sign_changes_10d",  # 近 10 日符号反转次数
    "comp_roll_max_10d",      # 近 10 日 |composite| 最大值
)

TECH_RAW_NAMES: tuple[str, ...] = (
    "tech_rsi14",
    "tech_macd_hist",
    "tech_ma60_bias",
    "tech_volatility_20",
    "tech_momentum_20",
    "tech_turnover_rate",     # 来自 stock_fundamental
    "tech_vol_ratio",
)

FEATURE_COLUMNS: tuple[str, ...] = (
    *SIGNAL_NAME_ORDER,
    *TRIGGER_FEATURE_NAMES,
    *TREND_FEATURE_NAMES,
    *COMPOSITE_TS_NAMES,
    *TECH_RAW_NAMES,
)

# ETF 走"纯价格 + 量"通道：剔除依赖 stock_signal_snapshot / stock_composite_score /
# stock_fundamental 的列。stock_technical 行虽然为空，但 FeatureBuilder._compute_basic_ma
# 会用 invest_model.technical.calculator.compute_technical 现场算齐 RSI/MACD/ma60_bias/
# volatility_20/momentum_20/vol_ratio，因此把这些"纯价格衍生"的 TECH_RAW 列也纳入 ETF
# 模型，比原先 14 维 trigger+trend 表达力强很多（避免训练后所有 horizon IC≈0、
# 推理 score 永远低于 buy_threshold 导致 ETF 永远 hold 无交易）。
# 唯一排除的是 tech_turnover_rate（依赖 stock_fundamental，ETF 无此表）。
_ETF_TECH_RAW_NAMES: tuple[str, ...] = tuple(
    c for c in TECH_RAW_NAMES if c != "tech_turnover_rate"
)

ETF_FEATURE_COLUMNS: tuple[str, ...] = (
    *TRIGGER_FEATURE_NAMES,
    *TREND_FEATURE_NAMES,
    *_ETF_TECH_RAW_NAMES,
)


def effective_feature_columns(
    code: str,
    etf_codes: set[str] | None = None,
) -> list[str]:
    """根据标的类型返回该 code 应使用的特征列。

    - ETF（在 etf_codes 中）→ 只用 trigger + trend（14 维），避免常年为 0 的列拖累模型
    - 其他个股 → 完整 60 维 FEATURE_COLUMNS
    """
    if etf_codes and code in etf_codes:
        return list(ETF_FEATURE_COLUMNS)
    return list(FEATURE_COLUMNS)


# ── helpers ──────────────────────────────────────────


def _safe_float(v) -> float:
    try:
        if v is None:
            return 0.0
        v = float(v)
        return v if np.isfinite(v) else 0.0
    except (TypeError, ValueError):
        return 0.0


_DIR_ORD = {
    "bullish": 2,
    "bullish_weak": 1,
    "neutral": 0,
    "bearish_weak": -1,
    "bearish": -2,
}

_PHASE_ORD = {
    "acceleration": 1,
    "stable": 0,
    "deceleration": -1,
    "reversal": -2,
}

_SHORT_DIR_ORD = {"bullish": 1, "neutral": 0, "bearish": -1}


# ── 子计算 ──────────────────────────────────────────


def _compute_trigger_features(df_ts: pd.DataFrame) -> dict[str, float]:
    """从日线+技术合并表的最后一行提取触发器底层连续量。"""
    out = {k: 0.0 for k in TRIGGER_FEATURE_NAMES}
    if df_ts is None or df_ts.empty or len(df_ts) < 2:
        return out

    row = df_ts.iloc[-1]
    close = _safe_float(row.get("close"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))

    # MA60 偏离
    if close > 0 and ma60 > 0:
        out["trig_ma60_bias"] = (close - ma60) / ma60

    # 20 日区间位置
    closes = pd.to_numeric(df_ts["close"], errors="coerce").dropna()
    if len(closes) >= 5:
        window = closes.tail(20)
        hi = float(window.max())
        lo = float(window.min())
        rng = hi - lo
        if rng > 0 and close > 0:
            out["trig_pos_in_20d"] = (close - lo) / rng
            out["trig_dist_from_high"] = (hi - close) / rng
            out["trig_dist_from_low"] = (close - lo) / rng

    # 量价（后半段量 / 前半段量 - 1）
    if "volume" in df_ts.columns and len(df_ts) >= 10:
        recent = df_ts.tail(10).copy()
        vols = pd.to_numeric(recent["volume"], errors="coerce")
        if not vols.isna().all():
            half = len(vols) // 2
            v1 = float(vols.iloc[:half].mean())
            v2 = float(vols.iloc[half:].mean())
            if v1 > 0:
                out["trig_vol_change"] = (v2 - v1) / v1

    # 均线突破
    if len(df_ts) >= 2:
        prev = df_ts.iloc[-2]
        c0 = _safe_float(prev.get("close"))
        m20p = _safe_float(prev.get("ma20"))
        m60p = _safe_float(prev.get("ma60"))
        if c0 > 0 and m20p > 0 and ma20 > 0:
            if c0 <= m20p and close > ma20:
                out["trig_break_above"] = 1.0
            elif c0 >= m20p and close < ma20:
                out["trig_break_below"] = 1.0
        if c0 > 0 and m60p > 0 and ma60 > 0:
            if c0 <= m60p and close > ma60:
                out["trig_break_above"] = 1.0
            elif c0 >= m60p and close < ma60:
                out["trig_break_below"] = 1.0

    # 10 日价格变化（背离辅助特征）
    if len(closes) >= 11:
        p_now = float(closes.iloc[-1])
        p_old = float(closes.iloc[-11])
        if p_old > 0:
            out["trig_price_trend_10d"] = (p_now - p_old) / p_old

    return out


def _compute_trend_features(df_ts: pd.DataFrame) -> dict[str, float]:
    """计算 trend ordinal 编码 + 多周期 MA 排列得分。"""
    out = {k: 0.0 for k in TREND_FEATURE_NAMES}
    if df_ts is None or df_ts.empty or len(df_ts) < 5:
        return out

    row = df_ts.iloc[-1]
    close = _safe_float(row.get("close"))
    ma5 = _safe_float(row.get("ma5"))
    ma10 = _safe_float(row.get("ma10"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))

    if close <= 0 or ma20 <= 0 or ma60 <= 0:
        return out

    # 中期方向
    if close > ma20 and ma20 > ma60:
        direction = "bullish"
    elif close < ma20 and ma20 < ma60:
        direction = "bearish"
    elif close > ma60:
        direction = "bullish_weak"
    elif close < ma60:
        direction = "bearish_weak"
    else:
        direction = "neutral"

    # 短期方向
    closes = pd.to_numeric(df_ts["close"], errors="coerce").dropna()
    ret_5d = 0.0
    if len(closes) >= 6:
        p0 = float(closes.iloc[-6])
        if p0 > 0:
            ret_5d = (close - p0) / p0

    if ma5 > 0 and ma10 > 0:
        if close > ma5 and ma5 > ma10:
            short_dir = "bullish"
        elif close < ma5 and ma5 < ma10:
            short_dir = "bearish"
        else:
            short_dir = "bullish" if ret_5d > 0.01 else "bearish" if ret_5d < -0.01 else "neutral"
    else:
        short_dir = "bullish" if ret_5d > 0.01 else "bearish" if ret_5d < -0.01 else "neutral"

    # MA20 斜率（连续值）
    ma_slope = 0.0
    if len(df_ts) >= 6:
        ma20_prev = _safe_float(df_ts.iloc[-6].get("ma20"))
        if ma20_prev > 0:
            ma_slope = (ma20 - ma20_prev) / ma20_prev

    # 阶段判定（同 advisor._identify_phase 简化版，转 ordinal）
    is_bull = direction in ("bullish", "bullish_weak")
    is_bear = direction in ("bearish", "bearish_weak")
    if is_bull:
        if short_dir == "bullish" and ma_slope > 0.005:
            phase = "acceleration"
        elif short_dir == "bullish":
            phase = "stable"
        elif ma_slope > 0.005:
            phase = "stable"
        elif ma_slope < -0.005:
            phase = "reversal"
        elif ret_5d < -0.04:
            phase = "deceleration"
        else:
            phase = "stable"
    elif is_bear:
        if short_dir == "bearish" and ma_slope < -0.005:
            phase = "acceleration"
        elif short_dir == "bearish":
            phase = "stable"
        elif ma_slope < -0.005:
            phase = "stable"
        elif ma_slope > 0.005:
            phase = "reversal"
        elif ret_5d > 0.04:
            phase = "deceleration"
        else:
            phase = "stable"
    else:
        phase = "stable"

    # 多周期 MA 排列分（5 → 60 升序排列得正分，反之负分）
    ma_align = 0.0
    if all(v > 0 for v in (ma5, ma10, ma20, ma60)):
        ups = (ma5 > ma10) + (ma10 > ma20) + (ma20 > ma60)
        downs = (ma5 < ma10) + (ma10 < ma20) + (ma20 < ma60)
        ma_align = (ups - downs) / 3.0

    out["trend_dir_ord"] = float(_DIR_ORD.get(direction, 0))
    out["trend_phase_ord"] = float(_PHASE_ORD.get(phase, 0))
    out["trend_short_dir_ord"] = float(_SHORT_DIR_ORD.get(short_dir, 0))
    out["trend_ma_slope"] = ma_slope
    out["trend_ret_5d"] = ret_5d
    out["trend_ma_alignment"] = ma_align
    return out


def _compute_composite_ts_features(comp_hist: list[float]) -> dict[str, float]:
    """从近 N 天 composite 序列构造时序衍生特征。"""
    out = {k: 0.0 for k in COMPOSITE_TS_NAMES}
    if not comp_hist:
        return out

    arr = np.asarray(comp_hist, dtype=float)
    n = len(arr)

    if n >= 1:
        out["comp_lag1"] = float(arr[-1])
    if n >= 4:
        out["comp_lag3"] = float(arr[-4])
    if n >= 6:
        out["comp_lag5"] = float(arr[-6])

    if n >= 6:
        recent3 = arr[-3:].mean()
        prev3 = arr[-6:-3].mean()
        out["comp_delta_3d"] = float(recent3 - prev3)

    if n >= 10:
        recent5 = arr[-5:].mean()
        prev5 = arr[-10:-5].mean()
        out["comp_delta_5d"] = float(recent5 - prev5)

    if n >= 5:
        out["comp_std_5d"] = float(arr[-5:].std(ddof=0))

    if n >= 10:
        sub = arr[-10:]
        signs = np.sign(sub)
        changes = int(np.sum(signs[1:] != signs[:-1]))
        out["comp_sign_changes_10d"] = float(changes)
        out["comp_roll_max_10d"] = float(np.abs(sub).max())

    return out


def _compute_tech_raw_features(
    df_ts: pd.DataFrame,
    fundamental_row: pd.Series | None,
) -> dict[str, float]:
    """提取最近一日的若干原始技术/估值指标。"""
    out = {k: 0.0 for k in TECH_RAW_NAMES}
    if df_ts is None or df_ts.empty:
        return out

    row = df_ts.iloc[-1]
    out["tech_rsi14"] = _safe_float(row.get("rsi_14"))
    out["tech_macd_hist"] = _safe_float(row.get("macd_hist"))
    out["tech_ma60_bias"] = _safe_float(row.get("ma60_bias"))
    out["tech_volatility_20"] = _safe_float(row.get("volatility_20"))
    out["tech_momentum_20"] = _safe_float(row.get("momentum_20"))
    out["tech_vol_ratio"] = _safe_float(row.get("vol_ratio"))

    if fundamental_row is not None:
        out["tech_turnover_rate"] = _safe_float(fundamental_row.get("turnover_rate"))

    return out


# ── 主类 ──────────────────────────────────────────


@dataclass
class FeatureBuilder:
    """逐票特征矩阵构造器。

    使用方式：
      builder = FeatureBuilder(engine)
      X = builder.build_history(code, '20220101', '20260430')  # 训练用
      x = builder.build_single(code, '20260430')               # 推理用
    """

    engine: Engine
    ts_lookback_days: int = 80     # 触发器/趋势计算所需 K 线
    composite_lookback: int = 12   # composite 时序衍生

    # 批量预加载缓存：code -> preload dict（来自 _preload_for_history）。
    # 用于避免回测/日内多次 build_single 时反复查库。
    _preload_cache: dict[str, dict] = field(default_factory=dict, repr=False, compare=False)

    def warmup(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
    ) -> None:
        """一次性为多只标的预加载历史区间所需的全部数据。

        预加载完成后，后续在 [start_date, end_date] 区间内的 ``build_single`` 调用
        会直接命中内存缓存，避免逐日重复查库。

        典型使用场景：回测引擎在主循环之前调一次 warmup，性能可提升 5-10 倍。
        """
        for code in codes:
            try:
                self._preload_cache[code] = self._preload_for_history(
                    code, start_date, end_date
                )
            except Exception as e:
                logger.warning(f"FeatureBuilder.warmup 失败 code={code}: {e}")

    def get_cached_preload(self, code: str) -> dict | None:
        """供下游模块（如 advisor._load_recent_ts）复用 ts/signals 缓存。"""
        return self._preload_cache.get(code)

    def clear_cache(self) -> None:
        self._preload_cache.clear()

    def build_single(
        self,
        code: str,
        trade_date: str,
    ) -> pd.Series | None:
        """构造单票单日的特征向量。

        Returns
        -------
        pd.Series | None
            索引为 FEATURE_COLUMNS 的 Series；若数据不足返回 None。
        """
        preload = self._preload_cache.get(code)
        feats = self._build_one_day(code, trade_date, preload=preload)
        if feats is None:
            return None
        return pd.Series(feats)[list(FEATURE_COLUMNS)]

    def build_history(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """构造历史特征矩阵。

        Returns
        -------
        pd.DataFrame
            索引为 trade_date，列为 FEATURE_COLUMNS。每行为该交易日特征。
        """
        from invest_model.repositories.calendar_repo import CalendarRepository

        cal = CalendarRepository(self.engine)
        dates = cal.get_trade_dates(start_date, end_date)
        if not dates:
            return pd.DataFrame(columns=list(FEATURE_COLUMNS))

        # 一次性预加载日线/技术/composite/fundamental，避免逐日反复查库
        preload = self._preload_for_history(code, start_date, end_date)

        rows: list[dict] = []
        for dt in dates:
            feats = self._build_one_day(code, dt, preload=preload)
            if feats is None:
                continue
            feats["trade_date"] = dt
            rows.append(feats)

        if not rows:
            return pd.DataFrame(columns=list(FEATURE_COLUMNS))

        df = pd.DataFrame(rows).set_index("trade_date")
        # 严格列序
        for c in FEATURE_COLUMNS:
            if c not in df.columns:
                df[c] = 0.0
        return df[list(FEATURE_COLUMNS)]

    # ── 内部 ─────────────────────────────────────

    def _preload_for_history(
        self, code: str, start_date: str, end_date: str
    ) -> dict:
        """一次性加载历史区间的所有依赖数据。"""
        # 起点向前推 ts_lookback_days 自然日，确保最早交易日有足够回看
        ext_start = (
            datetime.strptime(start_date, "%Y%m%d") - timedelta(days=self.ts_lookback_days * 2)
        ).strftime("%Y%m%d")

        daily_repo = StockDailyRepository(self.engine)
        tech_repo = TechnicalRepository(self.engine)
        etf_repo = ETFRepository(self.engine)

        daily = daily_repo.get_daily(code, ext_start, end_date)
        if daily.empty:
            daily = etf_repo.get_daily(code, ext_start, end_date)

        tech = tech_repo.get_technical(code, ext_start, end_date)
        if not tech.empty:
            merged = daily.merge(
                tech,
                on=["code", "trade_date"],
                how="left",
                suffixes=("", "_tech"),
            )
        else:
            merged = self._compute_basic_ma(daily) if not daily.empty else daily
        merged = merged.sort_values("trade_date").reset_index(drop=True) if not merged.empty else merged

        # 信号分快照
        signal_df = self._load_signal_snapshots(code, ext_start, end_date)

        # composite 历史
        composite_df = self._load_composite_history(code, ext_start, end_date)

        # 估值（取 turnover_rate）
        fund_df = self._load_fundamental(code, ext_start, end_date)

        return {
            "ts": merged,
            "signals": signal_df,
            "composite": composite_df,
            "fundamental": fund_df,
        }

    def _build_one_day(
        self,
        code: str,
        trade_date: str,
        preload: dict | None = None,
    ) -> dict | None:
        """构造单日特征 dict。"""
        if preload is None:
            preload = self._preload_for_history(code, trade_date, trade_date)

        ts: pd.DataFrame = preload.get("ts")
        if ts is None or ts.empty:
            return None

        # 截取至 trade_date，且要求至少一条
        ts_until = ts[ts["trade_date"] <= trade_date]
        if ts_until.empty:
            return None
        # 仅保留近 ts_lookback_days
        ts_until = ts_until.tail(self.ts_lookback_days).reset_index(drop=True)

        # signals 取该交易日
        signals_df: pd.DataFrame = preload.get("signals")
        sig_row: pd.Series | None = None
        if signals_df is not None and not signals_df.empty:
            sub = signals_df[
                (signals_df["code"] == code) & (signals_df["trade_date"] == trade_date)
            ]
            if not sub.empty:
                sig_row = sub.iloc[0]

        # composite 历史（取 ≤ trade_date 最近 N 条）
        composite_df: pd.DataFrame = preload.get("composite")
        comp_hist: list[float] = []
        if composite_df is not None and not composite_df.empty:
            sub = composite_df[
                (composite_df["code"] == code) & (composite_df["trade_date"] <= trade_date)
            ].sort_values("trade_date").tail(self.composite_lookback)
            comp_hist = sub["composite"].astype(float).tolist()

        # fundamental
        fund_df: pd.DataFrame = preload.get("fundamental")
        fund_row: pd.Series | None = None
        if fund_df is not None and not fund_df.empty:
            sub = fund_df[
                (fund_df["code"] == code) & (fund_df["trade_date"] <= trade_date)
            ].sort_values("trade_date").tail(1)
            if not sub.empty:
                fund_row = sub.iloc[0]

        feats: dict[str, float] = {}

        # 1. 信号分（注意：此处历史样本若没有信号快照，全置 0；推理时由 advisor 注入实时分）
        for name in SIGNAL_NAME_ORDER:
            feats[name] = _safe_float(sig_row.get(name)) if sig_row is not None else 0.0

        # 2. trigger 连续值
        feats.update(_compute_trigger_features(ts_until))

        # 3. trend
        feats.update(_compute_trend_features(ts_until))

        # 4. composite 时序
        feats.update(_compute_composite_ts_features(comp_hist))

        # 5. 原始指标
        feats.update(_compute_tech_raw_features(ts_until, fund_row))

        return feats

    # ── 数据加载 helpers ─────────────────────────

    def _load_signal_snapshots(
        self, code: str, start: str, end: str
    ) -> pd.DataFrame:
        """从 stock_signal_snapshot 读取信号 score，转换为以信号名为列的宽表。

        stock_signal_snapshot 表存储格式：每行 (code, trade_date, name, score, ...)。
        如果该表不存在或为空，返回空 DataFrame，调用方将所有信号填 0。
        """
        sql = """
            SELECT code, trade_date, signal_name, score
            FROM stock_signal_snapshot
            WHERE code = :c AND trade_date BETWEEN :s AND :e
        """
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(
                    sa_text(sql),
                    conn,
                    params={"c": code, "s": start, "e": end},
                )
        except Exception as e:
            logger.warning(f"_load_signal_snapshots 失败 code={code}: {e}")
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        wide = df.pivot_table(
            index=["code", "trade_date"],
            columns="signal_name",
            values="score",
            aggfunc="last",
        ).reset_index()
        wide.columns.name = None
        return wide

    def _load_composite_history(
        self, code: str, start: str, end: str
    ) -> pd.DataFrame:
        sql = """
            SELECT code, trade_date, composite
            FROM stock_composite_score
            WHERE code = :c AND trade_date BETWEEN :s AND :e
            ORDER BY trade_date
        """
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(
                    sa_text(sql), conn, params={"c": code, "s": start, "e": end}
                )
        except Exception as e:
            logger.warning(f"_load_composite_history 失败 code={code}: {e}")
            return pd.DataFrame()

    def _load_fundamental(
        self, code: str, start: str, end: str
    ) -> pd.DataFrame:
        sql = """
            SELECT code, trade_date, turnover_rate, turnover_rate_f, pe_ttm, pb
            FROM stock_fundamental
            WHERE code = :c AND trade_date BETWEEN :s AND :e
            ORDER BY trade_date
        """
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(
                    sa_text(sql), conn, params={"c": code, "s": start, "e": end}
                )
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def _compute_basic_ma(df: pd.DataFrame) -> pd.DataFrame:
        """对 ETF 等无 stock_technical 的标的，基于日线现场算技术指标。

        覆盖 _compute_tech_raw_features 需要的全部纯价格/量衍生列：
          ma5/10/20/60、rsi_14、macd_hist、ma60_bias、volatility_20、
          momentum_20、vol_ratio。

        缺失 ``turnover_rate``（依赖 stock_fundamental，ETF 无此表），
        该列在特征侧会被 effective_feature_columns 自动剔除。
        """
        if df is None or df.empty:
            return df

        df = df.copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(
            df.get("volume", pd.Series(dtype=float)), errors="coerce"
        )
        if "pct_chg" in df.columns:
            df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
        df = df.sort_values("trade_date").reset_index(drop=True)

        # 复用 technical.calculator 的纯价格序列衍生函数；
        # 它原本服务于 stock_daily 全量字段，但仅依赖 close/volume/pct_chg，对 ETF 同样适用。
        try:
            from invest_model.technical.calculator import compute_technical

            enriched = compute_technical(df, code="", is_st=False)
            return enriched
        except Exception as e:  # noqa: BLE001 - 回退到最朴素的 MA，保持原行为
            logger.warning(f"_compute_basic_ma 退化到 MA-only: {e}")
            for w in (5, 10, 20, 60):
                df[f"ma{w}"] = df["close"].rolling(w, min_periods=1).mean()
            return df


def feature_columns(include_signals: bool = True) -> list[str]:
    """返回完整特征列名列表。"""
    if include_signals:
        return list(FEATURE_COLUMNS)
    return [c for c in FEATURE_COLUMNS if c not in SIGNAL_NAME_ORDER]
