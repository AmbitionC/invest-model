"""技术指标计算 — 纯 pandas 实现，无外部 TA 依赖"""

import math

import numpy as np
import pandas as pd


# ── 均线系列 (Moving Averages) ───────────────────────────────────

_MA_WINDOWS = [5, 10, 20, 60, 120, 250]


def _moving_averages(close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({
        f"ma{w}": close.rolling(w).mean() for w in _MA_WINDOWS
    })


# ── 布林带 (Bollinger Bands) ─────────────────────────────────────

def _bollinger(close: pd.Series, window: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    mid = close.rolling(window).mean()
    std = close.rolling(window).std(ddof=1)
    return pd.DataFrame({
        "boll_mid": mid,
        "boll_upper": mid + num_std * std,
        "boll_lower": mid - num_std * std,
    })


# ── MACD ─────────────────────────────────────────────────────────

def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = 2 * (dif - dea)
    return pd.DataFrame({
        "macd_dif": dif,
        "macd_dea": dea,
        "macd_hist": hist,
    })


# ── RSI (Relative Strength Index) ───────────────────────────────

def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


# ── MA60 偏离度 (Bias) ──────────────────────────────────────────

def _ma_bias(close: pd.Series, window: int = 60) -> pd.Series:
    ma = close.rolling(window).mean()
    return (close - ma) / ma.replace(0, np.nan)


# ── 成交量比率 (Volume Ratio) ────────────────────────────────────

def _vol_ratio(volume: pd.Series, window: int = 5) -> pd.Series:
    ma_vol = volume.rolling(window).mean()
    return volume / ma_vol.replace(0, np.nan)


# ── 动量 (Momentum) ─────────────────────────────────────────────

def _momentum(close: pd.Series, period: int = 20) -> pd.Series:
    return close.pct_change(period)


# ── 波动率 (Volatility) ─────────────────────────────────────────

def _volatility(close: pd.Series, window: int = 20) -> pd.Series:
    log_ret = np.log(close / close.shift(1))
    return log_ret.rolling(window).std(ddof=1) * math.sqrt(252)


# ── 涨跌停标记 (Limit Up/Down Flags) ────────────────────────────

def _board_limit_pct(code: str) -> float:
    """根据股票代码判定所在板块的涨跌停幅度上限（%）。

    优先按交易所后缀识别，再按代码前缀兜底：
      .BJ  / 43xxxx / 83xxxx / 87xxxx  → 北交所 30%
      688xxx(.SH)                       → 科创板 20%
      30xxxx(.SZ)                       → 创业板 20%
      其余 (.SH / .SZ)                 → 沪深主板 10%
    """
    if code.endswith(".BJ"):
        return 30.0

    prefix = code.split(".")[0] if "." in code else code
    if prefix.startswith("688") or prefix.startswith("30"):
        return 20.0
    if prefix.startswith("43") or prefix.startswith("83") or prefix.startswith("87"):
        return 30.0
    return 10.0


def _limit_flags(pct_chg: pd.Series, is_st: pd.Series, code: str = "") -> pd.DataFrame:
    """根据涨跌幅、ST 状态和板块判定涨跌停。

    沪深主板：普通 10%（容差 9.8%），ST 5%（容差 4.8%）
    创业板/科创板：统一 20%（容差 19.8%），ST 同样 20%
    北交所：统一 30%（容差 29.8%）
    """
    board_pct = _board_limit_pct(code)
    if board_pct >= 20.0:
        threshold = pd.Series(board_pct - 0.2, index=pct_chg.index)
    else:
        threshold = is_st.map(lambda x: 4.8 if x else 9.8)

    return pd.DataFrame({
        "is_limit_up": (pct_chg >= threshold).astype(int),
        "is_limit_down": (pct_chg <= -threshold).astype(int),
        "is_st": is_st.astype(int),
    })


# ── 公开入口 ─────────────────────────────────────────────────────

WARMUP_DAYS = 260

def compute_technical(df: pd.DataFrame, code: str = "",
                      is_st: "bool | pd.Series" = False) -> pd.DataFrame:
    """
    计算全部技术指标。

    Parameters
    ----------
    df : DataFrame
        必须包含 close, volume, pct_chg 列，按 trade_date 升序排列。
    code : str
        股票代码（如 "300750.SZ"），用于判定板块涨跌停阈值。
    is_st : bool | pd.Series
        ST 状态。可传入标量（当前状态广播到所有行）或逐行 Series
        （支持历史 ST 状态变化的时序回测场景）。

    Returns
    -------
    DataFrame
        原始列 + 全部技术指标列。前 WARMUP_DAYS 行的指标值可能为 NaN。
    """
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)
    pct_chg = df["pct_chg"].astype(float) if "pct_chg" in df.columns else pd.Series(np.nan, index=df.index)

    boll = _bollinger(close)
    macd = _macd(close)
    ma = _moving_averages(close)

    if isinstance(is_st, pd.Series):
        st_series = is_st.reindex(df.index).fillna(False).astype(bool)
    else:
        st_series = pd.Series(bool(is_st), index=df.index)

    limits = _limit_flags(pct_chg, st_series, code=code)

    result = df.copy()
    result[["boll_upper", "boll_mid", "boll_lower"]] = boll
    result[["macd_dif", "macd_dea", "macd_hist"]] = macd
    result["rsi_6"] = _rsi(close, 6)
    result["rsi_14"] = _rsi(close, 14)
    result["ma60_bias"] = _ma_bias(close, 60)
    result["vol_ratio"] = _vol_ratio(volume, 5)
    result["momentum_20"] = _momentum(close, 20)
    result["volatility_20"] = _volatility(close, 20)
    for col in ma.columns:
        result[col] = ma[col]
    result[["is_limit_up", "is_limit_down", "is_st"]] = limits

    return result
