"""技术指标信号解读器

从 calculator.py 产出的原始技术指标中提取可解释的交易信号，
每个信号带有方向、量化评分和人类可读标签。

本模块实现 SignalGenerator 接口，可被模型层统一消费。
"""

import numpy as np
import pandas as pd

from invest_model.technical.signals import (
    Signal,
    SignalDirection,
    SignalGenerator,
    SignalSnapshot,
    score_to_strength,
)


# ── 各指标信号生成函数 ──────────────────────────────────────────


def _macd_signal(row: pd.Series) -> Signal:
    """MACD 趋势信号

    评分逻辑：
    - DIF > DEA 且 DIF > 0 → 多头强势 (score 0.6~1.0，按 hist 幅度缩放)
    - DIF > DEA 且 DIF <= 0 → 金叉向上 (score 0.2~0.6)
    - DIF < DEA 且 DIF < 0 → 空头主导 (score -0.6~-1.0)
    - DIF < DEA 且 DIF >= 0 → 死叉向下 (score -0.2~-0.6)
    """
    dif = float(row.get("macd_dif", 0) or 0)
    dea = float(row.get("macd_dea", 0) or 0)
    hist = float(row.get("macd_hist", 0) or 0)

    hist_scale = min(abs(hist) / 0.5, 1.0) if hist != 0 else 0

    if dif > dea and dif > 0:
        score = 0.6 + 0.4 * hist_scale
        label = "MACD 多头强势，趋势向上"
        direction = SignalDirection.BULLISH
    elif dif > dea:
        score = 0.2 + 0.4 * hist_scale
        label = "MACD 金叉，趋势转多"
        direction = SignalDirection.BULLISH
    elif dif < dea and dif < 0:
        score = -(0.6 + 0.4 * hist_scale)
        label = "MACD 空头主导，趋势偏弱"
        direction = SignalDirection.BEARISH
    else:
        score = -(0.2 + 0.4 * hist_scale)
        label = "MACD 死叉，需关注下行风险"
        direction = SignalDirection.BEARISH

    return Signal(
        name="macd_trend",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"macd_dif": dif, "macd_dea": dea, "macd_hist": hist},
    )


def _rsi_signal(row: pd.Series) -> Signal:
    """RSI 超买超卖信号

    评分逻辑：
    - RSI > 80 → 强超买 (score -0.8)
    - RSI > 70 → 超买 (score -0.4~-0.8，线性插值)
    - RSI < 20 → 强超卖 (score 0.8)
    - RSI < 30 → 超卖 (score 0.4~0.8，线性插值)
    - 30~70 之间 → 中性 (score 接近 0)
    """
    rsi = float(row.get("rsi_14", 50) or 50)

    if rsi > 80:
        score = -0.8
        label = f"RSI 强超买区域 ({rsi:.1f})，回调压力大"
        direction = SignalDirection.BEARISH
    elif rsi > 70:
        t = (rsi - 70) / 10.0
        score = -(0.4 + 0.4 * t)
        label = f"RSI 超买区域 ({rsi:.1f})，短期或有回调压力"
        direction = SignalDirection.BEARISH
    elif rsi < 20:
        score = 0.8
        label = f"RSI 强超卖区域 ({rsi:.1f})，反弹概率较高"
        direction = SignalDirection.BULLISH
    elif rsi < 30:
        t = (30 - rsi) / 10.0
        score = 0.4 + 0.4 * t
        label = f"RSI 超卖区域 ({rsi:.1f})，可能接近反弹"
        direction = SignalDirection.BULLISH
    else:
        score = (50 - rsi) / 50.0 * 0.2
        label = f"RSI 中性 ({rsi:.1f})"
        direction = SignalDirection.NEUTRAL

    return Signal(
        name="rsi_extreme",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"rsi_14": rsi},
    )


def _boll_signal(row: pd.Series) -> Signal:
    """布林带位置信号

    评分逻辑：以 close 在布林带内的百分位映射评分。
    - 百分位 > 0.8 → 接近上轨，看空 (score 负)
    - 百分位 < 0.2 → 接近下轨，看多 (score 正)
    - 中轨附近 → 中性
    """
    close = float(row.get("close", 0) or 0)
    upper = float(row.get("boll_upper", 0) or 0)
    lower = float(row.get("boll_lower", 0) or 0)

    if upper == lower or upper == 0:
        return Signal(
            name="boll_position",
            direction=SignalDirection.NEUTRAL,
            score=0.0,
            strength=score_to_strength(0),
            label="布林带数据不足",
            indicator_values={"close": close, "boll_upper": upper, "boll_lower": lower},
        )

    pct = (close - lower) / (upper - lower)
    pct = max(0.0, min(1.0, pct))

    if pct > 0.8:
        score = -(pct - 0.5) * 1.5
        label = f"价格接近布林上轨 ({pct:.0%})，波动加大"
        direction = SignalDirection.BEARISH
    elif pct < 0.2:
        score = (0.5 - pct) * 1.5
        label = f"价格接近布林下轨 ({pct:.0%})，存在支撑"
        direction = SignalDirection.BULLISH
    else:
        score = (0.5 - pct) * 0.4
        label = f"价格处于布林中轨区域 ({pct:.0%})"
        direction = SignalDirection.NEUTRAL

    score = max(-1.0, min(1.0, score))

    return Signal(
        name="boll_position",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"close": close, "boll_upper": upper, "boll_lower": lower, "boll_pct": pct},
    )


def _ma_bias_signal(row: pd.Series) -> Signal:
    """MA60 偏离度信号

    评分逻辑：偏离度绝对值越大，均值回归概率越高。
    - 偏离 > +15% → 看空 (过度偏离)
    - 偏离 < -15% → 看多 (过度偏离)
    - |偏离| < 5% → 中性
    """
    bias = float(row.get("ma60_bias", 0) or 0)

    if abs(bias) > 0.15:
        if bias > 0:
            score = -min(bias / 0.3, 1.0)
            label = f"MA60 大幅偏离上方 ({bias:.2%})，均值回归概率增大"
            direction = SignalDirection.BEARISH
        else:
            score = min(abs(bias) / 0.3, 1.0)
            label = f"MA60 大幅偏离下方 ({bias:.2%})，均值回归概率增大"
            direction = SignalDirection.BULLISH
    elif abs(bias) > 0.10:
        if bias > 0:
            score = -bias / 0.3
            label = f"MA60 偏离上方 ({bias:.2%})，关注回归风险"
            direction = SignalDirection.BEARISH
        else:
            score = abs(bias) / 0.3
            label = f"MA60 偏离下方 ({bias:.2%})，关注回归机会"
            direction = SignalDirection.BULLISH
    else:
        score = -bias / 0.3
        label = f"MA60 偏离度正常 ({bias:.2%})"
        direction = SignalDirection.NEUTRAL

    score = max(-1.0, min(1.0, score))

    return Signal(
        name="ma_bias",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"ma60_bias": bias},
    )


def _vol_ratio_signal(row: pd.Series) -> Signal:
    """量比信号（结合涨跌方向）

    放量上涨 → 正分（量价配合）
    放量下跌 → 负分（出货信号）
    缩量 → 弱负分（交投清淡）
    """
    vol = float(row.get("vol_ratio", 1.0) or 1.0)
    close = float(row.get("close", 0) or 0)
    pre_close = float(row.get("pre_close", 0) or 0)
    price_chg = (close / pre_close - 1.0) if pre_close > 0 else 0.0

    if vol > 2.0:
        if price_chg > 0.01:
            score = min(0.4, 0.2 + (vol - 2.0) * 0.1)
            label = f"量比 {vol:.2f} 放量上涨 {price_chg:+.1%}，量价配合"
            direction = SignalDirection.BULLISH
        elif price_chg < -0.01:
            score = max(-0.5, -0.2 - (vol - 2.0) * 0.15)
            label = f"量比 {vol:.2f} 放量下跌 {price_chg:+.1%}，疑似出货"
            direction = SignalDirection.BEARISH
        else:
            score = 0.0
            label = f"量比 {vol:.2f} 放量平盘，方向不明"
            direction = SignalDirection.NEUTRAL
    elif vol < 0.3:
        score = -0.15
        label = f"量比 {vol:.2f}，成交极度萎缩"
        direction = SignalDirection.BEARISH
    elif vol < 0.5:
        score = -0.05
        label = f"量比 {vol:.2f}，成交萎缩"
        direction = SignalDirection.BEARISH
    else:
        score = 0.0
        label = f"量比 {vol:.2f}，成交正常"
        direction = SignalDirection.NEUTRAL

    return Signal(
        name="vol_ratio",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"vol_ratio": vol, "price_chg": round(price_chg, 4)},
    )


def _momentum_signal(row: pd.Series) -> Signal:
    """20 日动量信号"""
    mom = float(row.get("momentum_20", 0) or 0)

    if mom > 0.15:
        score = min(mom / 0.3, 1.0)
        label = f"20日动量强劲 ({mom:.2%})，上升趋势明显"
        direction = SignalDirection.BULLISH
    elif mom > 0.05:
        score = mom / 0.3
        label = f"20日动量偏多 ({mom:.2%})"
        direction = SignalDirection.BULLISH
    elif mom < -0.15:
        score = max(mom / 0.3, -1.0)
        label = f"20日动量疲弱 ({mom:.2%})，下降趋势明显"
        direction = SignalDirection.BEARISH
    elif mom < -0.05:
        score = mom / 0.3
        label = f"20日动量偏空 ({mom:.2%})"
        direction = SignalDirection.BEARISH
    else:
        score = mom / 0.3
        label = f"20日动量中性 ({mom:.2%})"
        direction = SignalDirection.NEUTRAL

    score = max(-1.0, min(1.0, score))

    return Signal(
        name="momentum_20",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"momentum_20": mom},
    )


def _volatility_signal(row: pd.Series) -> Signal:
    """20 日波动率信号（高波动≠看空，但代表风险加大）"""
    vol = float(row.get("volatility_20", 0) or 0)

    if vol > 0.50:
        score = -0.3
        label = f"20日年化波动率极高 ({vol:.2%})，风险显著"
        direction = SignalDirection.BEARISH
    elif vol > 0.35:
        score = -0.15
        label = f"20日年化波动率偏高 ({vol:.2%})"
        direction = SignalDirection.NEUTRAL
    elif vol < 0.10:
        score = 0.1
        label = f"20日年化波动率极低 ({vol:.2%})，可能酝酿变盘"
        direction = SignalDirection.NEUTRAL
    else:
        score = 0.0
        label = f"20日年化波动率正常 ({vol:.2%})"
        direction = SignalDirection.NEUTRAL

    return Signal(
        name="volatility_20",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"volatility_20": vol},
    )


# ── 综合信号生成器 ──────────────────────────────────────────────


# 各信号在综合评分中的权重（可通过配置调整）
DEFAULT_SIGNAL_WEIGHTS = {
    "macd_trend": 0.25,
    "rsi_extreme": 0.15,
    "boll_position": 0.15,
    "ma_bias": 0.15,
    "momentum_20": 0.15,
    "vol_ratio": 0.10,
    "volatility_20": 0.05,
}

_SIGNAL_GENERATORS = [
    _macd_signal,
    _rsi_signal,
    _boll_signal,
    _ma_bias_signal,
    _vol_ratio_signal,
    _momentum_signal,
    _volatility_signal,
]


class TechnicalSignalGenerator(SignalGenerator):
    """技术面信号生成器

    从 stock_technical 表的指标数据中提取 7 类技术信号，
    并计算加权综合评分。

    Attributes
    ----------
    category : str
        信号类别，固定为 "technical"
    scope : str
        数据作用域，"time_series"（每票拿自己的历史即可）
    required_tables : tuple
        依赖的数据库表
    """

    category = "technical"
    scope = "time_series"
    required_tables = ("stock_technical", "stock_daily")

    def __init__(self, weights: dict[str, float] | None = None):
        self.weights = weights or DEFAULT_SIGNAL_WEIGHTS

    def required_columns(self) -> list[str]:
        return [
            "macd_dif", "macd_dea", "macd_hist",
            "rsi_14",
            "close", "boll_upper", "boll_lower",
            "ma60_bias",
            "vol_ratio",
            "momentum_20",
            "volatility_20",
        ]

    def generate(self, code: str, data: pd.DataFrame) -> list[Signal]:
        self.validate_input(data)
        row = data.iloc[-1]
        return [gen(row) for gen in _SIGNAL_GENERATORS]

    def generate_partial(self, code: str, data: pd.DataFrame) -> list[Signal]:
        """对缺少部分指标列的数据（如 ETF），尽量生成可用信号。"""
        row = data.iloc[-1]
        signals = []
        for gen in _SIGNAL_GENERATORS:
            try:
                sig = gen(row)
                if sig.score != 0:
                    signals.append(sig)
            except (TypeError, ValueError, KeyError):
                continue
        return signals

    def generate_snapshot(self, code: str, data: pd.DataFrame) -> SignalSnapshot:
        """生成完整的信号快照（含综合评分）"""
        signals = self.generate(code, data)

        weighted_sum = 0.0
        weight_total = 0.0
        for sig in signals:
            w = self.weights.get(sig.name, 0.1)
            weighted_sum += sig.score * w
            weight_total += w

        composite = weighted_sum / weight_total if weight_total > 0 else 0.0

        trade_date = str(data.iloc[-1].get("trade_date", ""))
        return SignalSnapshot(
            code=code,
            trade_date=trade_date,
            signals=signals,
            composite_score=max(-1.0, min(1.0, composite)),
        )

    def generate_for_date(self, code: str, trade_date: str, context: dict) -> list[Signal]:
        """批量评分入口：从 context["time_series"] 中取对应 DataFrame。"""
        ts_map = context.get("time_series", {}) or {}
        df = ts_map.get(code)
        if df is None or df.empty:
            return []
        try:
            return self.generate(code, df)
        except ValueError:
            return self.generate_partial(code, df)
