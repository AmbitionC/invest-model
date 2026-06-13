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


# ── DataFrame 级别信号（需要时间序列） ──────────────────────────


def _macd_divergence_signal(data: pd.DataFrame) -> list[Signal]:
    """MACD 底背离/顶背离检测。

    底背离：价格创近期新低，但 DIF 未创新低 → 看涨反转信号
    顶背离：价格创近期新高，但 DIF 未创新高 → 看跌反转信号

    使用 20 日窗口寻找前一个波峰/波谷进行对比。
    """
    signals = []
    if "macd_dif" not in data.columns or "close" not in data.columns:
        return signals

    closes = pd.to_numeric(data["close"], errors="coerce")
    difs = pd.to_numeric(data["macd_dif"], errors="coerce")

    if len(closes) < 20 or closes.isna().sum() > len(closes) * 0.3:
        return signals

    # 取最近 30 天的数据
    window = min(30, len(closes))
    c = closes.iloc[-window:].values
    d = difs.iloc[-window:].values

    if len(c) < 15:
        return signals

    # 寻找近期低点（前半段 vs 后半段）
    mid = len(c) // 2
    first_half_low_idx = np.argmin(c[:mid])
    second_half_low_idx = mid + np.argmin(c[mid:])

    # 底背离检测：后半段价格低点 <= 前半段低点，但 DIF 后半段低点 > 前半段低点
    if c[second_half_low_idx] <= c[first_half_low_idx] * 1.01:
        if d[second_half_low_idx] > d[first_half_low_idx] + 0.01:
            # 确认当前价格在后半段低点附近（距低点不超过 3%）
            current_price = c[-1]
            recent_low = c[second_half_low_idx]
            if current_price <= recent_low * 1.06:
                strength = min(0.8, (d[second_half_low_idx] - d[first_half_low_idx]) * 5)
                signals.append(Signal(
                    name="macd_bottom_divergence",
                    direction=SignalDirection.BULLISH,
                    score=max(0.3, strength),
                    strength=score_to_strength(strength),
                    label=f"MACD底背离: 价格创新低但DIF抬高，潜在反转",
                    indicator_values={
                        "price_low1": round(float(c[first_half_low_idx]), 2),
                        "price_low2": round(float(c[second_half_low_idx]), 2),
                        "dif_low1": round(float(d[first_half_low_idx]), 4),
                        "dif_low2": round(float(d[second_half_low_idx]), 4),
                    },
                ))

    # 顶背离检测：后半段价格高点 >= 前半段高点，但 DIF 后半段高点 < 前半段高点
    first_half_high_idx = np.argmax(c[:mid])
    second_half_high_idx = mid + np.argmax(c[mid:])

    if c[second_half_high_idx] >= c[first_half_high_idx] * 0.99:
        if d[second_half_high_idx] < d[first_half_high_idx] - 0.01:
            current_price = c[-1]
            recent_high = c[second_half_high_idx]
            if current_price >= recent_high * 0.94:
                strength = min(0.8, (d[first_half_high_idx] - d[second_half_high_idx]) * 5)
                signals.append(Signal(
                    name="macd_top_divergence",
                    direction=SignalDirection.BEARISH,
                    score=max(-0.8, -max(0.3, strength)),
                    strength=score_to_strength(-strength),
                    label=f"MACD顶背离: 价格创新高但DIF走低，上涨动能衰竭",
                    indicator_values={
                        "price_high1": round(float(c[first_half_high_idx]), 2),
                        "price_high2": round(float(c[second_half_high_idx]), 2),
                        "dif_high1": round(float(d[first_half_high_idx]), 4),
                        "dif_high2": round(float(d[second_half_high_idx]), 4),
                    },
                ))

    return signals


def _volume_pattern_signal(data: pd.DataFrame) -> list[Signal]:
    """量价形态信号检测：

    1. 放量突破 — 突破20日高点且量比>1.5，确认突破有效性
    2. 缩量回踩 — 回调到均线附近但量能萎缩，健康回踩信号
    3. 地量见底 — 成交量处于60日最低水平，可能筑底完成
    4. 天量见顶 — 成交量处于60日最高水平且放量滞涨
    """
    signals = []
    if "close" not in data.columns or "volume" not in data.columns:
        return signals

    closes = pd.to_numeric(data["close"], errors="coerce")
    volumes = pd.to_numeric(data["volume"], errors="coerce")

    if len(closes) < 20 or len(volumes) < 20:
        return signals

    current_close = float(closes.iloc[-1])
    current_vol = float(volumes.iloc[-1])

    # 量能基准
    vol_ma5 = float(volumes.tail(5).mean())
    vol_ma20 = float(volumes.tail(20).mean())
    vol_ratio = current_vol / vol_ma20 if vol_ma20 > 0 else 1.0

    # 价格参考
    high_20d = float(closes.tail(20).max())
    low_20d = float(closes.tail(20).min())
    prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else current_close
    price_chg = (current_close - prev_close) / prev_close if prev_close > 0 else 0

    # MA20 参考
    ma20 = float(closes.tail(20).mean()) if len(closes) >= 20 else current_close

    # 1. 放量突破：突破20日高点 + 量比>1.3
    if current_close >= high_20d * 0.99 and vol_ratio > 1.3 and price_chg > 0.01:
        score = min(0.7, 0.3 + (vol_ratio - 1.3) * 0.15)
        signals.append(Signal(
            name="volume_breakout",
            direction=SignalDirection.BULLISH,
            score=score,
            strength=score_to_strength(score),
            label=f"放量突破: 创20日新高+量比{vol_ratio:.1f}，突破有效",
            indicator_values={"vol_ratio": round(vol_ratio, 2), "high_20d": round(high_20d, 2)},
        ))

    # 2. 缩量回踩：价格回调到MA20附近 + 量能萎缩
    ma_proximity = abs(current_close - ma20) / ma20 if ma20 > 0 else 1
    vol_shrink_ratio = current_vol / vol_ma20 if vol_ma20 > 0 else 1
    if (ma_proximity < 0.02 and vol_shrink_ratio < 0.6
            and current_close > low_20d * 1.03):
        score = min(0.5, 0.2 + (0.6 - vol_shrink_ratio) * 0.5)
        signals.append(Signal(
            name="volume_pullback",
            direction=SignalDirection.BULLISH,
            score=score,
            strength=score_to_strength(score),
            label=f"缩量回踩: 价格回到MA20附近+量缩至{vol_shrink_ratio:.0%}，健康回踩",
            indicator_values={"ma_proximity": round(ma_proximity, 4), "vol_shrink": round(vol_shrink_ratio, 3)},
        ))

    # 3. 地量见底：成交量是60日最低水平 + 价格已跌较多
    if len(volumes) >= 60:
        vol_60_min = float(volumes.tail(60).min())
        vol_60_pct = (current_vol - vol_60_min) / vol_60_min if vol_60_min > 0 else 1
        price_60_low = float(closes.tail(60).min())
        near_low = (current_close - price_60_low) / price_60_low if price_60_low > 0 else 1

        if vol_60_pct < 0.1 and near_low < 0.05:
            signals.append(Signal(
                name="volume_dry_up",
                direction=SignalDirection.BULLISH,
                score=0.4,
                strength=score_to_strength(0.4),
                label=f"地量信号: 成交量接近60日最低，抛压衰竭，可能见底",
                indicator_values={"vol_pct_from_min": round(vol_60_pct, 3), "price_near_low": round(near_low, 3)},
            ))

        # 4. 天量见顶：量能极大 + 价格滞涨或收阴
        vol_60_max = float(volumes.tail(60).max())
        vol_max_pct = (current_vol - vol_60_max) / vol_60_max if vol_60_max > 0 else 0
        near_high = (high_20d - current_close) / high_20d if high_20d > 0 else 1

        if vol_max_pct > -0.1 and vol_ratio > 2.5 and price_chg < 0.01 and near_high < 0.02:
            signals.append(Signal(
                name="volume_climax",
                direction=SignalDirection.BEARISH,
                score=-0.5,
                strength=score_to_strength(-0.5),
                label=f"天量滞涨: 量比{vol_ratio:.1f}接近60日最高但价格停滞，可能见顶",
                indicator_values={"vol_ratio": round(vol_ratio, 2), "price_chg": round(price_chg, 4)},
            ))

    return signals


def _rsi_divergence_signal(data: pd.DataFrame) -> list[Signal]:
    """RSI 背离检测。

    底背离：价格创近期新低但 RSI 未创新低 → 超卖反弹信号
    顶背离：价格创近期新高但 RSI 未创新高 → 超买回调信号
    """
    signals = []
    if "rsi_14" not in data.columns or "close" not in data.columns:
        return signals

    closes = pd.to_numeric(data["close"], errors="coerce")
    rsis = pd.to_numeric(data["rsi_14"], errors="coerce")

    if len(closes) < 20 or rsis.isna().sum() > len(rsis) * 0.5:
        return signals

    window = min(30, len(closes))
    c = closes.iloc[-window:].values
    r = rsis.iloc[-window:].values

    if len(c) < 15:
        return signals

    mid = len(c) // 2

    # RSI 底背离
    first_low_idx = np.argmin(c[:mid])
    second_low_idx = mid + np.argmin(c[mid:])

    if (c[second_low_idx] <= c[first_low_idx] * 1.01
            and not np.isnan(r[first_low_idx]) and not np.isnan(r[second_low_idx])):
        if r[second_low_idx] > r[first_low_idx] + 3:
            current_price = c[-1]
            if current_price <= c[second_low_idx] * 1.06:
                rsi_diff = r[second_low_idx] - r[first_low_idx]
                score = min(0.6, rsi_diff / 20)
                signals.append(Signal(
                    name="rsi_bottom_divergence",
                    direction=SignalDirection.BULLISH,
                    score=max(0.25, score),
                    strength=score_to_strength(score),
                    label=f"RSI底背离: 价格创新低但RSI抬高({r[first_low_idx]:.0f}→{r[second_low_idx]:.0f})，反弹信号",
                    indicator_values={
                        "rsi_low1": round(float(r[first_low_idx]), 1),
                        "rsi_low2": round(float(r[second_low_idx]), 1),
                    },
                ))

    # RSI 顶背离
    first_high_idx = np.argmax(c[:mid])
    second_high_idx = mid + np.argmax(c[mid:])

    if (c[second_high_idx] >= c[first_high_idx] * 0.99
            and not np.isnan(r[first_high_idx]) and not np.isnan(r[second_high_idx])):
        if r[second_high_idx] < r[first_high_idx] - 3:
            current_price = c[-1]
            if current_price >= c[second_high_idx] * 0.94:
                rsi_diff = r[first_high_idx] - r[second_high_idx]
                score = min(0.6, rsi_diff / 20)
                signals.append(Signal(
                    name="rsi_top_divergence",
                    direction=SignalDirection.BEARISH,
                    score=max(-0.6, -max(0.25, score)),
                    strength=score_to_strength(-score),
                    label=f"RSI顶背离: 价格创新高但RSI走低({r[first_high_idx]:.0f}→{r[second_high_idx]:.0f})，回调风险",
                    indicator_values={
                        "rsi_high1": round(float(r[first_high_idx]), 1),
                        "rsi_high2": round(float(r[second_high_idx]), 1),
                    },
                ))

    return signals


def _macd_histogram_signal(data: pd.DataFrame) -> list[Signal]:
    """MACD 柱状图方向 + 动量衰减检测。

    - 柱状图方向：hist 连续 N 日递增/递减，表示趋势加速/减速
    - 动量衰减：hist 绝对值在缩小（虽然方向未变），预警趋势末端
    """
    signals = []
    hist_col = "macd_hist"
    if hist_col not in data.columns:
        return signals

    hist_series = pd.to_numeric(data[hist_col], errors="coerce").dropna()
    if len(hist_series) < 5:
        return signals

    recent_5 = hist_series.tail(5).values
    current_hist = float(recent_5[-1])

    # 计算 hist 变化方向（连续递增/递减天数）
    diffs = np.diff(recent_5)
    increasing = sum(1 for d in diffs if d > 0)
    decreasing = sum(1 for d in diffs if d < 0)

    # 柱状图方向信号
    if increasing >= 3 and current_hist > 0:
        score = min(0.6, increasing * 0.15)
        label = f"MACD柱状图连续放大({increasing}日)，多头加速"
        direction = SignalDirection.BULLISH
    elif increasing >= 3 and current_hist < 0:
        score = min(0.4, increasing * 0.1)
        label = f"MACD柱状图负值收窄({increasing}日)，空头减弱"
        direction = SignalDirection.BULLISH
    elif decreasing >= 3 and current_hist < 0:
        score = max(-0.6, -(decreasing * 0.15))
        label = f"MACD柱状图连续缩小({decreasing}日)，空头加速"
        direction = SignalDirection.BEARISH
    elif decreasing >= 3 and current_hist > 0:
        score = max(-0.4, -(decreasing * 0.1))
        label = f"MACD柱状图正值缩窄({decreasing}日)，多头减弱"
        direction = SignalDirection.BEARISH
    else:
        score = 0.0
        label = "MACD柱状图无明显方向趋势"
        direction = SignalDirection.NEUTRAL

    signals.append(Signal(
        name="macd_hist_direction",
        direction=direction,
        score=score,
        strength=score_to_strength(score),
        label=label,
        indicator_values={"hist_current": round(current_hist, 4), "hist_increasing": increasing, "hist_decreasing": decreasing},
    ))

    # 动量衰减检测：|hist| 连续缩小但方向未变 → 趋势末端预警
    abs_hist = np.abs(recent_5)
    abs_diffs = np.diff(abs_hist)
    shrinking = sum(1 for d in abs_diffs if d < -0.001)

    if shrinking >= 3:
        if current_hist > 0:
            decay_score = max(-0.5, -(shrinking * 0.12))
            decay_label = f"多头动量衰减({shrinking}日|hist|缩小)，顶部预警"
            decay_dir = SignalDirection.BEARISH
        elif current_hist < 0:
            decay_score = min(0.5, shrinking * 0.12)
            decay_label = f"空头动量衰减({shrinking}日|hist|缩小)，底部预警"
            decay_dir = SignalDirection.BULLISH
        else:
            decay_score = 0.0
            decay_label = "动量中性"
            decay_dir = SignalDirection.NEUTRAL

        if decay_score != 0:
            signals.append(Signal(
                name="macd_momentum_decay",
                direction=decay_dir,
                score=decay_score,
                strength=score_to_strength(decay_score),
                label=decay_label,
                indicator_values={"shrinking_days": shrinking, "abs_hist_latest": round(abs_hist[-1], 4)},
            ))

    return signals


def _price_momentum_signals(data: pd.DataFrame) -> list[Signal]:
    """5日/20日价格动量信号 — 直接衡量近期价格变化方向和力度。

    与 momentum_20（基于预计算字段）不同，这里直接从 close 序列计算实际收益率，
    提供更直接的趋势跟踪信号。
    """
    signals = []
    closes = pd.to_numeric(data["close"], errors="coerce").dropna()
    if len(closes) < 2:
        return signals

    current = float(closes.iloc[-1])

    # 5日动量
    if len(closes) >= 6:
        prev_5 = float(closes.iloc[-6])
        ret_5d = (current - prev_5) / prev_5 if prev_5 > 0 else 0.0

        if ret_5d > 0.08:
            score_5 = min(ret_5d / 0.12, 1.0)
            label_5 = f"5日涨幅强劲({ret_5d:.2%})，短线动量充足"
            dir_5 = SignalDirection.BULLISH
        elif ret_5d > 0.02:
            score_5 = ret_5d / 0.12
            label_5 = f"5日小幅上涨({ret_5d:.2%})，短线偏多"
            dir_5 = SignalDirection.BULLISH
        elif ret_5d < -0.08:
            score_5 = max(ret_5d / 0.12, -1.0)
            label_5 = f"5日跌幅较大({ret_5d:.2%})，短线动量恶化"
            dir_5 = SignalDirection.BEARISH
        elif ret_5d < -0.02:
            score_5 = ret_5d / 0.12
            label_5 = f"5日小幅下跌({ret_5d:.2%})，短线偏弱"
            dir_5 = SignalDirection.BEARISH
        else:
            score_5 = ret_5d / 0.12
            label_5 = f"5日涨跌幅微小({ret_5d:.2%})"
            dir_5 = SignalDirection.NEUTRAL

        score_5 = max(-1.0, min(1.0, score_5))
        signals.append(Signal(
            name="price_momentum_5d",
            direction=dir_5,
            score=score_5,
            strength=score_to_strength(score_5),
            label=label_5,
            indicator_values={"return_5d": round(ret_5d, 5)},
        ))

    # 20日动量
    if len(closes) >= 21:
        prev_20 = float(closes.iloc[-21])
        ret_20d = (current - prev_20) / prev_20 if prev_20 > 0 else 0.0

        if ret_20d > 0.15:
            score_20 = min(ret_20d / 0.25, 1.0)
            label_20 = f"20日涨幅显著({ret_20d:.2%})，中期趋势强"
            dir_20 = SignalDirection.BULLISH
        elif ret_20d > 0.04:
            score_20 = ret_20d / 0.25
            label_20 = f"20日温和上涨({ret_20d:.2%})，中期偏多"
            dir_20 = SignalDirection.BULLISH
        elif ret_20d < -0.15:
            score_20 = max(ret_20d / 0.25, -1.0)
            label_20 = f"20日跌幅显著({ret_20d:.2%})，中期趋势弱"
            dir_20 = SignalDirection.BEARISH
        elif ret_20d < -0.04:
            score_20 = ret_20d / 0.25
            label_20 = f"20日温和下跌({ret_20d:.2%})，中期偏弱"
            dir_20 = SignalDirection.BEARISH
        else:
            score_20 = ret_20d / 0.25
            label_20 = f"20日涨跌幅平淡({ret_20d:.2%})"
            dir_20 = SignalDirection.NEUTRAL

        score_20 = max(-1.0, min(1.0, score_20))
        signals.append(Signal(
            name="price_momentum_20d",
            direction=dir_20,
            score=score_20,
            strength=score_to_strength(score_20),
            label=label_20,
            indicator_values={"return_20d": round(ret_20d, 5)},
        ))

    return signals


# ── 综合信号生成器 ──────────────────────────────────────────────


# 各信号在综合评分中的权重（可通过配置调整）
DEFAULT_SIGNAL_WEIGHTS = {
    # 趋势类
    "macd_trend": 0.12,
    "macd_hist_direction": 0.08,
    "macd_momentum_decay": 0.06,
    "price_momentum_5d": 0.12,
    "price_momentum_20d": 0.10,
    "momentum_20": 0.06,
    # 量价关系类
    "macd_bottom_divergence": 0.08,
    "macd_top_divergence": 0.08,
    "rsi_bottom_divergence": 0.05,
    "rsi_top_divergence": 0.05,
    "volume_breakout": 0.06,
    "volume_pullback": 0.04,
    "volume_dry_up": 0.03,
    "volume_climax": 0.04,
    "vol_ratio": 0.03,
    # 反转/辅助类
    "rsi_extreme": 0.04,
    "boll_position": 0.04,
    "ma_bias": 0.04,
    "volatility_20": 0.02,
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

# DataFrame 级别的信号生成器（需要整个时间序列）
_DF_SIGNAL_GENERATORS = [
    _macd_histogram_signal,
    _macd_divergence_signal,
    _volume_pattern_signal,
    _rsi_divergence_signal,
    _price_momentum_signals,
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
        signals = [gen(row) for gen in _SIGNAL_GENERATORS]
        for df_gen in _DF_SIGNAL_GENERATORS:
            signals.extend(df_gen(data))
        return signals

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
        for df_gen in _DF_SIGNAL_GENERATORS:
            try:
                signals.extend(df_gen(data))
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
