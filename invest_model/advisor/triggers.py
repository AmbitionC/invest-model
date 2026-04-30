"""关键位置触发检测器。

检测 4 类信号触发条件，每个返回 (triggered: bool, description: str, direction_hint)：
  1. ma60_deviation  — 偏离 60 日均线过高时止盈/止损
  2. local_extreme   — 近 N 日局部极值附近
  3. volume_price_div — 量价背离（价格新高但量能萎缩）
  4. trend_break      — 突破/跌破关键均线
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class TriggerResult:
    name: str
    triggered: bool
    direction: str  # "bullish" / "bearish" / "neutral"
    description: str
    values: dict


class TriggerDetector:
    """对单票日线 + 技术指标数据检测关键位置。"""

    def __init__(
        self,
        ma60_threshold: float = 0.15,
        local_window: int = 20,
        local_pct: float = 0.05,
        divergence_window: int = 10,
    ):
        self.ma60_threshold = ma60_threshold
        self.local_window = local_window
        self.local_pct = local_pct
        self.divergence_window = divergence_window

    def detect_all(self, df: pd.DataFrame) -> list[TriggerResult]:
        """对已按 trade_date 升序排列的 daily+technical 合并 DataFrame 运行全部检测。"""
        if df is None or df.empty or len(df) < 5:
            return []
        results = [
            self._ma60_deviation(df),
            self._local_extreme(df),
            self._volume_price_divergence(df),
            self._trend_break(df),
        ]
        return [r for r in results if r is not None]

    # ------------------------------------------------------------------

    def _ma60_deviation(self, df: pd.DataFrame) -> TriggerResult | None:
        row = df.iloc[-1]
        close = _float(row, "close")
        ma60 = _float(row, "ma60")
        if close <= 0 or ma60 <= 0:
            return None
        bias = (close - ma60) / ma60

        if bias > self.ma60_threshold:
            return TriggerResult(
                name="ma60_deviation",
                triggered=True,
                direction="bearish",
                description=f"价格偏离MA60 {bias:+.1%}，超过+{self.ma60_threshold:.0%}阈值，注意止盈",
                values={"bias": round(bias, 4), "close": close, "ma60": round(ma60, 2)},
            )
        if bias < -self.ma60_threshold:
            return TriggerResult(
                name="ma60_deviation",
                triggered=True,
                direction="bullish",
                description=f"价格偏离MA60 {bias:+.1%}，低于-{self.ma60_threshold:.0%}阈值，可能超跌",
                values={"bias": round(bias, 4), "close": close, "ma60": round(ma60, 2)},
            )
        return TriggerResult(
            name="ma60_deviation", triggered=False, direction="neutral",
            description=f"MA60偏离 {bias:+.1%}，正常范围",
            values={"bias": round(bias, 4)},
        )

    def _local_extreme(self, df: pd.DataFrame) -> TriggerResult | None:
        closes = pd.to_numeric(df["close"], errors="coerce").dropna()
        if len(closes) < self.local_window:
            return None
        window = closes.tail(self.local_window)
        latest = float(closes.iloc[-1])
        hi = float(window.max())
        lo = float(window.min())
        rng = hi - lo
        if rng <= 0:
            return None

        pct_from_hi = (hi - latest) / rng
        pct_from_lo = (latest - lo) / rng

        if pct_from_hi <= self.local_pct:
            return TriggerResult(
                name="local_extreme",
                triggered=True,
                direction="bearish",
                description=f"价格处于近{self.local_window}日最高区间(距高点{pct_from_hi:.0%})，注意止盈",
                values={"close": latest, "high_20d": hi, "low_20d": lo},
            )
        if pct_from_lo <= self.local_pct:
            return TriggerResult(
                name="local_extreme",
                triggered=True,
                direction="bullish",
                description=f"价格处于近{self.local_window}日最低区间(距低点{pct_from_lo:.0%})，可能见底",
                values={"close": latest, "high_20d": hi, "low_20d": lo},
            )
        return TriggerResult(
            name="local_extreme", triggered=False, direction="neutral",
            description="价格在近期波动范围中间",
            values={"close": latest, "high_20d": hi, "low_20d": lo},
        )

    def _volume_price_divergence(self, df: pd.DataFrame) -> TriggerResult | None:
        if len(df) < self.divergence_window:
            return None
        recent = df.tail(self.divergence_window).copy()
        closes = pd.to_numeric(recent["close"], errors="coerce")
        volumes = pd.to_numeric(recent["volume"], errors="coerce")
        if closes.isna().all() or volumes.isna().all():
            return None

        price_trend = float(closes.iloc[-1]) - float(closes.iloc[0])
        first_half_vol = float(volumes.iloc[: len(volumes) // 2].mean())
        second_half_vol = float(volumes.iloc[len(volumes) // 2 :].mean())

        if first_half_vol <= 0:
            return None
        vol_change = (second_half_vol - first_half_vol) / first_half_vol

        if price_trend > 0 and vol_change < -0.20:
            return TriggerResult(
                name="volume_price_divergence",
                triggered=True,
                direction="bearish",
                description=f"量价背离: 价格上涨但成交量萎缩{vol_change:.0%}，上涨动能不足",
                values={"price_trend": round(price_trend, 2), "vol_change": round(vol_change, 3)},
            )
        if price_trend < 0 and vol_change < -0.20:
            return TriggerResult(
                name="volume_price_divergence",
                triggered=True,
                direction="bullish",
                description=f"缩量下跌: 价格下跌但量能萎缩{vol_change:.0%}，抛压减轻",
                values={"price_trend": round(price_trend, 2), "vol_change": round(vol_change, 3)},
            )
        return TriggerResult(
            name="volume_price_divergence", triggered=False, direction="neutral",
            description="量价配合正常",
            values={"price_trend": round(price_trend, 2), "vol_change": round(vol_change, 3)},
        )

    def _trend_break(self, df: pd.DataFrame) -> TriggerResult | None:
        if len(df) < 3:
            return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        close_now = _float(curr, "close")
        close_prev = _float(prev, "close")

        for ma_col in ("ma20", "ma60"):
            ma_now = _float(curr, ma_col)
            ma_prev = _float(prev, ma_col)
            if ma_now <= 0 or ma_prev <= 0:
                continue

            if close_prev <= ma_prev and close_now > ma_now:
                return TriggerResult(
                    name="trend_break",
                    triggered=True,
                    direction="bullish",
                    description=f"突破{ma_col.upper()}: 收盘价 {close_now:.2f} 上穿 {ma_col.upper()} {ma_now:.2f}",
                    values={"close": close_now, ma_col: round(ma_now, 2), "type": "break_above"},
                )
            if close_prev >= ma_prev and close_now < ma_now:
                return TriggerResult(
                    name="trend_break",
                    triggered=True,
                    direction="bearish",
                    description=f"跌破{ma_col.upper()}: 收盘价 {close_now:.2f} 下穿 {ma_col.upper()} {ma_now:.2f}",
                    values={"close": close_now, ma_col: round(ma_now, 2), "type": "break_below"},
                )

        return TriggerResult(
            name="trend_break", triggered=False, direction="neutral",
            description="均线无突破/跌破", values={},
        )


def _float(row: pd.Series, col: str) -> float:
    try:
        v = row.get(col)
        return float(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else 0.0
    except (TypeError, ValueError):
        return 0.0
