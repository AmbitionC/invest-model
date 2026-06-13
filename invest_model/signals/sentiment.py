"""情绪与事件信号生成器。

三类信号：
    turnover_extreme       换手率 20 日分位（放量=动量确认，看多为主；缩量偏空）
    holder_count_trend     股东户数环比（下降→筹码集中→看多）
    insider_net_30d        最近 30 天大股东净增减持股数

数据依赖：stock_fundamental.turnover_rate、stock_holder_count、stock_holder_trade
scope: history（按股票拿各自历史窗口）
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from invest_model.signals.base import (
    CategorizedSignalGenerator,
    Signal,
    SignalDirection,
    score_to_strength,
)
from invest_model.signals.normalize import value_to_score
from invest_model.signals.registry import register


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(f):
        return None
    return f


def _neutral(name: str, label: str, values: dict) -> Signal:
    return Signal(
        name=name,
        direction=SignalDirection.NEUTRAL,
        score=0.0,
        strength=score_to_strength(0.0),
        label=label,
        indicator_values=values,
    )


def _direction_of(score: float) -> SignalDirection:
    if score > 0.1:
        return SignalDirection.BULLISH
    if score < -0.1:
        return SignalDirection.BEARISH
    return SignalDirection.NEUTRAL


@register("sentiment")
class SentimentSignalGenerator(CategorizedSignalGenerator):
    """情绪/事件信号生成器。"""

    category = "sentiment"
    scope = "history"
    required_tables = ("stock_fundamental", "stock_holder_count", "stock_holder_trade")

    SIGNAL_NAMES = ("turnover_extreme", "holder_count_trend", "insider_net_30d")

    def required_columns(self) -> list[str]:
        return ["turnover_rate"]

    def validate_input(self, data: pd.DataFrame) -> bool:  # noqa: D401
        return True

    def generate(self, code: str, data: pd.DataFrame) -> list[Signal]:
        if data is None:
            data = pd.DataFrame()
        return [
            self._turnover_signal(data, 0.0),
            self._holder_count_signal(data),
            self._insider_signal(data),
        ]

    def generate_for_date(self, code: str, trade_date: str, context: dict) -> list[Signal]:
        turnover_map: dict[str, pd.DataFrame] = context.get("turnover_history", {}) or {}
        holder_cnt_map: dict[str, pd.DataFrame] = context.get("holder_count_history", {}) or {}
        insider_map: dict[str, pd.DataFrame] = context.get("insider_recent", {}) or {}
        pct_chg_map: dict[str, float] = context.get("pct_chg_map", {}) or {}

        turnover_df = turnover_map.get(code, pd.DataFrame())
        holder_df = holder_cnt_map.get(code, pd.DataFrame())
        insider_df = insider_map.get(code, pd.DataFrame())
        pct_chg = pct_chg_map.get(code, 0.0)

        signals: list[Signal] = []
        signals.append(self._turnover_signal(turnover_df, pct_chg))
        signals.append(self._holder_count_signal(holder_df))
        signals.append(self._insider_signal(insider_df))
        return signals

    # ── 单信号实现 ──────────────────────────────────────────

    def _turnover_signal(self, df: pd.DataFrame, pct_chg: float = 0.0) -> Signal:
        if df is None or df.empty or "turnover_rate" not in df.columns:
            return _neutral("turnover_extreme", "换手率数据缺失", {})
        s = pd.to_numeric(df["turnover_rate"], errors="coerce").dropna()
        if len(s) < 5:
            return _neutral("turnover_extreme", "换手率窗口不足", {})
        latest = float(s.iloc[-1])
        rank = float(s.rank(pct=True, method="average").iloc[-1])

        # 放量 = 动量确认：A股放量通常看多（资金涌入），除非伴随明显下跌
        if rank >= 0.90:
            if pct_chg > 1.0:
                score = 0.5
                label = f"放量上涨 ({latest:.2%}, 涨{pct_chg:+.1f}%)，动量强劲"
            elif pct_chg < -1.5:
                score = -0.4
                label = f"放量下跌 ({latest:.2%}, 跌{pct_chg:+.1f}%)，警惕出货"
            else:
                score = 0.15
                label = f"换手率极高 ({latest:.2%})，资金活跃"
        elif rank >= 0.80:
            if pct_chg > 1.0:
                score = 0.3
                label = f"温和放量上涨 ({latest:.2%})，量价配合"
            elif pct_chg < -1.5:
                score = -0.2
                label = f"换手率偏高 ({latest:.2%})，伴随下跌"
            else:
                score = 0.1
                label = f"换手率偏高 ({latest:.2%})，关注度提升"
        elif rank <= 0.05:
            score = -0.15
            label = f"换手率极度萎缩 ({latest:.2%})，交投清淡"
        elif rank <= 0.15:
            score = -0.05
            label = f"换手率偏低 ({latest:.2%})"
        elif 0.4 <= rank <= 0.7:
            score = 0.1
            label = f"换手率健康 ({latest:.2%})"
        else:
            score = 0.0
            label = f"换手率正常 ({latest:.2%})"

        return Signal(
            name="turnover_extreme",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"turnover_rate": latest, "turnover_rank_20d": rank, "pct_chg": pct_chg},
        )

    def _holder_count_signal(self, df: pd.DataFrame) -> Signal:
        if df is None or df.empty or "holder_num" not in df.columns:
            return _neutral("holder_count_trend", "股东户数数据缺失", {})
        s = pd.to_numeric(df["holder_num"], errors="coerce").dropna()
        if len(s) < 2:
            return _neutral("holder_count_trend", "股东户数历史不足", {})

        recent = float(s.iloc[-1])
        prev = float(s.iloc[-2])
        if prev <= 0:
            return _neutral("holder_count_trend", "股东户数基数异常", {})
        change = (recent - prev) / prev

        # 户数下降 → 筹码集中 → 看多
        thresholds = [(-0.15, 0.7), (-0.05, 0.4), (-0.01, 0.15), (0.01, 0.0), (0.05, -0.2), (0.15, -0.5), (10.0, -0.7)]
        score = value_to_score(change, thresholds, default=0.0)
        # 方向语义：负 change → 正 score（看多）
        score = -score
        if change < -0.05:
            label = f"股东户数较上期下降 {-change:.2%}，筹码明显集中"
        elif change < -0.01:
            label = f"股东户数小幅下降 ({-change:.2%})"
        elif change > 0.05:
            label = f"股东户数较上期增加 {change:.2%}，筹码分散"
        elif change > 0.01:
            label = f"股东户数小幅增加 ({change:.2%})"
        else:
            label = f"股东户数基本稳定 ({change:.2%})"

        return Signal(
            name="holder_count_trend",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"holder_num": recent, "holder_num_change": change},
        )

    def _insider_signal(self, df: pd.DataFrame) -> Signal:
        if df is None or df.empty or "change_vol" not in df.columns:
            return _neutral("insider_net_30d", "大股东变动数据缺失", {})

        tmp = df.copy()
        tmp["change_vol"] = pd.to_numeric(tmp["change_vol"], errors="coerce").fillna(0.0)
        if "trade_type" in tmp.columns:
            sign = tmp["trade_type"].astype(str).str.upper().map(
                lambda x: -1.0 if ("DE" in x or "减" in x or "SELL" in x) else 1.0
            )
            tmp["signed"] = tmp["change_vol"].abs() * sign
        else:
            tmp["signed"] = tmp["change_vol"]

        net = float(tmp["signed"].sum())

        # 单位是股数；用绝对量的万股做阈值，适合 A 股级别
        ten_thousand = net / 10000.0
        thresholds = [(-1000.0, -0.7), (-200.0, -0.4), (-10.0, -0.15), (10.0, 0.0), (200.0, 0.3), (1000.0, 0.6), (1e12, 0.8)]
        score = value_to_score(ten_thousand, thresholds, default=0.0)
        if ten_thousand > 200:
            label = f"30 日大股东净增持 {ten_thousand:.0f} 万股，看多信号"
        elif ten_thousand > 10:
            label = f"30 日大股东小幅增持 ({ten_thousand:.0f} 万股)"
        elif ten_thousand < -200:
            label = f"30 日大股东净减持 {-ten_thousand:.0f} 万股，看空信号"
        elif ten_thousand < -10:
            label = f"30 日大股东小幅减持 ({-ten_thousand:.0f} 万股)"
        else:
            label = "30 日内大股东无显著变动"

        return Signal(
            name="insider_net_30d",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"insider_net_vol_30d": net},
        )


# ── 预加载 ──────────────────────────────────────────


def prepare_sentiment_context(
    engine,
    codes: list[str],
    trade_date: str,
) -> dict:
    """预加载情绪/事件信号所需数据。

    - turnover: 从 stock_fundamental 拉最近 30 个交易日的 turnover_rate
    - holder_count: 拉最近 2 期
    - insider: 最近 30 日的 stock_holder_trade
    """
    from invest_model.repositories.base import BaseRepository
    from invest_model.signals.money_flow import _shift_trade_date

    base = BaseRepository(engine)
    if not codes:
        return {"turnover_history": {}, "holder_count_history": {}, "insider_recent": {}}

    placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
    code_params = {f"c{i}": c for i, c in enumerate(codes)}

    start_30 = _shift_trade_date(engine, trade_date, -30)
    turnover_df = base.read_sql(
        f"""
        SELECT code, trade_date, turnover_rate
        FROM stock_fundamental
        WHERE code IN ({placeholders})
          AND trade_date BETWEEN :start AND :end
        ORDER BY code, trade_date
        """,
        {**code_params, "start": start_30, "end": trade_date},
    )
    turnover_map: dict[str, pd.DataFrame] = {}
    if not turnover_df.empty:
        for code, sub in turnover_df.groupby("code"):
            turnover_map[code] = sub.reset_index(drop=True)

    holder_df = base.read_sql(
        f"""
        SELECT code, end_date, holder_num, holder_num_change
        FROM stock_holder_count
        WHERE code IN ({placeholders})
        ORDER BY code, end_date
        """,
        code_params,
    )
    holder_map: dict[str, pd.DataFrame] = {}
    if not holder_df.empty:
        for code, sub in holder_df.groupby("code"):
            holder_map[code] = sub.tail(6).reset_index(drop=True)

    # 大股东增减持：近 30 个自然日（ann_date 字符串比较）
    from datetime import datetime, timedelta

    try:
        ann_start_dt = datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=30)
        ann_start = ann_start_dt.strftime("%Y%m%d")
    except ValueError:
        ann_start = trade_date

    insider_df = base.read_sql(
        f"""
        SELECT code, ann_date, trade_type, change_vol
        FROM stock_holder_trade
        WHERE code IN ({placeholders})
          AND ann_date BETWEEN :start AND :end
        """,
        {**code_params, "start": ann_start, "end": trade_date},
    )
    insider_map: dict[str, pd.DataFrame] = {}
    if not insider_df.empty:
        for code, sub in insider_df.groupby("code"):
            insider_map[code] = sub.reset_index(drop=True)

    # 拉取最新日 pct_chg 用于换手率方向判断
    daily_chg_df = base.read_sql(
        f"""
        SELECT code, pct_chg
        FROM stock_daily
        WHERE code IN ({placeholders}) AND trade_date = :end
        """,
        {**code_params, "end": trade_date},
    )
    pct_chg_map: dict[str, float] = {}
    if not daily_chg_df.empty:
        for _, r in daily_chg_df.iterrows():
            try:
                pct_chg_map[r["code"]] = float(r["pct_chg"])
            except (TypeError, ValueError):
                pass

    return {
        "turnover_history": turnover_map,
        "holder_count_history": holder_map,
        "insider_recent": insider_map,
        "pct_chg_map": pct_chg_map,
    }
