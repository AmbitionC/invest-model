"""资金流信号生成器。

四类信号：
    main_inflow_5d    (buy_lg+buy_elg-sell_lg-sell_elg) 5 日累计 / 流通市值
    elg_ratio         超大单近 5 日净占比
    margin_delta_5d   融资余额 5 日变化率
    northbound_net_5d 北向资金 5 日累计净流入（沪股通+深股通，亿元）

数据依赖：stock_cashflow（moneyflow） + stock_margin + stock_fundamental（取流通市值）
         + stock_northbound_flow（沪深港通资金流向）
scope: history（需拿最近 5~10 个交易日的历史窗口）

说明：
    stock_cashflow 字段含义（tushare moneyflow）：
        buy_sm_vol / sell_sm_vol:   小单成交量（手）
        buy_md_vol / sell_md_vol:   中单成交量
        buy_lg_vol / sell_lg_vol:   大单成交量
        buy_elg_vol / sell_elg_vol: 特大单成交量
        net_mf_vol:                 净流入量（手）
    这里用"主力 = 大单 + 特大单"，不用单独金额列（源数据里只有成交量）。
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


@register("money_flow")
class MoneyFlowSignalGenerator(CategorizedSignalGenerator):
    """资金流信号生成器。

    `generate_for_date` 从 context 中取预加载好的 cashflow / margin / circ_mv
    并在内存中计算评分；`generate(code, df)` 作为备用入口，期望 df 是已
    merge 好的 (cashflow + margin + circ_mv) 时序 DataFrame。
    """

    category = "money_flow"
    scope = "history"
    required_tables = ("stock_cashflow", "stock_margin", "stock_fundamental")

    SIGNAL_NAMES = ("main_inflow_5d", "elg_ratio", "margin_delta_5d", "northbound_net_5d")

    def required_columns(self) -> list[str]:
        return [
            "trade_date",
            "buy_lg_vol", "sell_lg_vol",
            "buy_elg_vol", "sell_elg_vol",
            "rzye",
            "circ_mv",
        ]

    def validate_input(self, data: pd.DataFrame) -> bool:  # noqa: D401
        return True

    def generate(self, code: str, data: pd.DataFrame) -> list[Signal]:
        if data is None or data.empty:
            return [_neutral(n, f"{n} 资金流数据缺失", {}) for n in self.SIGNAL_NAMES]
        return [
            self._main_inflow(data),
            self._elg_ratio(data),
            self._margin_delta(data),
            _neutral("northbound_net_5d", "北向资金需通过 context 加载", {}),
        ]

    def generate_for_date(self, code: str, trade_date: str, context: dict) -> list[Signal]:
        cf_map: dict[str, pd.DataFrame] = context.get("cashflow_history", {}) or {}
        margin_map: dict[str, pd.DataFrame] = context.get("margin_history", {}) or {}
        circ_mv_map: dict[str, float] = context.get("circ_mv", {}) or {}
        nb_df: pd.DataFrame = context.get("northbound_history", pd.DataFrame())

        cf = cf_map.get(code)
        margin = margin_map.get(code)
        circ_mv = circ_mv_map.get(code)

        has_any = (cf is not None and not cf.empty) or (margin is not None and not margin.empty)
        if not has_any:
            signals = [_neutral(n, f"{n} 资金流数据缺失", {}) for n in ("main_inflow_5d", "elg_ratio", "margin_delta_5d")]
        else:
            df_pieces = []
            if cf is not None and not cf.empty:
                df_pieces.append(cf.set_index("trade_date"))
            if margin is not None and not margin.empty:
                df_pieces.append(margin.set_index("trade_date")[["rzye"]])
            if df_pieces:
                merged = pd.concat(df_pieces, axis=1).sort_index()
                merged = merged.reset_index().rename(columns={"index": "trade_date"})
            else:
                merged = pd.DataFrame()
            if circ_mv is not None and np.isfinite(circ_mv):
                merged["circ_mv"] = circ_mv
            signals = [
                self._main_inflow(merged),
                self._elg_ratio(merged),
                self._margin_delta(merged),
            ]

        signals.append(self._northbound_signal(nb_df))
        return signals

    # ── 单信号实现 ──────────────────────────────────────────

    def _main_inflow(self, df: pd.DataFrame) -> Signal:
        need = {"buy_lg_vol", "sell_lg_vol", "buy_elg_vol", "sell_elg_vol"}
        if not need.issubset(df.columns):
            return _neutral("main_inflow_5d", "缺少资金流数据", {})

        tail = df.sort_values("trade_date").tail(5).copy()
        if tail.empty:
            return _neutral("main_inflow_5d", "资金流窗口为空", {})

        for col in need:
            tail[col] = pd.to_numeric(tail[col], errors="coerce").fillna(0.0)

        net_vol = (
            tail["buy_lg_vol"] + tail["buy_elg_vol"]
            - tail["sell_lg_vol"] - tail["sell_elg_vol"]
        ).sum()

        circ_mv = _safe_float(tail["circ_mv"].iloc[-1]) if "circ_mv" in tail.columns else None

        if circ_mv is None or circ_mv <= 0:
            norm = None
        else:
            # circ_mv 单位：万元；net_vol 单位：手(=100 股)。
            # 这里只用量的方向做强弱判断，不追求精确货币口径。
            norm = net_vol / circ_mv

        if norm is None:
            thresholds = [(-5000000, -0.6), (-1000000, -0.3), (1000000, 0.0), (5000000, 0.3), (1e12, 0.6)]
            score = value_to_score(net_vol, thresholds, default=0.0)
            label = f"主力 5 日净流入量 {net_vol/10000:.1f} 万手"
        else:
            thresholds = [(-3.0, -0.8), (-0.5, -0.4), (0.5, 0.0), (3.0, 0.4), (100.0, 0.8)]
            score = value_to_score(norm, thresholds, default=0.0)
            label = f"主力 5 日净流入 {net_vol/10000:.1f} 万手 (流通占比 {norm:.2f})"

        return Signal(
            name="main_inflow_5d",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"net_vol_5d": float(net_vol), "circ_mv": circ_mv},
        )

    def _elg_ratio(self, df: pd.DataFrame) -> Signal:
        need = {"buy_elg_vol", "sell_elg_vol", "buy_lg_vol", "sell_lg_vol",
                "buy_md_vol", "sell_md_vol", "buy_sm_vol", "sell_sm_vol"}
        if not need.issubset(df.columns):
            return _neutral("elg_ratio", "缺少分单成交数据", {})

        tail = df.sort_values("trade_date").tail(5).copy()
        if tail.empty:
            return _neutral("elg_ratio", "分单数据窗口为空", {})

        for col in need:
            tail[col] = pd.to_numeric(tail[col], errors="coerce").fillna(0.0)

        elg_net = (tail["buy_elg_vol"] - tail["sell_elg_vol"]).sum()
        total = (
            tail["buy_sm_vol"] + tail["sell_sm_vol"]
            + tail["buy_md_vol"] + tail["sell_md_vol"]
            + tail["buy_lg_vol"] + tail["sell_lg_vol"]
            + tail["buy_elg_vol"] + tail["sell_elg_vol"]
        ).sum()

        if total <= 0:
            return _neutral("elg_ratio", "成交量为 0", {})

        ratio = elg_net / total
        thresholds = [(-0.10, -0.7), (-0.03, -0.3), (0.0, 0.0), (0.03, 0.3), (0.10, 0.7), (1.0, 1.0)]
        score = value_to_score(ratio, thresholds, default=0.0)
        if ratio > 0.03:
            label = f"超大单净流入占比 {ratio:.2%}，主力买盘积极"
        elif ratio < -0.03:
            label = f"超大单净流出占比 {ratio:.2%}，主力资金撤离"
        else:
            label = f"超大单资金基本平衡 ({ratio:.2%})"
        return Signal(
            name="elg_ratio",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"elg_net_ratio_5d": float(ratio)},
        )

    def _margin_delta(self, df: pd.DataFrame) -> Signal:
        if "rzye" not in df.columns:
            return _neutral("margin_delta_5d", "缺少融资数据", {})
        s = df.sort_values("trade_date")["rzye"].dropna()
        s = pd.to_numeric(s, errors="coerce").dropna()
        if len(s) < 2:
            return _neutral("margin_delta_5d", "融资窗口不足", {})

        earliest = float(s.iloc[0])
        latest = float(s.iloc[-1])
        if earliest <= 0:
            return _neutral("margin_delta_5d", "融资余额基数异常", {"rzye": latest})
        delta = (latest - earliest) / earliest

        thresholds = [(-0.05, -0.6), (-0.02, -0.3), (0.02, 0.0), (0.05, 0.3), (0.10, 0.6), (1.0, 0.9)]
        score = value_to_score(delta, thresholds, default=0.0)
        if delta > 0.02:
            label = f"融资余额 5 日增长 {delta:.2%}，杠杆资金看多"
        elif delta < -0.02:
            label = f"融资余额 5 日下降 {delta:.2%}，杠杆资金离场"
        else:
            label = f"融资余额稳定 ({delta:.2%})"
        return Signal(
            name="margin_delta_5d",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"rzye_latest": latest, "rzye_delta_5d": float(delta)},
        )

    @staticmethod
    def _northbound_signal(nb_df: pd.DataFrame) -> Signal:
        """北向资金 5 日累计净流入信号。

        north_money 字段来自 tushare moneyflow_hsgt，单位为万元。
        内部转换为亿元后做阈值判断。
        这是一个市场级别信号（非个股级别），对全部股票相同。
        """
        if nb_df is None or nb_df.empty or "north_money" not in nb_df.columns:
            return _neutral("northbound_net_5d", "北向资金数据缺失", {})

        s = pd.to_numeric(nb_df["north_money"], errors="coerce").dropna()
        if len(s) < 2:
            return _neutral("northbound_net_5d", "北向资金窗口不足", {})

        # 万元 → 亿元
        s_yi = s / 10000.0
        tail = s_yi.tail(5)
        net_5d = float(tail.sum())
        latest = float(s_yi.iloc[-1])

        # 阈值设计（亿元）：5日合计 >100亿 强看多, <-100亿 强看空
        thresholds = [
            (-200.0, -0.8), (-100.0, -0.5), (-30.0, -0.2),
            (30.0, 0.0), (100.0, 0.3), (200.0, 0.6), (1e6, 0.9),
        ]
        score = value_to_score(net_5d, thresholds, default=0.0)

        if net_5d > 100:
            label = f"北向资金 5 日累计净流入 {net_5d:.1f} 亿，外资大幅加仓"
        elif net_5d > 30:
            label = f"北向资金 5 日小幅净流入 {net_5d:.1f} 亿"
        elif net_5d < -100:
            label = f"北向资金 5 日累计净流出 {-net_5d:.1f} 亿，外资撤离"
        elif net_5d < -30:
            label = f"北向资金 5 日小幅净流出 {-net_5d:.1f} 亿"
        else:
            label = f"北向资金 5 日流向平衡 ({net_5d:+.1f} 亿)"

        return Signal(
            name="northbound_net_5d",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"north_net_5d_yi": net_5d, "north_latest_yi": latest},
        )


# ── 预加载函数 ──────────────────────────────────────────


def prepare_money_flow_context(
    engine,
    codes: list[str],
    trade_date: str,
    window_days: int = 10,
) -> dict:
    """预加载每只股票最近 window_days 天的资金流/融资余额和最新流通市值。"""
    from invest_model.repositories.base import BaseRepository

    base = BaseRepository(engine)
    start = _shift_trade_date(engine, trade_date, -window_days)

    cf_map: dict[str, pd.DataFrame] = {}
    margin_map: dict[str, pd.DataFrame] = {}
    circ_mv_map: dict[str, float] = {}

    if not codes:
        return {"cashflow_history": cf_map, "margin_history": margin_map, "circ_mv": circ_mv_map}

    placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
    code_params = {f"c{i}": c for i, c in enumerate(codes)}

    cf_df = base.read_sql(
        f"""
        SELECT code, trade_date,
               buy_sm_vol, sell_sm_vol,
               buy_md_vol, sell_md_vol,
               buy_lg_vol, sell_lg_vol,
               buy_elg_vol, sell_elg_vol,
               net_mf_vol
        FROM stock_cashflow
        WHERE code IN ({placeholders})
          AND trade_date BETWEEN :start AND :end
        ORDER BY code, trade_date
        """,
        {**code_params, "start": start, "end": trade_date},
    )
    if not cf_df.empty:
        for code, sub in cf_df.groupby("code"):
            cf_map[code] = sub.reset_index(drop=True)

    margin_df = base.read_sql(
        f"""
        SELECT code, trade_date, rzye
        FROM stock_margin
        WHERE code IN ({placeholders})
          AND trade_date BETWEEN :start AND :end
        ORDER BY code, trade_date
        """,
        {**code_params, "start": start, "end": trade_date},
    )
    if not margin_df.empty:
        for code, sub in margin_df.groupby("code"):
            margin_map[code] = sub.reset_index(drop=True)

    fund_df = base.read_sql(
        f"""
        SELECT code, circ_mv
        FROM stock_fundamental
        WHERE code IN ({placeholders}) AND trade_date = :date
        """,
        {**code_params, "date": trade_date},
    )
    if not fund_df.empty:
        for _, r in fund_df.iterrows():
            if pd.notna(r["circ_mv"]):
                circ_mv_map[r["code"]] = float(r["circ_mv"])

    # 北向资金（市场级别，所有股票共享）
    nb_df = base.read_sql(
        """
        SELECT trade_date, north_money
        FROM stock_northbound_flow
        WHERE trade_date BETWEEN :start AND :end
        ORDER BY trade_date
        """,
        {"start": start, "end": trade_date},
    )

    return {
        "cashflow_history": cf_map,
        "margin_history": margin_map,
        "circ_mv": circ_mv_map,
        "northbound_history": nb_df,
    }


def _shift_trade_date(engine, trade_date: str, days: int) -> str:
    """以 trade_calendar 为准，返回交易日 shift。days 负数表示往前数。"""
    from invest_model.repositories.calendar_repo import CalendarRepository

    cal = CalendarRepository(engine)
    if days >= 0:
        dates = cal.get_trade_dates(trade_date, "20991231")
        idx = min(days, max(len(dates) - 1, 0))
        return dates[idx] if dates else trade_date
    back_days = -days
    start = "19900101"
    dates = cal.get_trade_dates(start, trade_date)
    if not dates:
        return trade_date
    idx = max(len(dates) - 1 - back_days, 0)
    return dates[idx]
