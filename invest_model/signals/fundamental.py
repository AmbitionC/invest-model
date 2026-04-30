"""基本面信号生成器。

五类信号（全部 score ∈ [-1, 1]）:
    pe_rank          行业内 PE-TTM 分位，低看多
    pb_rank          行业内 PB 分位，低看多
    roe_level        最新 ROE 绝对水平，高看多
    revenue_growth   营收同比，正值看多
    debt_level       资产负债率，高看空

数据依赖：stock_fundamental + stock_fina_indicator + stock_info。
scope: cross_section（需全市场截面做行业内分位与 zscore）
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
from invest_model.signals.normalize import (
    industry_neutral_zscore,
    quantile_rank,
    value_to_score,
)
from invest_model.signals.registry import register


def _safe_float(v: Any) -> float | None:
    """把混杂类型转成 float，非法返回 None。"""
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


@register("fundamental")
class FundamentalSignalGenerator(CategorizedSignalGenerator):
    """基本面信号生成器。

    工作流程（由 CompositeScorer 调度）：
        1. 预加载当日全市场截面估值（含行业），算行业内分位并缓存到 context
        2. 预加载每只股票最新财报指标（dict 形式）并缓存到 context
        3. 对每只股票产出 5 个 Signal
    """

    category = "fundamental"
    scope = "cross_section"
    required_tables = ("stock_fundamental", "stock_fina_indicator", "stock_info")

    SIGNAL_NAMES = ("pe_rank", "pb_rank", "roe_level", "revenue_growth", "debt_level")

    def required_columns(self) -> list[str]:
        return [
            "pe_ttm_rank",
            "pb_rank",
            "roe",
            "revenue_yoy",
            "debt_to_asset",
        ]

    def validate_input(self, data: pd.DataFrame) -> bool:  # noqa: D401
        """基本面信号做了列容错，不强制要求全部列存在。"""
        return True

    def generate(self, code: str, data: pd.DataFrame) -> list[Signal]:
        """兼容 SignalGenerator 接口：从单行 DataFrame 产出 5 个信号。

        期望 data 至少含以下列（缺失列会回退为 NEUTRAL）：
            pe_ttm_rank, pb_rank, roe, revenue_yoy, debt_to_asset
        """
        if data is None or data.empty:
            return [_neutral(n, f"{n} 数据缺失", {}) for n in self.SIGNAL_NAMES]
        row = data.iloc[-1]
        return [
            self._pe_signal(row),
            self._pb_signal(row),
            self._roe_signal(row),
            self._growth_signal(row),
            self._debt_signal(row),
        ]

    def generate_for_date(self, code: str, trade_date: str, context: dict) -> list[Signal]:
        cs: pd.DataFrame | None = context.get("cross_section")
        fina_map: dict[str, pd.Series] = context.get("fundamental_latest_fina", {}) or {}

        if cs is None or cs.empty or code not in cs.index:
            return [_neutral(n, f"{n} 截面数据缺失", {}) for n in self.SIGNAL_NAMES]

        row = cs.loc[code].copy()
        fina = fina_map.get(code)
        if fina is not None:
            for col in ("roe", "revenue_yoy", "debt_to_asset"):
                if col not in row or _safe_float(row.get(col)) is None:
                    row[col] = fina.get(col) if col in fina.index else None

        df_row = pd.DataFrame([row])
        return self.generate(code, df_row)

    # ── 单信号实现 ──────────────────────────────────────────

    def _pe_signal(self, row: pd.Series) -> Signal:
        rank = _safe_float(row.get("pe_ttm_rank"))
        pe = _safe_float(row.get("pe_ttm"))
        if rank is None:
            return _neutral("pe_rank", "PE 数据缺失", {"pe_ttm": pe})
        score = (0.5 - rank) * 2.0
        score = max(-1.0, min(1.0, score))
        if pe is not None and pe < 0:
            score = min(score, -0.2)
            label = f"PE 为负 ({pe:.1f})，盈利承压"
            return Signal(
                name="pe_rank",
                direction=SignalDirection.BEARISH,
                score=score,
                strength=score_to_strength(score),
                label=label,
                indicator_values={"pe_ttm": pe, "pe_ttm_rank": rank},
            )
        if score > 0.2:
            label = f"PE 处于行业低位 (分位 {rank:.0%}，PE={pe:.1f})" if pe else f"PE 分位 {rank:.0%}"
        elif score < -0.2:
            label = f"PE 处于行业高位 (分位 {rank:.0%}，PE={pe:.1f})" if pe else f"PE 分位 {rank:.0%}"
        else:
            label = f"PE 分位居中 ({rank:.0%})"
        return Signal(
            name="pe_rank",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"pe_ttm": pe, "pe_ttm_rank": rank},
        )

    def _pb_signal(self, row: pd.Series) -> Signal:
        rank = _safe_float(row.get("pb_rank"))
        pb = _safe_float(row.get("pb"))
        if rank is None:
            return _neutral("pb_rank", "PB 数据缺失", {"pb": pb})
        score = (0.5 - rank) * 2.0
        score = max(-1.0, min(1.0, score))
        if score > 0.2:
            label = f"PB 处于行业低位 (分位 {rank:.0%}，PB={pb:.2f})" if pb else f"PB 分位 {rank:.0%}"
        elif score < -0.2:
            label = f"PB 处于行业高位 (分位 {rank:.0%}，PB={pb:.2f})" if pb else f"PB 分位 {rank:.0%}"
        else:
            label = f"PB 分位居中 ({rank:.0%})"
        return Signal(
            name="pb_rank",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"pb": pb, "pb_rank": rank},
        )

    def _roe_signal(self, row: pd.Series) -> Signal:
        roe = _safe_float(row.get("roe"))
        if roe is None:
            return _neutral("roe_level", "ROE 数据缺失", {})
        thresholds = [(-0.05, -0.8), (0.03, -0.4), (0.08, 0.0), (0.15, 0.4), (0.25, 0.7), (1.0, 1.0)]
        score = value_to_score(roe, thresholds, default=0.0)
        if roe > 0.15:
            label = f"ROE 优秀 ({roe:.2%})"
        elif roe > 0.08:
            label = f"ROE 良好 ({roe:.2%})"
        elif roe > 0:
            label = f"ROE 偏弱 ({roe:.2%})"
        else:
            label = f"ROE 为负 ({roe:.2%})，盈利能力堪忧"
        return Signal(
            name="roe_level",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"roe": roe},
        )

    def _growth_signal(self, row: pd.Series) -> Signal:
        g = _safe_float(row.get("revenue_yoy"))
        if g is None:
            return _neutral("revenue_growth", "营收同比数据缺失", {})
        thresholds = [(-0.30, -0.8), (-0.10, -0.4), (0.0, -0.15), (0.10, 0.2), (0.25, 0.5), (0.50, 0.8), (5.0, 1.0)]
        score = value_to_score(g, thresholds, default=0.0)
        if g > 0.25:
            label = f"营收高增长 ({g:.2%})"
        elif g > 0.1:
            label = f"营收稳健增长 ({g:.2%})"
        elif g > 0:
            label = f"营收微增 ({g:.2%})"
        elif g > -0.1:
            label = f"营收小幅下滑 ({g:.2%})"
        else:
            label = f"营收显著下滑 ({g:.2%})"
        return Signal(
            name="revenue_growth",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"revenue_yoy": g},
        )

    def _debt_signal(self, row: pd.Series) -> Signal:
        d = _safe_float(row.get("debt_to_asset"))
        if d is None:
            return _neutral("debt_level", "资产负债率缺失", {})
        d_frac = d if d <= 1.5 else d / 100.0
        thresholds = [(0.30, 0.4), (0.50, 0.1), (0.65, 0.0), (0.80, -0.4), (1.5, -0.8)]
        score = value_to_score(d_frac, thresholds, default=0.0)
        if d_frac < 0.30:
            label = f"资产负债率低 ({d_frac:.1%})，财务稳健"
        elif d_frac < 0.60:
            label = f"资产负债率适中 ({d_frac:.1%})"
        elif d_frac < 0.75:
            label = f"资产负债率偏高 ({d_frac:.1%})"
        else:
            label = f"资产负债率高 ({d_frac:.1%})，杠杆风险"
        return Signal(
            name="debt_level",
            direction=_direction_of(score),
            score=score,
            strength=score_to_strength(score),
            label=label,
            indicator_values={"debt_to_asset": d_frac},
        )


# ── 供 CompositeScorer 调用的截面预处理 ──────────────────────────


def prepare_fundamental_context(
    engine,
    codes: list[str],
    trade_date: str,
) -> dict:
    """为 FundamentalSignalGenerator 预加载所需数据。

    返回 dict，包含：
        - "cross_section": DataFrame，以 code 为 index，含 pe_ttm / pb /
          行业 industry、估算得到的 pe_ttm_rank / pb_rank、最新 roe / revenue_yoy /
          debt_to_asset（merge 自 fina）。
        - "fundamental_latest_fina": {code -> pd.Series}
    """
    from invest_model.repositories.fundamental_signal_repo import FundamentalSignalRepository

    repo = FundamentalSignalRepository(engine)
    cs = repo.get_valuation_cross_section(trade_date)
    if cs.empty:
        return {"cross_section": pd.DataFrame(), "fundamental_latest_fina": {}}

    cs = cs.set_index("code")
    cs["pe_ttm_rank"] = _rank_within_industry(cs, "pe_ttm")
    cs["pb_rank"] = _rank_within_industry(cs, "pb")

    fina_map: dict[str, pd.Series] = {}
    for code in codes:
        fina = repo.get_latest_fina(code)
        if fina is not None:
            fina_map[code] = fina
            if code in cs.index:
                for col in ("roe", "revenue_yoy", "debt_to_asset"):
                    if col in fina.index and pd.notna(fina[col]):
                        cs.loc[code, col] = fina[col]

    return {
        "cross_section": cs,
        "fundamental_latest_fina": fina_map,
    }


def _rank_within_industry(df: pd.DataFrame, col: str) -> pd.Series:
    """行业内分位数。对缺失/负 PE 做特殊处理：负值视为极高分位（估值最贵/风险最大）。"""
    if col not in df.columns:
        return pd.Series(0.5, index=df.index)
    if "industry" not in df.columns:
        return quantile_rank(df[col])

    out = pd.Series(0.5, index=df.index)
    for ind, sub in df.groupby("industry", dropna=False):
        s = pd.to_numeric(sub[col], errors="coerce")
        if len(s.dropna()) < 5 or pd.isna(ind):
            out.loc[sub.index] = quantile_rank(s)
            continue
        pos = s.where(s > 0)
        if pos.dropna().empty:
            out.loc[sub.index] = quantile_rank(s)
            continue
        pos_ranks = pos.rank(pct=True, method="average")
        ranks = pos_ranks.where(s > 0, 1.0)
        ranks = ranks.fillna(0.5)
        out.loc[sub.index] = ranks
    return out
