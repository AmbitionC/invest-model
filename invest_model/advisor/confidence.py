"""置信度引擎。

base 分使用分位数映射：当前 |composite| 在该标的历史 |composite| 分布中的百分位。
无 profile 时退回 tanh fallback。

最终 confidence = clip(base + consistency + trigger_bonus - penalty, 0, 100)
"""

from __future__ import annotations

from dataclasses import dataclass
from math import tanh
from typing import TYPE_CHECKING, Sequence

from invest_model.advisor.triggers import TriggerResult

if TYPE_CHECKING:
    from invest_model.advisor.calibration import CalibrationProfile


@dataclass
class ConfidenceResult:
    confidence: int       # 0-100
    direction: str        # "bullish" / "bearish" / "neutral"
    base_score: float
    consistency_bonus: float
    trigger_bonus: float
    penalty: float
    explanation: str


class ConfidenceEngine:
    """根据多维信号计算综合置信度。"""

    def __init__(
        self,
        base_max: float = 60.0,
        fallback_threshold: float = 0.12,
        consistency_scale: float = 20.0,
        trigger_per_hit: float = 8.0,
        trigger_cap: float = 20.0,
        ambiguity_threshold: float = 0.04,
    ):
        self.base_max = base_max
        self.fallback_threshold = fallback_threshold
        self.consistency_scale = consistency_scale
        self.trigger_per_hit = trigger_per_hit
        self.trigger_cap = trigger_cap
        self.ambiguity_threshold = ambiguity_threshold

    def compute(
        self,
        composite: float,
        sub_scores: dict[str, float],
        triggers: Sequence[TriggerResult],
        profile: "CalibrationProfile | None" = None,
    ) -> ConfidenceResult:
        direction = "bullish" if composite > 0.01 else "bearish" if composite < -0.01 else "neutral"

        # 1) base: 分位数映射（有 profile）或 tanh fallback
        abs_comp = abs(composite)
        if profile and profile.abs_values:
            pct_rank = profile.percentile_rank(abs_comp)
            base = pct_rank * self.base_max
        else:
            base = self._fallback_base(abs_comp)

        # 2) consistency: 子分同向一致性
        consistency = self._consistency(sub_scores, direction)

        # 3) trigger bonus: 同向触发加分
        t_bonus = self._trigger_bonus(triggers, direction)

        # 4) penalty: 信号模糊惩罚
        penalty = 0.0
        explanation_parts = []
        if abs_comp < self.ambiguity_threshold:
            signs = [1 if v > 0 else -1 if v < 0 else 0 for v in sub_scores.values() if v != 0]
            if signs and len(set(signs)) > 1:
                base_penalty = 20.0
                if t_bonus > 0:
                    base_penalty = max(5.0, base_penalty - t_bonus)
                penalty = base_penalty
                explanation_parts.append("信号模糊")
                if t_bonus == 0:
                    direction = "neutral"

        raw = base + consistency + t_bonus - penalty
        confidence = int(max(0, min(100, round(raw))))

        if confidence < 40:
            direction = "neutral"

        if not explanation_parts:
            pct_label = f"P{int(base/self.base_max*100)}" if profile else "fallback"
            explanation_parts.append(f"强度={abs_comp:.3f}({pct_label})")
        if consistency > 5:
            explanation_parts.append(f"一致性+{consistency:.0f}")
        if t_bonus > 0:
            triggered_names = [t.name for t in triggers if t.triggered]
            explanation_parts.append(f"触发{','.join(triggered_names)}+{t_bonus:.0f}")
        if penalty > 0:
            explanation_parts.append(f"模糊-{penalty:.0f}")

        return ConfidenceResult(
            confidence=confidence,
            direction=direction,
            base_score=base,
            consistency_bonus=consistency,
            trigger_bonus=t_bonus,
            penalty=penalty,
            explanation="; ".join(explanation_parts),
        )

    def _fallback_base(self, abs_composite: float) -> float:
        """无 profile 时用 tanh fallback。"""
        normalized = abs_composite / self.fallback_threshold
        return self.base_max * tanh(normalized * 1.2)

    def _consistency(self, sub_scores: dict[str, float], direction: str) -> float:
        if not sub_scores:
            return 0.0
        values = [v for v in sub_scores.values() if v != 0]
        if not values:
            return 0.0
        if direction == "bullish":
            same = sum(1 for v in values if v > 0)
        elif direction == "bearish":
            same = sum(1 for v in values if v < 0)
        else:
            return 0.0
        ratio = same / len(values)
        return ratio * self.consistency_scale

    def _trigger_bonus(self, triggers: Sequence[TriggerResult], direction: str) -> float:
        hits = sum(1 for t in triggers if t.triggered and t.direction == direction)
        return min(hits * self.trigger_per_hit, self.trigger_cap)
