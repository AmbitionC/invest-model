"""评分归因与可读摘要。

输入 `SignalSnapshot`（可能含多个类别），输出：
- top_bullish / top_bearish：各取若干条按 |score| 排序的信号
- narrative：生成人类可读的一句话摘要
"""

from __future__ import annotations

from invest_model.signals.base import Signal, SignalDirection, SignalSnapshot


def top_bullish(snapshot: SignalSnapshot, n: int = 3) -> list[Signal]:
    sigs = [s for s in snapshot.signals if s.direction == SignalDirection.BULLISH]
    return sorted(sigs, key=lambda s: abs(s.score), reverse=True)[:n]


def top_bearish(snapshot: SignalSnapshot, n: int = 3) -> list[Signal]:
    sigs = [s for s in snapshot.signals if s.direction == SignalDirection.BEARISH]
    return sorted(sigs, key=lambda s: abs(s.score), reverse=True)[:n]


def narrative(snapshot: SignalSnapshot, max_each: int = 2) -> str:
    """生成 "看多：..；看空：.." 的一句话摘要。"""
    bulls = top_bullish(snapshot, max_each)
    bears = top_bearish(snapshot, max_each)

    parts: list[str] = []
    if bulls:
        bull_text = "、".join([s.label for s in bulls])
        parts.append(f"看多：{bull_text}")
    if bears:
        bear_text = "、".join([s.label for s in bears])
        parts.append(f"看空：{bear_text}")

    if not parts:
        return "各项指标表现平稳，无明显信号。"

    comp = snapshot.composite_score
    tone = "整体偏多" if comp > 0.1 else "整体偏空" if comp < -0.1 else "多空相当"
    return f"{tone}（综合 {comp:+.2f}）。" + "；".join(parts) + "。"
