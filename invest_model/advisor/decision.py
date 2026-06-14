"""ML 驱动的决策层：把多 horizon 预测收益映射为目标仓位与具体操作。

设计原则：
1. 输出"目标仓位"而非"操作事件"
2. 仅当 |target - current| 超过 min_trade_size 才触发操作
3. 保留：止盈覆盖、非对称阈值（买谨慎卖灵活）、安全边际仓位调节
4. 通过 DecisionConfig 暴露执行档位（normal/confident/strict）+ 冷却 + 反向阈值

`make_decision` 是主入口，给定预测和当前持仓 + 冷却信息，输出 (action, target_position, ...)。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

# ── 决策档位（与持久化层 action 字段保持兼容）──

ACTION_LABELS: dict[str, str] = {
    "strong_buy": "强买",
    "buy": "买入",
    "add": "加仓",
    "hold": "观望",
    "reduce": "减仓",
    "clear": "清仓",
}

# ── 默认配置 ──

DEFAULT_HORIZON_WEIGHTS: dict[int, float] = {3: 0.3, 5: 0.5, 10: 0.2}

MAX_POSITION: float = 0.5         # 单票最大仓位
STRONG_BUY_DELTA: float = 0.20    # delta > 20% 视为强买
CLEAR_THRESHOLD: float = 0.30     # delta < -30% 视为清仓

# 非对称阈值倍率
BUY_THRESHOLD_MULT: float = 1.10  # 买入需要的强度上浮 10%

# 触发止盈的多重条件门槛
TAKE_PROFIT_MA60_BIAS: float = 0.15
TAKE_PROFIT_5D_DROP: float = -0.03

# 兼容旧接口：保留 module-level 常量供外部 import
MIN_TRADE_SIZE: float = 0.05
SCORE_TO_POSITION_SCALE: float = 50.0


# ── DecisionConfig ──────────────────────────────────────


@dataclass
class DecisionConfig:
    """决策层可调参数集中托管。

    通过 advisor / backtest config 传入，支持三档执行策略以及冷却机制：

    Attributes
    ----------
    execution_tier : str
        normal     - 全 5 档（strong_buy / buy / add / reduce / clear）
        confident  - 仅保留 buy + clear（小幅 add/reduce 降级为 hold）
        strict     - 仅保留 strong_buy + clear（其余降级为 hold）
    min_trade_size : float
        | target - current | < min_trade_size 时不操作
    score_to_position_scale : float
        score(log return) → tanh 映射的灵敏度。50 → 0.02 score 对应 ~76% 满仓；
        默认调低到 30 让仓位变化更平滑。
    sell_score_threshold : float
        score < sell_score_threshold 才允许"模型看空 → 强制减仓"。
        旧逻辑用 0，被微小负噪声触发；默认改为 -0.005。
    min_holding_days : int
        新开仓后 N 个交易日内不准 reduce/clear（防 T+1 反向）
    min_flat_days : int
        清仓后 N 个交易日内不准 buy/strong_buy（防 T+1 重买）
    take_profit_min_conditions : int
        止盈触发要求满足条件数（旧: 2/3；默认提到 3/3 更严）
    buy_threshold : float
        score 触发买入的基础阈值（log return）
    """

    execution_tier: str = "normal"
    min_trade_size: float = 0.05
    score_to_position_scale: float = 30.0
    sell_score_threshold: float = -0.003
    min_holding_days: int = 3
    min_flat_days: int = 2
    take_profit_min_conditions: int = 3
    buy_threshold: float = 0.003
    max_single_position: float = 0.20    # 单票最大仓位上限（20%）
    stop_loss_threshold: float = -0.10   # 持仓期间跌幅触发止损（-10%）

    def __post_init__(self) -> None:
        if self.execution_tier not in ("normal", "confident", "strict"):
            raise ValueError(
                f"execution_tier 必须是 normal/confident/strict 之一: {self.execution_tier}"
            )


DEFAULT_DECISION_CONFIG = DecisionConfig()


@dataclass
class DecisionResult:
    """决策层输出。"""
    action: str
    target_position: float
    current_position: float
    delta_position: float
    horizon_score: float          # 多 horizon 加权后的得分
    safety_margin: float          # 0~1，安全边际
    take_profit_triggered: bool
    take_profit_reason: str
    notes: list[str]              # 决策过程文字说明


# ── 安全边际 ──────────────────────────────────────────


def calc_safety_margin(df_ts: pd.DataFrame, direction_sign: int) -> float:
    """根据 60 日价格位置 + MA20 偏离 + 波动率计算安全边际 (0-1)。

    direction_sign : +1 看多 / -1 看空 / 0 中性
    """
    if df_ts is None or df_ts.empty or len(df_ts) < 10:
        return 0.5

    closes = pd.to_numeric(df_ts["close"], errors="coerce").dropna()
    if len(closes) < 10:
        return 0.5

    close = float(closes.iloc[-1])
    window = closes.tail(60) if len(closes) >= 60 else closes
    hi = float(window.max())
    lo = float(window.min())
    position = (close - lo) / (hi - lo) if hi > lo else 0.5

    last = df_ts.iloc[-1]
    ma20 = _safe_float(last.get("ma20"))
    bias = (close - ma20) / ma20 if ma20 > 0 else 0.0

    rets = closes.pct_change().dropna().tail(20)
    vol = float(rets.std()) if len(rets) > 2 else 0.02

    if direction_sign > 0:
        position_score = 1.0 - position
    elif direction_sign < 0:
        position_score = position
    else:
        position_score = 0.5

    bias_score = max(0.0, 1.0 - abs(bias) * 5)
    vol_penalty = min(vol * 25, 0.2)
    margin = (position_score * 0.5 + bias_score * 0.3 + 0.2) * (1.0 - vol_penalty)
    return float(np.clip(margin, 0.0, 1.0))


# ── 止盈覆盖 ──────────────────────────────────────────


def check_take_profit(
    df_ts: pd.DataFrame,
    min_conditions: int = 3,
) -> tuple[bool, str]:
    """检查是否满足止盈条件（即使预测仍看多也应减仓）。

    候选条件（最多 3 项）：
      1. 价格相对 MA60 偏离 > +15%
      2. 处于近 20 日最高 5% 区间
      3. 近 5 日跌幅 > 3%

    满足 >= min_conditions 项时触发。默认 3/3 严格触发。
    """
    if df_ts is None or df_ts.empty or len(df_ts) < 6:
        return False, ""

    last = df_ts.iloc[-1]
    close = _safe_float(last.get("close"))
    ma60 = _safe_float(last.get("ma60"))

    reasons: list[str] = []

    if close > 0 and ma60 > 0:
        bias = (close - ma60) / ma60
        if bias > TAKE_PROFIT_MA60_BIAS:
            reasons.append(f"MA60偏离{bias:+.0%}")

    closes = pd.to_numeric(df_ts["close"], errors="coerce").dropna()
    if len(closes) >= 20:
        window = closes.tail(20)
        hi = float(window.max())
        lo = float(window.min())
        if hi > lo:
            pct_from_hi = (hi - close) / (hi - lo)
            if pct_from_hi <= 0.05:
                reasons.append("20日高点")

    if len(closes) >= 6:
        p0 = float(closes.iloc[-6])
        if p0 > 0:
            ret_5d = (close - p0) / p0
            if ret_5d < TAKE_PROFIT_5D_DROP:
                reasons.append(f"5日跌{ret_5d:.1%}")

    if len(reasons) >= max(1, min_conditions):
        return True, " + ".join(reasons)
    return False, ""


# ── horizon 加权得分 → 目标仓位 ──────────────────────────────


def horizon_weighted_score(
    predictions: dict[int, float],
    weights: dict[int, float] | None = None,
) -> float:
    """对多 horizon 预测做权重平均（log return 单位）。"""
    weights = weights or DEFAULT_HORIZON_WEIGHTS
    if not predictions:
        return 0.0
    total_w = sum(weights.get(h, 0.0) for h in predictions.keys())
    if total_w <= 0:
        return float(np.mean(list(predictions.values())))
    return sum(predictions[h] * weights.get(h, 0.0) for h in predictions.keys()) / total_w


def score_to_target_position(
    score: float,
    safety_margin: float,
    asymmetric: bool = True,
    buy_threshold: float = 0.005,
    score_scale: float = SCORE_TO_POSITION_SCALE,
    max_position: float = MAX_POSITION,
) -> float:
    """把 score(log return) 映射为目标仓位 (0 ~ max_position)。

    - score > 0：看多，仓位与 score 成 tanh 关系，并乘 safety_margin
    - score < 0：目标仓位为 0（不持有看空）
    - 默认非对称：buy_threshold 上浮 10%（更谨慎开仓）

    Parameters
    ----------
    score_scale : float
        tanh 灵敏度，越大越早饱和。DecisionConfig.score_to_position_scale 透传。
    max_position : float
        单票最大仓位上限，由 DecisionConfig.max_single_position 透传。
    """
    if asymmetric:
        eff_threshold = buy_threshold * BUY_THRESHOLD_MULT
    else:
        eff_threshold = buy_threshold

    if score <= eff_threshold:
        return 0.0

    excess = score - eff_threshold
    raw = float(np.tanh(excess * score_scale))
    target = raw * max_position * safety_margin
    return float(np.clip(target, 0.0, max_position))


# ── 操作映射 ──────────────────────────────────────────


def target_to_action(
    target: float,
    current: float,
    take_profit: bool = False,
    cfg: DecisionConfig | None = None,
) -> tuple[str, float]:
    """根据目标仓位与当前仓位的差，映射到 5 档操作。

    `cfg.execution_tier` 控制"哪些档位被允许执行"：
      - normal     -> 5 档全部保留
      - confident  -> 小幅 add/reduce 降级为 hold；buy/clear/strong_buy 保留
      - strict     -> 仅 strong_buy / clear / hold 保留

    Returns
    -------
    (action, delta_position)
    """
    cfg = cfg or DEFAULT_DECISION_CONFIG
    delta = round(target - current, 4)

    if take_profit and current > 0:
        if target == 0:
            return _apply_tier("clear", delta, cfg)
        return _apply_tier("reduce", delta, cfg)

    if abs(delta) < cfg.min_trade_size:
        return "hold", delta

    if delta > 0:
        if delta >= STRONG_BUY_DELTA and current < 0.05:
            return _apply_tier("strong_buy", delta, cfg)
        if current < 0.05:
            return _apply_tier("buy", delta, cfg)
        return _apply_tier("add", delta, cfg)
    else:
        if abs(delta) >= CLEAR_THRESHOLD or target <= 0.0:
            return _apply_tier("clear", delta, cfg)
        return _apply_tier("reduce", delta, cfg)


def _apply_tier(
    action: str,
    delta: float,
    cfg: DecisionConfig,
) -> tuple[str, float]:
    """根据 execution_tier 把不合规档位降级为 hold（delta 归零）。"""
    tier = cfg.execution_tier

    if tier == "normal":
        return action, delta

    if tier == "confident":
        # 仅保留 strong_buy / buy / clear；小幅 add / reduce 降级
        if action in ("strong_buy", "buy", "clear"):
            return action, delta
        # add / reduce 大幅时仍保留（视作 buy / clear 的弱化版）
        if action == "add" and abs(delta) >= 0.15:
            return "add", delta
        if action == "reduce" and abs(delta) >= 0.20:
            return "reduce", delta
        return "hold", 0.0

    if tier == "strict":
        # 仅保留 strong_buy / clear
        if action in ("strong_buy", "clear"):
            return action, delta
        return "hold", 0.0

    return action, delta


# ── 冷却判定 ──────────────────────────────────────────


def _trade_days_between(d1: str | None, d2: str, trade_dates: list[str] | None) -> int | None:
    """估算两个 trade_date 之间相隔的交易日数。

    若提供 trade_dates 列表则严格按交易日计；否则用自然日除以 1.4 的近似。
    返回 None 表示 d1 缺失。
    """
    if not d1 or not d2:
        return None
    if trade_dates is not None:
        try:
            i1 = trade_dates.index(d1)
            i2 = trade_dates.index(d2)
            return abs(i2 - i1)
        except ValueError:
            pass
    try:
        from datetime import datetime
        nd = (datetime.strptime(d2, "%Y%m%d") - datetime.strptime(d1, "%Y%m%d")).days
        return max(0, int(round(nd / 1.4)))  # 1 周 ≈ 5 交易日 / 7 自然日 → ÷ 1.4
    except Exception:
        return None


def _enforce_cooldown(
    action: str,
    cfg: DecisionConfig,
    current_position: float,
    trade_date: str,
    last_open_date: str | None,
    last_clear_date: str | None,
    trade_dates: list[str] | None,
    notes: list[str],
) -> str:
    """在 action 上叠加冷却规则。返回（可能被改写为 hold 的）action。"""
    # 持有期：开仓后 N 个交易日内不允许 reduce/clear（止盈仍允许通过）
    if action in ("reduce", "clear") and current_position > 0:
        days = _trade_days_between(last_open_date, trade_date, trade_dates)
        if days is not None and days < cfg.min_holding_days:
            notes.append(
                f"冷却(持有期{days}/{cfg.min_holding_days}d): {action} → hold"
            )
            return "hold"

    # 空仓期：清仓后 N 个交易日内不允许重新 buy/strong_buy
    if action in ("buy", "strong_buy"):
        days = _trade_days_between(last_clear_date, trade_date, trade_dates)
        if days is not None and days < cfg.min_flat_days:
            notes.append(
                f"冷却(空仓期{days}/{cfg.min_flat_days}d): {action} → hold"
            )
            return "hold"

    return action


# ── 主入口 ──────────────────────────────────────────


def make_decision(
    predictions: dict[int, float],
    current_position: float,
    df_ts: pd.DataFrame,
    horizon_weights: dict[int, float] | None = None,
    cfg: DecisionConfig | None = None,
    trade_date: str = "",
    last_open_date: str | None = None,
    last_clear_date: str | None = None,
    trade_dates: list[str] | None = None,
) -> DecisionResult:
    """ML 决策主入口。

    Parameters
    ----------
    predictions : dict[int, float]
        ML 输出的 {horizon: pred_log_return}
    current_position : float
        当前持仓比例 0~1
    df_ts : pd.DataFrame
        近 60+ 日 daily+technical 合并表，用于安全边际/止盈检查
    horizon_weights : dict[int, float] | None
        多 horizon 加权策略；若为 None 用 DEFAULT_HORIZON_WEIGHTS
    cfg : DecisionConfig | None
        决策层配置（执行档位/冷却/阈值），为 None 时使用 DEFAULT_DECISION_CONFIG
    trade_date : str
        当前交易日（YYYYMMDD），用于冷却天数判定
    last_open_date / last_clear_date : str | None
        该标的最近一次开仓 / 清仓的交易日，由调用方维护
    trade_dates : list[str] | None
        全交易日历列表（升序），用于精确计算交易日间隔；缺失时退化为自然日近似
    """
    cfg = cfg or DEFAULT_DECISION_CONFIG
    notes: list[str] = []

    # ── 止损检查（优先于所有其他逻辑）──
    # 使用 pct_chg 累乘计算持仓期间收益，避免除权日用收盘价对比产生误触发
    if (
        current_position > 0
        and last_open_date
        and cfg.stop_loss_threshold is not None
        and df_ts is not None
        and not df_ts.empty
        and "pct_chg" in df_ts.columns
    ):
        ts_sorted = df_ts.sort_values("trade_date")
        since_open = ts_sorted[ts_sorted["trade_date"] > last_open_date]
        if not since_open.empty:
            null_pct = since_open["pct_chg"].isna().mean()
            if null_pct > 0.2:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    f"止损计算: pct_chg 缺失率 {null_pct:.0%}（>{last_open_date}），结果可能低估实际亏损"
                )
            pnl = (since_open["pct_chg"].fillna(0) / 100 + 1).prod() - 1
            if pnl < cfg.stop_loss_threshold:
                return DecisionResult(
                    action="clear",
                    target_position=0.0,
                    current_position=round(current_position, 4),
                    delta_position=round(-current_position, 4),
                    horizon_score=0.0,
                    safety_margin=0.0,
                    take_profit_triggered=False,
                    take_profit_reason="",
                    notes=[f"止损: 持仓跌幅 {pnl:.1%} < {cfg.stop_loss_threshold:.0%}"],
                )

    score = horizon_weighted_score(predictions, horizon_weights)

    direction_sign = 1 if score > 0 else (-1 if score < 0 else 0)
    safety = calc_safety_margin(df_ts, direction_sign)

    take_profit, tp_reason = check_take_profit(
        df_ts, min_conditions=cfg.take_profit_min_conditions
    )

    target = score_to_target_position(
        score,
        safety_margin=safety,
        buy_threshold=cfg.buy_threshold,
        score_scale=cfg.score_to_position_scale,
        max_position=cfg.max_single_position,
    )

    # 死区：score 在 (sell_threshold, buy_threshold * BUY_THRESHOLD_MULT] 之间时，
    # 视为"无明确方向"，不主动调仓 → target 保持 current_position，避免 score 微弱
    # 负噪声触发 target=0 → 强制 clear 的旧 bug。
    if score <= cfg.buy_threshold * BUY_THRESHOLD_MULT and score >= cfg.sell_score_threshold:
        if not take_profit:
            target = current_position
            if abs(score) > 1e-6:
                notes.append(
                    f"死区(score={score:+.4f} 在 [{cfg.sell_score_threshold:+.4f},"
                    f" {cfg.buy_threshold * BUY_THRESHOLD_MULT:+.4f}] 内): 保持仓位"
                )

    if take_profit and current_position > 0:
        forced_target = min(target, current_position * 0.5, cfg.max_single_position)
        target = forced_target
        notes.append(f"止盈覆盖({tp_reason}): 目标仓位降至 {target:.0%}")

    action, delta = target_to_action(target, current_position, take_profit=take_profit, cfg=cfg)

    # 模型看空（score < sell_threshold）且当前持有但 action 仍为 hold（被死区或 tier
    # 抑制）→ 强制至少 reduce，保留快速止损能力。
    if score < cfg.sell_score_threshold and current_position > 0:
        if action == "hold":
            forced_action, forced_delta = _apply_tier(
                "reduce", round(0.0 - current_position, 4), cfg
            )
            if forced_action != "hold":
                action, delta = forced_action, forced_delta
                target = max(0.0, current_position + delta)
                notes.append(
                    f"模型看空(score={score:+.4f} < {cfg.sell_score_threshold:+.4f}): 强制减仓 → {action}"
                )

    # 冷却（持有期 / 空仓期）
    if cfg.min_holding_days > 0 or cfg.min_flat_days > 0:
        cooled = _enforce_cooldown(
            action,
            cfg=cfg,
            current_position=current_position,
            trade_date=trade_date,
            last_open_date=last_open_date,
            last_clear_date=last_clear_date,
            trade_dates=trade_dates,
            notes=notes,
        )
        if cooled != action:
            action = cooled
            delta = 0.0
            target = current_position  # 不改变目标

    notes.append(
        f"score={score:+.4f}, safety={safety:.2f}, "
        f"target={target:.0%}, current={current_position:.0%}, tier={cfg.execution_tier}"
    )

    return DecisionResult(
        action=action,
        target_position=round(target, 4),
        current_position=round(current_position, 4),
        delta_position=round(delta, 4),
        horizon_score=round(float(score), 6),
        safety_margin=round(safety, 4),
        take_profit_triggered=take_profit,
        take_profit_reason=tp_reason,
        notes=notes,
    )


# ── helpers ─────────────────────────────────────────


def _safe_float(v) -> float:
    try:
        if v is None:
            return 0.0
        v = float(v)
        return v if np.isfinite(v) else 0.0
    except (TypeError, ValueError):
        return 0.0
