"""共享风控逻辑：硬止损 / 均线移动止盈 / 逻辑止损 / 左侧趋势过滤。

把投顾《圈子选股体系执行细则》里的风控规则抽成**纯函数 + 单调状态机**，
供两处共用，确保「回测怎么算、实盘就怎么发单」完全一致：
  - 回测引擎 :mod:`invest_model.backtest.cs_engine`（逐日循环，增量推进档位）
  - 实盘操作计划 :mod:`invest_model.orchestration.action_plan`（自建仓日回放重建档位）

移动止盈档位（单调递增，价格反弹不自动回补，回补留给调仓日）：
  0=满仓  1=破MA5减半  2=破MA10再减半(到1/4)  3=破MA20清仓
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# 各档「相对上一档」应保留的比例：破MA5减半、破MA10再减半、破MA20清零。
PER_STEP_KEEP: dict[int, float] = {1: 0.5, 2: 0.5, 3: 0.0}


@dataclass
class RiskConfig:
    enabled: bool = True
    hard_stop_pct: float = 0.08          # 单票较成本浮亏达此值清仓
    account_dd_stop: float = 0.15        # 账户较峰值回撤达此值清仓转现金（0=关闭）
    ma_trailing: bool = True             # 均线移动止盈
    trail_full: bool = False             # False=仅破MA20清仓（放宽，月度书默认）；True=破5减半/破10再减半/破20清仓
    trend_filter: bool = False           # 仅买 MA60 走平向上（左侧趋势过滤）
    trend_ma: int = 60
    intraday_valid_days: int = 3         # 早午盘信号默认有效交易日数


def ma_tail(series: pd.Series, n: int) -> float:
    """末值的 n 日简单均线；样本不足 n 返回 nan。"""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < n:
        return float("nan")
    return float(s.tail(n).mean())


def step_tier(close: float, ma5: float, ma10: float, ma20: float, cur_tier: int,
              full: bool = True) -> int:
    """单日推进移动止盈档位（单调，只升不降）。引擎逐日调用。

    full=True：破5减半(档1)/破10再减半(档2)/破20清仓(档3)。
    full=False（放宽）：仅破 MA20 清仓，忽略 MA5/MA10 的逐档减仓。
    """
    tier = cur_tier
    if np.isfinite(ma20) and close < ma20:
        return max(tier, 3)
    if not full:
        return tier
    if np.isfinite(ma10) and close < ma10:
        tier = max(tier, 2)
    elif np.isfinite(ma5) and close < ma5:
        tier = max(tier, 1)
    return tier


def keep_from_step(cur_tier: int, new_tier: int) -> float:
    """从 cur_tier 推进到 new_tier 时，应保留「当前仓位」的比例（各跨档因子连乘）。"""
    frac = 1.0
    for t in range(cur_tier + 1, new_tier + 1):
        frac *= PER_STEP_KEEP.get(t, 0.0)
    return frac


def replay_tier(close_hist: pd.Series, start_tier: int = 0, full: bool = True) -> int:
    """自建仓起逐日回放，重建当前移动止盈档位。实盘 action_plan 用。

    close_hist：建仓日起到评估日(含)的收盘价序列（index=trade_date，升序）。
    """
    s = pd.to_numeric(close_hist, errors="coerce")
    if s.empty:
        return start_tier
    ma5 = s.rolling(5).mean().to_numpy()
    ma10 = s.rolling(10).mean().to_numpy()
    ma20 = s.rolling(20).mean().to_numpy()
    closes = s.to_numpy()
    tier = start_tier
    for i in range(len(closes)):
        c = closes[i]
        if not np.isfinite(c):
            continue
        tier = step_tier(c, ma5[i], ma10[i], ma20[i], tier, full=full)
    return tier


@dataclass
class ExitDecision:
    action: str                 # "hold" | "trim" | "exit"
    keep_frac: float            # 应保留「当前仓位」的比例 (0~1)
    reason: str
    stop_price: float = float("nan")
    ma5: float = float("nan")
    ma10: float = float("nan")
    ma20: float = float("nan")
    new_tier: int = 0


def evaluate_holding(close_hist: pd.Series, cost: float, cfg: RiskConfig,
                     in_exit_codes: bool = False, prev_tier: int = 0) -> ExitDecision:
    """对单只持仓做风控评估，返回退出决策（回测/实盘共用同一套判定）。

    优先级：逻辑证伪 > 硬止损 > 均线移动止盈 > 持有。
    close_hist：截至评估日(含)的收盘价序列；cost：建仓成本价；
    prev_tier：评估日之前已触发的档位（回测逐日传入；实盘传「回放至昨日」的档位）。
    """
    s = pd.to_numeric(close_hist, errors="coerce").dropna()
    close = float(s.iloc[-1]) if not s.empty else float("nan")
    ma5, ma10, ma20 = (ma_tail(close_hist, n) for n in (5, 10, 20))
    has_cost = bool(cost) and np.isfinite(cost) and cost > 0
    stop_price = cost * (1 - cfg.hard_stop_pct) if has_cost else float("nan")
    base = dict(stop_price=stop_price, ma5=ma5, ma10=ma10, ma20=ma20, new_tier=prev_tier)

    # 1) 逻辑证伪 → 无条件清仓
    if in_exit_codes:
        return ExitDecision("exit", 0.0, "逻辑证伪清仓", **{**base, "new_tier": 3})
    # 2) 硬止损
    if cfg.hard_stop_pct and has_cost and np.isfinite(close) and close / cost - 1 <= -cfg.hard_stop_pct:
        return ExitDecision("exit", 0.0, f"硬止损(-{cfg.hard_stop_pct:.0%})", **{**base, "new_tier": 3})
    # 3) 均线移动止盈
    if cfg.ma_trailing and np.isfinite(close):
        tier = step_tier(close, ma5, ma10, ma20, prev_tier, full=cfg.trail_full)
        if tier >= 3:
            return ExitDecision("exit", 0.0, "破MA20清仓", **{**base, "new_tier": 3})
        if tier > prev_tier:
            keep = keep_from_step(prev_tier, tier)
            label = {1: "破MA5减半", 2: "破MA10减至1/4"}.get(tier, "移动止盈减仓")
            return ExitDecision("trim", keep, label, **{**base, "new_tier": tier})
    return ExitDecision("hold", 1.0, "持有", **base)


def trend_ok_close(close_hist: pd.Series, cfg: RiskConfig) -> bool:
    """左侧趋势过滤：MA60 走平或向上 且 收盘站上 MA60。样本不足视为通过。"""
    s = pd.to_numeric(close_hist, errors="coerce").dropna()
    n = cfg.trend_ma
    if len(s) < n + 5:
        return True  # 数据不足不拦
    ma = s.rolling(n).mean().dropna()
    if len(ma) < 6:
        return True
    slope = float(ma.iloc[-1] - ma.iloc[-6])  # 近 5 日 MA60 斜率
    return bool(s.iloc[-1] >= ma.iloc[-1] and slope >= 0)
