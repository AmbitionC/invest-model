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
    time_stop_days: int = 0              # 时间止损：买入 N 日横盘未创新高→减仓（0=关闭）
    time_stop_keep: float = 0.5          # 触发时保留比例（0.5=减半，0=离场）
    profit_protect: bool = True          # 盈利保护：浮盈达标后按「自峰值回撤」锁盈
    pp_trigger: float = 0.15             # 持有期峰值收盘较成本浮盈达此值 → 保护启动
    pp_trim_dd: float = 0.08             # 自峰值回撤达此值 → 减半锁盈
    pp_exit_dd: float = 0.12             # 自峰值回撤达此值 → 清仓止盈
    pp_trim_keep: float = 0.5            # 减半档保留比例


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


def time_stop(hold_hist: pd.Series, cfg: RiskConfig, prev_tier: int = 0) -> ExitDecision | None:
    """时间止损（手册第3步）：买入后 N 日既未触发移动止盈、又未破位，且横盘未创新高
    → 减仓/离场。仅在持仓未触发其它风控时检查。hold_hist 为自建仓日起的收盘序列。

    返回 ExitDecision（trim/exit）或 None（不触发）。
    """
    if not cfg.time_stop_days:
        return None
    s = pd.to_numeric(hold_hist, errors="coerce").dropna()
    if len(s) < cfg.time_stop_days:
        return None
    entry_close = float(s.iloc[0])
    made_new_high = bool((s.iloc[1:] > entry_close).any())
    if prev_tier == 0 and not made_new_high:           # 横盘、未启动、未减仓
        keep = cfg.time_stop_keep
        act = "exit" if keep <= 0 else "trim"
        return ExitDecision(act, keep, f"时间止损({cfg.time_stop_days}日横盘未创新高)",
                            new_tier=max(prev_tier, 1))
    return None


def pp_step(close: float, peak: float, cost: float, cfg: RiskConfig, cur_tier: int) -> int:
    """单日推进盈利保护档位（单调，只升不降）。0=未触发 1=减半锁盈 2=清仓止盈。

    peak 为持有期内截至当日的最高收盘。峰值较成本浮盈未达 pp_trigger 时保护不启动。
    """
    if not (np.isfinite(close) and np.isfinite(peak) and peak > 0 and cost > 0):
        return cur_tier
    if peak / cost - 1 < cfg.pp_trigger:
        return cur_tier
    dd = 1 - close / peak
    if dd >= cfg.pp_exit_dd:
        return max(cur_tier, 2)
    if dd >= cfg.pp_trim_dd:
        return max(cur_tier, 1)
    return cur_tier


def replay_pp_tier(hold_hist: pd.Series, cost: float, cfg: RiskConfig,
                   start_tier: int = 0) -> int:
    """自建仓日起逐日回放，重建盈利保护档位（与 replay_tier 同构）。"""
    s = pd.to_numeric(hold_hist, errors="coerce").dropna()
    if s.empty or not cost or not np.isfinite(cost) or cost <= 0:
        return start_tier
    tier, peak = start_tier, float("-inf")
    for c in s.to_numpy(dtype=float):
        peak = max(peak, c)
        tier = pp_step(c, peak, cost, cfg, tier)
    return tier


def profit_protect(hold_hist: pd.Series, cost: float, cfg: RiskConfig,
                   prev_tier: int = 0) -> ExitDecision | None:
    """盈利保护（回撤止盈）：浮盈曾达 pp_trigger 后，价格自持有期峰值回撤
    达 pp_trim_dd → 减半锁盈；达 pp_exit_dd → 清仓止盈。

    补上原体系「浮盈只有 MA20 追踪、可回吐 30%+ 才触发」的缺口——
    高位票在跌回 MA20 之前就先把利润锁住。hold_hist 为自建仓日起(含评估日)
    的收盘序列；prev_tier 为回放至前一日的档位。返回 None 表示不触发。
    """
    if not cfg.profit_protect or prev_tier >= 2:
        return None
    s = pd.to_numeric(hold_hist, errors="coerce").dropna()
    if s.empty or not cost or not np.isfinite(cost) or cost <= 0:
        return None
    close, peak = float(s.iloc[-1]), float(s.max())
    tier = pp_step(close, peak, cost, cfg, prev_tier)
    if tier <= prev_tier:
        return None
    dd = 1 - close / peak
    if tier >= 2:
        return ExitDecision("exit", 0.0,
                            f"盈利保护止盈(自峰值回撤{dd:.0%}≥{cfg.pp_exit_dd:.0%})",
                            new_tier=tier)
    return ExitDecision("trim", cfg.pp_trim_keep,
                        f"盈利保护减半(自峰值回撤{dd:.0%}≥{cfg.pp_trim_dd:.0%})",
                        new_tier=tier)


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
