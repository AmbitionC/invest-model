"""顶部特征即时判定（P16 候选）——供计划层"顶部特征预警"提示行使用。

判据与 `scripts/analysis` 回测预登记常量**逐字一致**（跑数前写死、勿按结果回调）：
浮盈曾达标（≥15%）后，20 日已实现波动升到近 250 日 ≥80 分位（波动骤放大）**且**
5/60 日量比 ≥1.5（放量）→ 视为顶部特征。

治理定位：**只提示、不自动减仓**——个股大样本（6901 周期）控回撤证据极强，但差预登记 E12
第②条（跑赢买入持有 55%<60%），未过"自动交易"判据；故仅作计划层影子提示，人工决策。
若将来要自动减仓，须重新预登记 E12（判据改回撤/捕获口径）并过关，见 docs/model_change_proposals.md P16。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

TOP_VOL_PCTL = 0.80          # 20 日已实现波动的近 250 日分位阈值
TOP_VOL_WIN = 20
TOP_VOL_LOOKBACK = 250
TOP_VOLUME_RATIO = 1.5       # 5 日均量 / 60 日均量
TOP_MIN_PROFIT = 0.15        # 入场以来峰值浮盈须曾达此值
PEAK_FALLBACK_WIN = 120      # 无入场日时的峰值回看窗（交易日）


def top_feature_now(close: pd.Series, volume: pd.Series, cost: float,
                    entry_date: str | None = None) -> bool:
    """判断"今日"（序列末端）是否呈顶部特征。

    close/volume：截至今日的（前复权）收盘与成交量序列，index=trade_date（升序、字符串）。
    cost：持仓成本；entry_date：入场日（用于界定"入场以来峰值"），缺失则回看 PEAK_FALLBACK_WIN 日。
    """
    c = pd.to_numeric(close, errors="coerce").dropna()
    if len(c) < 80 or cost is None or cost <= 0:
        return False
    # 峰值浮盈达标（入场以来 / 回看窗）
    if entry_date:
        since = c[c.index >= str(entry_date)]
        peak = float(since.max()) if not since.empty else float(c.iloc[-PEAK_FALLBACK_WIN:].max())
    else:
        peak = float(c.iloc[-PEAK_FALLBACK_WIN:].max())
    if peak / cost - 1 < TOP_MIN_PROFIT:
        return False
    # 波动骤放大：20 日已实现波动在近 250 日的分位 ≥ 阈值
    ret = c.pct_change()
    vol20 = ret.rolling(TOP_VOL_WIN).std() * np.sqrt(250)
    cur = vol20.iloc[-1]
    look = vol20.iloc[-TOP_VOL_LOOKBACK:].dropna()
    if len(look) < 60 or not np.isfinite(cur):
        return False
    if float((look <= cur).mean()) < TOP_VOL_PCTL:
        return False
    # 放量：5/60 日量比 ≥ 阈值（无量数据时此条豁免，与回测口径一致）
    v = pd.to_numeric(volume, errors="coerce")
    if v.notna().sum() > 60:
        v5 = v.rolling(5).mean().iloc[-1]
        v60 = v.rolling(60).mean().iloc[-1]
        ratio = v5 / v60 if (np.isfinite(v5) and np.isfinite(v60) and v60 > 0) else np.nan
        if not (np.isfinite(ratio) and ratio >= TOP_VOLUME_RATIO):
            return False
    return True
