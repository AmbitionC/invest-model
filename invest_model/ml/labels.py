"""多 horizon 前瞻收益率标签构造。

目标：对 close 序列计算 t 日相对未来 t+h 日的对数收益，作为 XGBoost 的回归目标。
horizon 默认 3/5/10 个交易日。

注意：
1. 训练集尾部 max(horizons) 天没有完整 label，需丢弃
2. 使用对数收益 log(close[t+h]/close[t])，对极端值更稳健
3. 输入 DataFrame 需按 trade_date 升序排列，含 close 列
"""

from __future__ import annotations

import numpy as np
import pandas as pd

LABEL_HORIZONS: tuple[int, ...] = (3, 5, 10)


def make_forward_returns(
    df: pd.DataFrame,
    horizons: tuple[int, ...] = LABEL_HORIZONS,
    close_col: str = "close",
    date_col: str = "trade_date",
) -> pd.DataFrame:
    """对单票日线序列生成多 horizon 前瞻对数收益率。

    Parameters
    ----------
    df : pd.DataFrame
        必须包含 close_col 与 date_col。建议升序排列，但函数内部会重排。
    horizons : tuple[int, ...]
        前瞻交易日数列表，例如 (3, 5, 10)
    close_col : str
        收盘价列名，默认 "close"
    date_col : str
        交易日列名，默认 "trade_date"

    Returns
    -------
    pd.DataFrame
        含 [date_col, "fwd_ret_{h}d", ...] 的 DataFrame，索引重置。
        尾部 max(horizons) 行有 NaN，调用方应在合并特征后统一 dropna。
    """
    if df is None or df.empty or close_col not in df.columns:
        return pd.DataFrame(columns=[date_col] + [f"fwd_ret_{h}d" for h in horizons])

    out = df[[date_col, close_col]].copy()
    out = out.sort_values(date_col).reset_index(drop=True)
    closes = pd.to_numeric(out[close_col], errors="coerce")

    for h in horizons:
        future = closes.shift(-h)
        # 对数收益，避免极端跌价导致的负值带宽
        with np.errstate(divide="ignore", invalid="ignore"):
            out[f"fwd_ret_{h}d"] = np.where(
                (closes > 0) & (future > 0),
                np.log(future / closes),
                np.nan,
            )

    return out.drop(columns=[close_col])


def label_columns(horizons: tuple[int, ...] = LABEL_HORIZONS) -> list[str]:
    """返回标签列名列表。"""
    return [f"fwd_ret_{h}d" for h in horizons]
