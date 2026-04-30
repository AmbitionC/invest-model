"""分位数 / 截面排名标准化器

将原始因子值归一化到 [0, 1] 区间，消除量纲差异，
使不同因子可在同一尺度下加权比较。
"""

import numpy as np
import pandas as pd


class RankNormalizer:
    """基于排名的截面标准化器"""

    @staticmethod
    def normalize_cross_section(values: pd.Series) -> pd.Series:
        """截面排名归一化：将一组值按排名映射到 [0, 1]。

        NaN 值保持为 NaN。排名采用 "average" 策略处理并列。

        Parameters
        ----------
        values : pd.Series
            原始因子值（如全市场某日 PE）

        Returns
        -------
        pd.Series
            归一化后的值，0 = 最小，1 = 最大
        """
        if values.dropna().empty:
            return pd.Series(np.nan, index=values.index)
        ranks = values.rank(method="average", na_option="keep")
        count = values.notna().sum()
        if count <= 1:
            return pd.Series(0.5, index=values.index).where(values.notna())
        return (ranks - 1) / (count - 1)

    @staticmethod
    def normalize_percentile(value: float, series: pd.Series) -> float:
        """计算单个值在序列中的百分位（0~1）。

        Parameters
        ----------
        value : float
            待计算的值
        series : pd.Series
            参考分布

        Returns
        -------
        float
            0.0 ~ 1.0，表示 value 在 series 中的百分位排名
        """
        if np.isnan(value):
            return np.nan
        clean = series.dropna()
        if clean.empty:
            return 0.5
        return float((clean < value).sum()) / len(clean)

    @staticmethod
    def normalize_inverse(values: pd.Series) -> pd.Series:
        """反向截面归一化：值越小排名越高（适用于 PE/PB 等低优指标）。

        Returns
        -------
        pd.Series
            归一化后的值，0 = 最大（最差），1 = 最小（最优）
        """
        return 1.0 - RankNormalizer.normalize_cross_section(values)


def winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """缩尾处理：将极端值截断到指定分位数范围，减轻离群值影响。"""
    q_low = series.quantile(lower)
    q_high = series.quantile(upper)
    return series.clip(q_low, q_high)
