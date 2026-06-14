"""截面多因子层：原始数据加载 → 因子计算 → 去极值/中性化/标准化 → 落库。"""

from invest_model.factors.library import FACTOR_DIRECTION, FACTORS, compute_factors
from invest_model.factors.loader import FactorDataLoader
from invest_model.factors.pipeline import FactorPipeline
from invest_model.factors.processor import process_factors

__all__ = [
    "FACTORS",
    "FACTOR_DIRECTION",
    "compute_factors",
    "FactorDataLoader",
    "FactorPipeline",
    "process_factors",
]
