"""FactorPipeline：对某交易日 universe 计算处理后因子暴露并落 factor_exposure。"""

from __future__ import annotations

import pandas as pd

from invest_model.factors.library import CANDIDATE_FACTORS, FACTORS, compute_factors
from invest_model.factors.loader import FactorDataLoader
from invest_model.factors.processor import process_factors
from invest_model.logger import get_logger
from invest_model.repositories.factor_repo import FactorRepository

logger = get_logger()


class FactorPipeline:
    def __init__(self, engine):
        self.engine = engine
        self.loader = FactorDataLoader(engine)
        self.repo = FactorRepository(engine)

    def compute_date(self, trade_date: str, codes: list[str],
                     persist: bool = True, neutralize: bool = True) -> pd.DataFrame:
        """返回 index=code、列=FACTORS 的处理后暴露宽表。"""
        raw = self.loader.load_cross_section(trade_date, codes)
        if raw.empty:
            return pd.DataFrame()
        factors = compute_factors(raw)
        processed = process_factors(factors, neutralize=neutralize)
        if persist and not processed.empty:
            # 候选因子暴露一并落库（供 IC 影子观察）；打分层只读 FACTORS
            cols = [f for f in FACTORS + CANDIDATE_FACTORS if f in processed.columns]
            self.repo.save_exposures_wide(trade_date, processed, cols)
        return processed

    def compute_dates(self, date_codes: dict[str, list[str]],
                      neutralize: bool = True) -> int:
        """批量计算多个调仓日。date_codes: {trade_date: [codes]}。返回处理日数。"""
        n = 0
        for d in sorted(date_codes):
            res = self.compute_date(d, date_codes[d], persist=True, neutralize=neutralize)
            if not res.empty:
                n += 1
        logger.info(f"因子流水线完成：{n} 个调仓日")
        return n
