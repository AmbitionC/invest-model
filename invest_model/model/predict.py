"""CSPredictor：用 ICCombiner 在某调仓日生成截面打分并落 model_prediction。"""

from __future__ import annotations

import pandas as pd

from invest_model.logger import get_logger
from invest_model.model.combiner import ICCombiner
from invest_model.repositories.factor_repo import FactorRepository
from invest_model.repositories.prediction_repo import PredictionRepository

logger = get_logger()


class CSPredictor:
    def __init__(self, engine, version: str = "ic_v1", window: int = 12,
                 mode: str = "icir"):
        self.engine = engine
        self.version = version
        self.combiner = ICCombiner(engine, window=window, mode=mode)
        self.frepo = FactorRepository(engine)
        self.prepo = PredictionRepository(engine)

    def predict(self, trade_date: str, persist: bool = True) -> pd.DataFrame:
        expo = self.frepo.get_exposures_wide(trade_date)
        if expo.empty:
            return pd.DataFrame()
        w = self.combiner.weights(trade_date)
        score = self.combiner.score(expo, w)
        out = pd.DataFrame({"code": score.index, "score": score.values})
        out["rank_pct"] = out["score"].rank(pct=True)
        out["trade_date"] = trade_date
        out["version"] = self.version
        out = out.sort_values("score", ascending=False).reset_index(drop=True)
        if persist and not out.empty:
            self.prepo.save_predictions(
                out[["trade_date", "version", "code", "score", "rank_pct"]]
            )
        return out

    def predict_dates(self, reb_dates: list[str]) -> int:
        n = 0
        for d in reb_dates:
            if not self.predict(d).empty:
                n += 1
        logger.info(f"预测完成：{n}/{len(reb_dates)} 个调仓日 (version={self.version})")
        return n
