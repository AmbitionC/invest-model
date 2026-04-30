"""股票信号顾问。

逐票独立计算置信度，输出多档操作建议。
不做组合层面的换仓，每只股票独立出手建议。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Optional

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.advisor.confidence import ConfidenceEngine, ConfidenceResult
from invest_model.advisor.triggers import TriggerDetector, TriggerResult
from invest_model.logger import get_logger
from invest_model.scoring.attribution import narrative
from invest_model.scoring.scorer import CompositeScorer, CATEGORY_COLUMN

logger = get_logger()

# 操作档位
ACTION_LABELS = {
    "strong_buy": "强买",
    "buy": "买入",
    "hold": "观望",
    "reduce": "减仓",
    "clear": "清仓",
}


@dataclass
class AdvisorSignal:
    code: str
    trade_date: str
    action: str             # strong_buy / buy / hold / reduce / clear
    confidence: int         # 0-100
    position_pct: float     # 建议本次操作仓位比例 (0.0~1.0)
    triggers: list[str]     # 触发的关键位置规则名
    attribution: str        # 归因摘要
    sub_scores: dict        # tech_score / fund_score / flow_score / sent_score
    composite: float        # 综合分 [-1, 1]
    confidence_detail: str  # 置信度计算明细
    name: str = ""

    @property
    def action_cn(self) -> str:
        return ACTION_LABELS.get(self.action, self.action)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["action_cn"] = self.action_cn
        return d


def _map_action_and_position(
    confidence: int, direction: str, threshold: int = 60
) -> tuple[str, float]:
    """根据置信度和方向映射操作档位和建议仓位比例。threshold 由校准决定。"""
    strong_threshold = threshold + 20
    if direction == "bullish":
        if confidence >= strong_threshold:
            return "strong_buy", 0.40
        if confidence >= threshold:
            return "buy", 0.20
        return "hold", 0.0
    elif direction == "bearish":
        if confidence >= strong_threshold:
            return "clear", 1.0
        if confidence >= threshold:
            return "reduce", 0.40
        return "hold", 0.0
    else:
        return "hold", 0.0


class StockAdvisor:
    """逐票独立信号顾问。"""

    def __init__(
        self,
        engine: Engine,
        trigger_detector: TriggerDetector | None = None,
        confidence_engine: ConfidenceEngine | None = None,
        profiles: dict | None = None,
        ts_lookback_days: int = 60,
    ):
        self.engine = engine
        self.scorer = CompositeScorer(engine)
        self.trigger = trigger_detector or TriggerDetector()
        self.confidence = confidence_engine or ConfidenceEngine()
        self.profiles = profiles
        self.ts_lookback_days = ts_lookback_days

        if self.profiles is None:
            self._load_profiles()

    def _load_profiles(self):
        """从数据库加载已有校准 profile（缓存）。"""
        from invest_model.advisor.persistence import load_calibration_profiles
        try:
            self.profiles = load_calibration_profiles(self.engine)
        except Exception:
            self.profiles = {}

    # ── public API ──

    def advise_batch(
        self,
        codes: list[str],
        trade_date: str,
        code_name_map: dict[str, str] | None = None,
    ) -> list[AdvisorSignal]:
        """对一批股票在某个交易日生成操作建议。"""
        if not codes:
            return []
        code_name_map = code_name_map or {}

        score_df, snapshots = self.scorer.score_batch(codes, trade_date)
        if score_df.empty:
            logger.warning(f"advise_batch: score_batch 返回空, date={trade_date}")
            return []

        ts_data = self._load_daily_tech(codes, trade_date)

        results: list[AdvisorSignal] = []
        for _, row in score_df.iterrows():
            code = row["code"]
            composite = float(row.get("composite", 0))
            sub = {
                "tech_score": float(row.get("tech_score", 0)),
                "fund_score": float(row.get("fund_score", 0)),
                "flow_score": float(row.get("flow_score", 0)),
                "sent_score": float(row.get("sent_score", 0)),
            }

            df_ts = ts_data.get(code, pd.DataFrame())
            trigger_results = self.trigger.detect_all(df_ts)

            profile = self.profiles.get(code) if self.profiles else None
            conf_result = self.confidence.compute(composite, sub, trigger_results, profile)
            threshold = profile.action_threshold if profile else 60
            action, position_pct = _map_action_and_position(
                conf_result.confidence, conf_result.direction, threshold
            )

            snap = snapshots.get(code)
            attrib = narrative(snap) if snap else ""
            triggered_names = [t.name for t in trigger_results if t.triggered]
            trigger_descs = [t.description for t in trigger_results if t.triggered]
            full_attribution = (
                f"{attrib} | 置信度{conf_result.confidence}分: {conf_result.explanation}"
            )
            if trigger_descs:
                full_attribution += " | 触发: " + "; ".join(trigger_descs)

            results.append(AdvisorSignal(
                code=code,
                trade_date=trade_date,
                action=action,
                confidence=conf_result.confidence,
                position_pct=round(position_pct, 2),
                triggers=triggered_names,
                attribution=full_attribution,
                sub_scores=sub,
                composite=round(composite, 5),
                confidence_detail=conf_result.explanation,
                name=code_name_map.get(code, ""),
            ))

        results.sort(key=lambda s: s.confidence, reverse=True)
        return results

    def advise_single(
        self, code: str, trade_date: str, name: str = ""
    ) -> AdvisorSignal:
        sigs = self.advise_batch([code], trade_date, {code: name})
        return sigs[0] if sigs else AdvisorSignal(
            code=code, trade_date=trade_date, action="hold",
            confidence=0, position_pct=0.0, triggers=[], attribution="无数据",
            sub_scores={}, composite=0.0, confidence_detail="", name=name,
        )

    def daily_report(
        self,
        codes: list[str],
        trade_date: str,
        code_name_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """生成一张操作建议 DataFrame。"""
        signals = self.advise_batch(codes, trade_date, code_name_map)
        if not signals:
            return pd.DataFrame()
        rows = []
        for s in signals:
            rows.append({
                "代码": s.code,
                "名称": s.name,
                "操作": s.action_cn,
                "置信度": s.confidence,
                "仓位%": f"{s.position_pct:.0%}" if s.position_pct else "0%",
                "综合分": f"{s.composite:+.3f}",
                "技术": f"{s.sub_scores.get('tech_score', 0):+.3f}",
                "基本面": f"{s.sub_scores.get('fund_score', 0):+.3f}",
                "资金流": f"{s.sub_scores.get('flow_score', 0):+.3f}",
                "情绪": f"{s.sub_scores.get('sent_score', 0):+.3f}",
                "触发规则": ", ".join(s.triggers) if s.triggers else "无",
                "归因": s.attribution,
            })
        return pd.DataFrame(rows)

    # ── data loading ──

    def _load_daily_tech(
        self, codes: list[str], trade_date: str
    ) -> dict[str, pd.DataFrame]:
        """为触发检测加载近 N 日的 daily+technical 合并数据。支持股票和 ETF。"""
        from invest_model.repositories.stock_daily_repo import StockDailyRepository
        from invest_model.repositories.technical_repo import TechnicalRepository
        from invest_model.repositories.etf_repo import ETFRepository
        from datetime import datetime, timedelta

        try:
            end_dt = datetime.strptime(trade_date, "%Y%m%d")
        except ValueError:
            return {}
        start = (end_dt - timedelta(days=self.ts_lookback_days * 2)).strftime("%Y%m%d")

        daily_repo = StockDailyRepository(self.engine)
        tech_repo = TechnicalRepository(self.engine)
        etf_repo = ETFRepository(self.engine)

        result: dict[str, pd.DataFrame] = {}
        for code in codes:
            daily = daily_repo.get_daily(code, start, trade_date)
            if daily.empty:
                daily = etf_repo.get_daily(code, start, trade_date)
            if daily.empty:
                continue

            tech = tech_repo.get_technical(code, start, trade_date)
            if not tech.empty:
                merged = daily.merge(
                    tech, on=["code", "trade_date"], how="left", suffixes=("", "_tech")
                )
            else:
                merged = _compute_basic_ma(daily)
            result[code] = merged.sort_values("trade_date").tail(self.ts_lookback_days)
        return result


def _compute_basic_ma(df: pd.DataFrame) -> pd.DataFrame:
    """对没有 stock_technical 数据的标的（如 ETF），基于日线自行计算 MA。"""
    df = df.copy()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", pd.Series(dtype=float)), errors="coerce")
    df = df.sort_values("trade_date")
    df["ma5"] = df["close"].rolling(5, min_periods=1).mean()
    df["ma10"] = df["close"].rolling(10, min_periods=1).mean()
    df["ma20"] = df["close"].rolling(20, min_periods=1).mean()
    df["ma60"] = df["close"].rolling(60, min_periods=1).mean()
    return df
