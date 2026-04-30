"""综合评分器。

职责：
1. 从 `signals.registry` 拿全部 generator
2. 根据各 generator 的 `scope` 分组预加载数据（time_series / cross_section / history）
3. 对每只股票跑所有 generator，汇总出 `SignalSnapshot`
4. 按类别加权求 `composite_score`
5. 返回 DataFrame（一行一股）：tech_score / fund_score / flow_score / sent_score /
   composite / rank_pct / summary + 每只股票的 snapshots 字典（便于落库）

权重读 config.yaml 的 `scoring.category_weights`，缺失则退回到 DEFAULT。
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.config import load_config
from invest_model.logger import get_logger
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.repositories.technical_repo import TechnicalRepository
from invest_model.scoring.attribution import narrative
from invest_model.signals.base import (
    Signal,
    SignalSnapshot,
    score_to_strength,
)
from invest_model.signals.fundamental import prepare_fundamental_context
from invest_model.signals.money_flow import prepare_money_flow_context
from invest_model.signals.registry import (
    ensure_default_generators_loaded,
    get_all_generators,
)
from invest_model.signals.sentiment import prepare_sentiment_context

logger = get_logger()


DEFAULT_CATEGORY_WEIGHTS: dict[str, float] = {
    "technical": 0.45,
    "fundamental": 0.15,
    "money_flow": 0.25,
    "sentiment": 0.15,
}


CATEGORY_COLUMN: dict[str, str] = {
    "technical": "tech_score",
    "fundamental": "fund_score",
    "money_flow": "flow_score",
    "sentiment": "sent_score",
}


class CompositeScorer:
    """跨类别综合评分器。"""

    def __init__(
        self,
        engine: Engine,
        weights: dict[str, float] | None = None,
        ts_lookback_days: int = 250,
    ):
        self.engine = engine
        self.weights = weights or _load_weights_from_config()
        self.ts_lookback_days = ts_lookback_days
        ensure_default_generators_loaded()

    # ── 历史回灌 ──────────────────────────────────────────

    def backfill_history(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        step_days: int = 1,
        skip_existing: bool = True,
        persist: bool = True,
    ) -> int:
        """对 [start_date, end_date] 区间的每个交易日批量打分并落库。

        Parameters
        ----------
        codes : list[str]
            要打分的股票池
        start_date, end_date : str
            区间，YYYYMMDD
        step_days : int
            隔 N 个交易日打一次（用于加速）。1 表示逐日。
        skip_existing : bool
            True 时跳过 stock_composite_score 中已有 (all_codes, trade_date) 的日期
        persist : bool
            False 时只返回计数，不写库（用于干跑）

        Returns
        -------
        int
            总共写入的综合评分条数。
        """
        from invest_model.repositories.calendar_repo import CalendarRepository
        from invest_model.repositories.signal_repo import SignalRepository
        from invest_model.scoring.persistence import (
            save_composite_scores,
            save_signal_snapshots,
        )

        if not codes:
            return 0

        cal = CalendarRepository(self.engine)
        all_dates = cal.get_trade_dates(start_date, end_date)
        if not all_dates:
            logger.warning(f"区间无交易日: {start_date} ~ {end_date}")
            return 0

        target_dates = all_dates[::step_days]

        if skip_existing:
            repo = SignalRepository(self.engine)
            existing = repo.read_sql(
                """
                SELECT trade_date, COUNT(*) AS cnt
                FROM stock_composite_score
                WHERE trade_date BETWEEN :s AND :e
                GROUP BY trade_date
                """,
                {"s": start_date, "e": end_date},
            )
            complete = set(
                existing[existing["cnt"] >= len(codes)]["trade_date"].tolist()
            )
            target_dates = [d for d in target_dates if d not in complete]

        logger.info(
            f"backfill: 区间 {start_date}~{end_date} 共 {len(all_dates)} 个交易日, "
            f"本次实际打分 {len(target_dates)} 天, {len(codes)} 只股票"
        )

        total = 0
        for i, td in enumerate(target_dates, 1):
            try:
                df, snaps = self.score_batch(codes, td)
                if df.empty:
                    continue
                if persist:
                    save_signal_snapshots(self.engine, snaps)
                    save_composite_scores(self.engine, df)
                total += len(df)
            except Exception as e:  # pragma: no cover - defensive
                logger.error(f"backfill {td} 失败: {e}")

            if i % 10 == 0 or i == len(target_dates):
                logger.info(f"backfill 进度: {i}/{len(target_dates)}")

        return total

    # ── 对外主入口 ──────────────────────────────────────────

    def score_batch(
        self,
        codes: list[str],
        trade_date: str,
    ) -> tuple[pd.DataFrame, dict[str, SignalSnapshot]]:
        """对一批股票在某个交易日打分。

        Returns
        -------
        (df, snapshots)
            df: 每行一股，列包含每类别子分、composite、rank_pct、summary
            snapshots: {code -> SignalSnapshot}，便于落库明细
        """
        if not codes:
            return pd.DataFrame(), {}

        generators = get_all_generators()
        if not generators:
            logger.warning("registry 中未找到任何 generator，跳过打分")
            return pd.DataFrame(), {}

        context = self._prepare_context(codes, trade_date, generators)

        rows: list[dict] = []
        snapshots: dict[str, SignalSnapshot] = {}

        for code in codes:
            sigs: list[Signal] = []
            for gen in generators:
                try:
                    produced = gen.generate_for_date(code, trade_date, context)
                except Exception as e:  # pragma: no cover - defensive
                    logger.error(f"{gen.__class__.__name__} code={code} 失败: {e}")
                    produced = []
                sigs.extend(produced)

            cat_scores, composite = self._aggregate(sigs)
            snap = SignalSnapshot(
                code=code,
                trade_date=trade_date,
                signals=sigs,
                composite_score=composite,
                generated_at=datetime.now().isoformat(),
            )
            snapshots[code] = snap

            row = {"code": code, "trade_date": trade_date, "composite": composite}
            for cat, col in CATEGORY_COLUMN.items():
                row[col] = cat_scores.get(cat, 0.0)
            row["summary"] = narrative(snap)
            row["generated_at"] = snap.generated_at
            rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            df["rank_pct"] = df["composite"].rank(pct=True, method="average")
        return df, snapshots

    # ── 数据预加载 ──────────────────────────────────────────

    def _prepare_context(
        self,
        codes: list[str],
        trade_date: str,
        generators: Iterable,
    ) -> dict:
        context: dict = {}
        scopes = {g.scope for g in generators}
        categories = {g.category for g in generators}

        if "time_series" in scopes and "technical" in categories:
            context["time_series"] = self._load_time_series(codes, trade_date)

        if "fundamental" in categories:
            context.update(prepare_fundamental_context(self.engine, codes, trade_date))

        if "money_flow" in categories:
            context.update(prepare_money_flow_context(self.engine, codes, trade_date))

        if "sentiment" in categories:
            context.update(prepare_sentiment_context(self.engine, codes, trade_date))

        return context

    def _load_time_series(self, codes: list[str], trade_date: str) -> dict[str, pd.DataFrame]:
        """为技术信号批量加载 close + 全部技术指标。支持股票和 ETF。"""
        from invest_model.repositories.etf_repo import ETFRepository

        daily_repo = StockDailyRepository(self.engine)
        tech_repo = TechnicalRepository(self.engine)
        etf_repo = ETFRepository(self.engine)

        start = _shift_days(trade_date, -self.ts_lookback_days)

        ts_map: dict[str, pd.DataFrame] = {}
        for code in codes:
            daily = daily_repo.get_daily(code, start, trade_date)
            if daily.empty:
                daily = etf_repo.get_daily(code, start, trade_date)
            if daily.empty:
                continue

            tech = tech_repo.get_technical(code, start, trade_date)
            if not tech.empty:
                merged = daily.merge(
                    tech,
                    on=["code", "trade_date"],
                    how="left",
                    suffixes=("", "_tech"),
                )
            else:
                merged = daily.copy()
                merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
                merged = merged.sort_values("trade_date")
                merged["ma5"] = merged["close"].rolling(5, min_periods=1).mean()
                merged["ma10"] = merged["close"].rolling(10, min_periods=1).mean()
                merged["ma20"] = merged["close"].rolling(20, min_periods=1).mean()
                merged["ma60"] = merged["close"].rolling(60, min_periods=1).mean()
            merged = merged.sort_values("trade_date")
            ts_map[code] = merged
        return ts_map

    # ── 汇总 ──────────────────────────────────────────

    def _aggregate(self, signals: list[Signal]) -> tuple[dict[str, float], float]:
        cat_scores: dict[str, list[float]] = {c: [] for c in DEFAULT_CATEGORY_WEIGHTS.keys()}
        cat_hit: dict[str, list[Signal]] = {c: [] for c in DEFAULT_CATEGORY_WEIGHTS.keys()}

        for sig in signals:
            cat = _category_of(sig)
            if cat in cat_scores:
                cat_scores[cat].append(sig.score)
                cat_hit[cat].append(sig)

        avg_per_cat: dict[str, float] = {}
        for cat, scores in cat_scores.items():
            if cat == "technical" and cat_hit.get(cat):
                avg_per_cat[cat] = _tech_grouped_average(cat_hit[cat])
            elif scores:
                avg_per_cat[cat] = sum(scores) / len(scores)
            else:
                avg_per_cat[cat] = 0.0

        weighted = 0.0
        total_w = 0.0
        for cat, w in self.weights.items():
            if cat_hit.get(cat):
                weighted += avg_per_cat.get(cat, 0.0) * w
                total_w += w
        composite = weighted / total_w if total_w > 0 else 0.0
        composite = max(-1.0, min(1.0, composite))
        return avg_per_cat, composite


# ── helpers ──────────────────────────────────────────

# 技术信号分组权重：趋势组 > 反转/风险组 > 量/波动组
_TECH_GROUPS: dict[str, tuple[list[str], float]] = {
    "trend": (["macd_trend", "momentum_20"], 0.50),
    "reversal": (["rsi_extreme", "boll_position", "ma_bias"], 0.35),
    "volume": (["vol_ratio", "volatility_20"], 0.15),
}


def _tech_grouped_average(signals: list) -> float:
    """对技术子信号按趋势/反转/量价分组加权，而非简单等权平均。"""
    name_to_score: dict[str, float] = {s.name: s.score for s in signals}
    weighted_sum = 0.0
    weight_sum = 0.0
    for _group_name, (members, group_weight) in _TECH_GROUPS.items():
        group_scores = [name_to_score[m] for m in members if m in name_to_score]
        if group_scores:
            group_avg = sum(group_scores) / len(group_scores)
            weighted_sum += group_avg * group_weight
            weight_sum += group_weight
    if weight_sum == 0:
        return 0.0
    return weighted_sum / weight_sum


_SIGNAL_TO_CATEGORY: dict[str, str] = {
    # technical
    "macd_trend": "technical",
    "rsi_extreme": "technical",
    "boll_position": "technical",
    "ma_bias": "technical",
    "vol_ratio": "technical",
    "momentum_20": "technical",
    "volatility_20": "technical",
    # fundamental
    "pe_rank": "fundamental",
    "pb_rank": "fundamental",
    "roe_level": "fundamental",
    "revenue_growth": "fundamental",
    "debt_level": "fundamental",
    # money_flow
    "main_inflow_5d": "money_flow",
    "elg_ratio": "money_flow",
    "margin_delta_5d": "money_flow",
    # sentiment
    "turnover_extreme": "sentiment",
    "holder_count_trend": "sentiment",
    "insider_net_30d": "sentiment",
}


def _category_of(sig: Signal) -> str:
    return _SIGNAL_TO_CATEGORY.get(sig.name, "technical")


def _load_weights_from_config() -> dict[str, float]:
    try:
        cfg = load_config()
    except FileNotFoundError:
        return dict(DEFAULT_CATEGORY_WEIGHTS)
    weights = (cfg.get("scoring") or {}).get("category_weights") or {}
    merged = dict(DEFAULT_CATEGORY_WEIGHTS)
    for k, v in weights.items():
        try:
            merged[k] = float(v)
        except (TypeError, ValueError):
            continue
    return merged


def _shift_days(trade_date: str, days: int) -> str:
    """按自然日 shift trade_date（格式 YYYYMMDD）。用于时序预加载起点，
    不要求严格交易日精度。"""
    from datetime import datetime, timedelta

    try:
        d = datetime.strptime(trade_date, "%Y%m%d")
    except ValueError:
        return trade_date
    return (d + timedelta(days=days)).strftime("%Y%m%d")


__all__ = [
    "CompositeScorer",
    "DEFAULT_CATEGORY_WEIGHTS",
    "CATEGORY_COLUMN",
]

# Re-export so attribution/persistence can use score_to_strength without re-importing.
_ = score_to_strength
