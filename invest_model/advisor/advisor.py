"""ML 驱动的逐票信号顾问。

架构：
  特征构造 (ml.FeatureBuilder)
    → 多 horizon 推理 (ml.MLPredictor)
    → 决策映射 (advisor.decision.make_decision)
    → 输出 AdvisorSignal

相比旧的规则版 advisor，本模块：
- 移除：CompositeScorer 加权、ConfidenceEngine 计算、TriggerDetector 过滤、冷却机制
- 移除：分位数校准（CalibrationProfile）
- 保留：止盈覆盖、非对称阈值、安全边际仓位调节（已迁移到 advisor.decision）
- 新增：目标仓位制（target_position 输出，由 delta 决定操作类型）

旧的归因风格通过 SHAP top-k 特征 + 决策 notes 替代。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Optional

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.advisor.decision import (
    ACTION_LABELS,
    DEFAULT_DECISION_CONFIG,
    DEFAULT_HORIZON_WEIGHTS,
    DecisionConfig,
    DecisionResult,
    make_decision,
    target_to_action,
)
from invest_model.logger import get_logger
from invest_model.ml.features import FeatureBuilder, FEATURE_COLUMNS
from invest_model.ml.labels import LABEL_HORIZONS
from invest_model.ml.persistence import load_all_artifacts
from invest_model.ml.predictor import MLPredictor

logger = get_logger()


@dataclass
class AdvisorSignal:
    """逐票 ML 操作建议。

    与旧版字段尽量兼容，便于持久化层无感升级。
    """
    code: str
    trade_date: str
    action: str             # strong_buy / buy / add / hold / reduce / clear
    confidence: int         # 0-100，由 |score| 与历史模型 IC 派生
    position_pct: float     # 兼容字段：本次操作的仓位变化绝对值
    triggers: list[str] = field(default_factory=list)  # SHAP top 特征名
    attribution: str = ""
    sub_scores: dict = field(default_factory=dict)   # ML 预测明细（pred_3d/5d/10d）
    composite: float = 0.0  # 兼容字段：使用 horizon_score 填充
    confidence_detail: str = ""
    name: str = ""

    # ── ML 新增字段 ──
    target_position: float = 0.0
    current_position: float = 0.0
    delta_position: float = 0.0
    horizon_score: float = 0.0
    safety_margin: float = 0.5
    take_profit: bool = False

    @property
    def action_cn(self) -> str:
        return ACTION_LABELS.get(self.action, self.action)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["action_cn"] = self.action_cn
        return d


# ── 主类 ─────────────────────────────────────────────


class StockAdvisor:
    """ML 信号顾问。

    Parameters
    ----------
    engine : Engine
    horizons : tuple[int, ...]
        模型 horizon 列表，需与训练时一致
    ts_lookback_days : int
        加载日线/技术指标的回看窗口（用于止盈/安全边际/特征）
    horizon_weights : dict | None
        多 horizon 综合权重
    feature_builder : FeatureBuilder | None
        允许外部注入（共享上下文/缓存）
    predictor : MLPredictor | None
        已加载好模型的 predictor，避免每次实例化都重新加载磁盘文件
    """

    def __init__(
        self,
        engine: Engine,
        horizons: tuple[int, ...] = LABEL_HORIZONS,
        ts_lookback_days: int = 80,
        horizon_weights: dict[int, float] | None = None,
        feature_builder: Optional[FeatureBuilder] = None,
        predictor: Optional[MLPredictor] = None,
        codes_for_predictor: list[str] | None = None,
        version: str | None = None,
        decision_config: DecisionConfig | None = None,
        ic_weighting: bool = True,
    ):
        self.engine = engine
        self.horizons = tuple(horizons)
        self.ts_lookback_days = ts_lookback_days
        self.horizon_weights = horizon_weights or DEFAULT_HORIZON_WEIGHTS
        self.feature_builder = feature_builder or FeatureBuilder(
            engine, ts_lookback_days=ts_lookback_days
        )
        self.predictor = predictor
        self._predictor_codes = codes_for_predictor or []
        self._predictor_version = version
        self.decision_config = decision_config or DEFAULT_DECISION_CONFIG
        self.ic_weighting = ic_weighting

    # ── 推理 ──────────────────────────────────────

    def _ensure_predictor(self, codes: list[str]) -> MLPredictor:
        """惰性加载 predictor。如果 codes 扩大，重新加载。"""
        need_load = (
            self.predictor is None
            or any(c not in self.predictor.artifacts for c in codes)
        )
        if need_load:
            logger.info(
                f"加载 ML 模型: {len(codes)} 只标的 × {len(self.horizons)} horizons "
                f"version={self._predictor_version or 'latest'} ic_weighting={self.ic_weighting}"
            )
            artifacts = load_all_artifacts(
                self.engine, codes, list(self.horizons), version=self._predictor_version
            )
            self.predictor = MLPredictor(
                artifacts=artifacts,
                horizon_weights=self.horizon_weights,
                ic_weighting=self.ic_weighting,
            )
        return self.predictor

    # ── 当前持仓查询（默认从 backtest_trades 反推或外部注入） ──

    def get_current_positions(
        self, codes: list[str], trade_date: str
    ) -> dict[str, float]:
        """默认实现：当前持仓全为 0（推理场景由调用方注入实际持仓）。

        回测引擎会重写此方法或直接传 current_position dict。
        """
        return {c: 0.0 for c in codes}

    # ── public API ──────────────────────────────

    def advise_batch(
        self,
        codes: list[str],
        trade_date: str,
        code_name_map: dict[str, str] | None = None,
        current_positions: dict[str, float] | None = None,
        last_action_dates: dict[str, dict[str, str | None]] | None = None,
        trade_dates: list[str] | None = None,
        regime_multiplier: float = 1.0,
    ) -> list[AdvisorSignal]:
        """对一批标的在某交易日生成 ML 操作建议。

        Parameters
        ----------
        last_action_dates : dict[code, {"open": str|None, "clear": str|None}] | None
            每只票最近一次开仓 / 清仓的交易日，用于冷却判定。
            缺失或 None 时退化为不启用冷却（与旧行为兼容）。
        trade_dates : list[str] | None
            交易日历升序列表，用于精确计算冷却天数。
        regime_multiplier : float
            市场状态仓位乘数（由 MarketRegimeDetector 提供）。
            < 1.0 时压缩目标仓位（如 TECH_DOMINANT/BEAR）；
            > 1.0 时放大（如 VALUE_ROTATION/BROAD_BULL），但最终仍受 max_single_position 约束。
            = 0.0 时屏蔽所有新开仓（熔断场景）。
        """
        if not codes:
            return []
        code_name_map = code_name_map or {}
        current_positions = current_positions or self.get_current_positions(codes, trade_date)
        last_action_dates = last_action_dates or {}

        predictor = self._ensure_predictor(codes)

        results: list[AdvisorSignal] = []
        for code in codes:
            la = last_action_dates.get(code) or {}
            sig = self._advise_single(
                code, trade_date,
                name=code_name_map.get(code, ""),
                current_position=current_positions.get(code, 0.0),
                predictor=predictor,
                last_open_date=la.get("open"),
                last_clear_date=la.get("clear"),
                trade_dates=trade_dates,
            )
            # 应用市场状态仓位乘数
            if regime_multiplier != 1.0:
                sig = self._apply_regime(sig, regime_multiplier)
            results.append(sig)

        results.sort(key=lambda s: abs(s.horizon_score), reverse=True)
        return results

    def _apply_regime(self, sig: "AdvisorSignal", multiplier: float) -> "AdvisorSignal":
        """对单票信号应用市场状态乘数，调整目标仓位并重新映射操作。"""
        max_pos = self.decision_config.max_single_position
        new_target = float(min(sig.target_position * multiplier, max_pos))
        new_target = round(max(new_target, 0.0), 4)
        new_delta = round(new_target - sig.current_position, 4)
        new_action, _ = target_to_action(
            new_target, sig.current_position,
            take_profit=sig.take_profit,
            cfg=self.decision_config,
        )
        # 仓位被压缩时在归因中注明
        if abs(new_target - sig.target_position) > 0.005:
            regime_note = (
                f"[Regime×{multiplier:.2f}] target: {sig.target_position:.0%}→{new_target:.0%}"
            )
            sig.attribution = f"{regime_note} | {sig.attribution}"
        sig.action = new_action
        sig.target_position = new_target
        sig.delta_position = new_delta
        sig.position_pct = round(abs(new_delta), 4)
        return sig

    def advise_single(
        self, code: str, trade_date: str, name: str = "",
        current_position: float = 0.0,
        last_open_date: str | None = None,
        last_clear_date: str | None = None,
        trade_dates: list[str] | None = None,
    ) -> AdvisorSignal:
        predictor = self._ensure_predictor([code])
        return self._advise_single(
            code, trade_date, name=name,
            current_position=current_position,
            predictor=predictor,
            last_open_date=last_open_date,
            last_clear_date=last_clear_date,
            trade_dates=trade_dates,
        )

    # ── 内部 ──────────────────────────────────────

    def _advise_single(
        self,
        code: str,
        trade_date: str,
        name: str,
        current_position: float,
        predictor: MLPredictor,
        last_open_date: str | None = None,
        last_clear_date: str | None = None,
        trade_dates: list[str] | None = None,
    ) -> AdvisorSignal:
        """逐票推理 + 决策。"""
        feature_vec = self.feature_builder.build_single(code, trade_date)
        if feature_vec is None:
            return self._empty_signal(code, trade_date, name, reason="无特征数据")

        pred = predictor.predict(code, feature_vec, trade_date=trade_date)
        if pred is None:
            return self._empty_signal(code, trade_date, name, reason="无可用模型")

        df_ts = self._load_recent_ts(code, trade_date)
        # 优先使用 predictor 的 IC 加权（按 cv_avg_ic 自动归一化），
        # 与 advisor.horizon_weights 行为分离：advisor 仅给静态 fallback。
        eff_w = predictor.effective_weights_for(code) if hasattr(predictor, "effective_weights_for") else self.horizon_weights
        decision = make_decision(
            predictions=pred.predictions,
            current_position=current_position,
            df_ts=df_ts,
            horizon_weights=eff_w or self.horizon_weights,
            cfg=self.decision_config,
            trade_date=trade_date,
            last_open_date=last_open_date,
            last_clear_date=last_clear_date,
            trade_dates=trade_dates,
        )

        return self._build_signal(
            code=code,
            trade_date=trade_date,
            name=name,
            pred=pred,
            decision=decision,
        )

    def _build_signal(
        self,
        code: str,
        trade_date: str,
        name: str,
        pred,
        decision: DecisionResult,
    ) -> AdvisorSignal:
        # confidence = 综合 |score| × 历史 IC 强度（粗糙映射 0-100）
        score_strength = min(abs(decision.horizon_score) * 50, 1.0)
        confidence = int(round(score_strength * 100))

        # 触发名 / 归因
        triggers = [name for name, _ in pred.shap_top]
        shap_desc = ", ".join(
            f"{n}={v:+.4f}" for n, v in pred.shap_top
        )

        pred_desc = " / ".join(
            f"h{h}={v:+.4f}" for h, v in sorted(pred.predictions.items())
        )
        attribution = (
            f"score={decision.horizon_score:+.4f} "
            f"target={decision.target_position:.0%} curr={decision.current_position:.0%} "
            f"safety={decision.safety_margin:.2f} | "
            f"preds: {pred_desc} | top: {shap_desc}"
        )
        if decision.take_profit_triggered:
            attribution += f" | 止盈: {decision.take_profit_reason}"
        if decision.notes:
            attribution += " | " + "; ".join(decision.notes[:2])

        sub_scores = {f"pred_{h}d": float(v) for h, v in pred.predictions.items()}

        return AdvisorSignal(
            code=code,
            trade_date=trade_date,
            action=decision.action,
            confidence=confidence,
            position_pct=round(abs(decision.delta_position), 4),
            triggers=triggers,
            attribution=attribution,
            sub_scores=sub_scores,
            composite=round(decision.horizon_score, 5),
            confidence_detail=shap_desc,
            name=name,
            target_position=decision.target_position,
            current_position=decision.current_position,
            delta_position=decision.delta_position,
            horizon_score=decision.horizon_score,
            safety_margin=decision.safety_margin,
            take_profit=decision.take_profit_triggered,
        )

    def _empty_signal(
        self, code: str, trade_date: str, name: str, reason: str
    ) -> AdvisorSignal:
        return AdvisorSignal(
            code=code,
            trade_date=trade_date,
            action="hold",
            confidence=0,
            position_pct=0.0,
            triggers=[],
            attribution=reason,
            sub_scores={},
            composite=0.0,
            confidence_detail=reason,
            name=name,
        )

    def _load_recent_ts(self, code: str, trade_date: str) -> pd.DataFrame:
        """加载止盈/安全边际所需的近 N 日合并表。

        优先复用 ``feature_builder._preload_cache``（由 BacktestEngine.warmup 填充）；
        缓存未命中时回退到 DB 查询。
        """
        cached = self.feature_builder.get_cached_preload(code) if self.feature_builder else None
        if cached is not None:
            ts = cached.get("ts")
            if ts is not None and not ts.empty:
                ts_until = ts[ts["trade_date"] <= trade_date]
                if not ts_until.empty:
                    return ts_until.sort_values("trade_date").tail(self.ts_lookback_days)

        from datetime import datetime, timedelta
        from invest_model.repositories.stock_daily_repo import StockDailyRepository
        from invest_model.repositories.technical_repo import TechnicalRepository
        from invest_model.repositories.etf_repo import ETFRepository

        try:
            end_dt = datetime.strptime(trade_date, "%Y%m%d")
        except ValueError:
            return pd.DataFrame()
        start = (end_dt - timedelta(days=self.ts_lookback_days * 2)).strftime("%Y%m%d")

        daily_repo = StockDailyRepository(self.engine)
        tech_repo = TechnicalRepository(self.engine)
        etf_repo = ETFRepository(self.engine)

        daily = daily_repo.get_daily(code, start, trade_date)
        if daily.empty:
            daily = etf_repo.get_daily(code, start, trade_date)
        if daily.empty:
            return pd.DataFrame()

        tech = tech_repo.get_technical(code, start, trade_date)
        if not tech.empty:
            merged = daily.merge(
                tech, on=["code", "trade_date"], how="left", suffixes=("", "_tech")
            )
        else:
            merged = self._compute_basic_ma(daily)
        return merged.sort_values("trade_date").tail(self.ts_lookback_days)

    @staticmethod
    def _compute_basic_ma(df: pd.DataFrame) -> pd.DataFrame:
        """对 ETF 等无 stock_technical 的标的，基于日线现场算技术指标。

        复用 ``FeatureBuilder._compute_basic_ma``：除 MA 外还会算 RSI / MACD /
        ma60_bias / volatility_20 / momentum_20 / vol_ratio，保证 advisor 的止盈
        判断、ML 特征构造拿到同一份"完整衍生指标"，避免 ETF 出现"日线侧只算 MA、
        ML 侧只看 trigger + trend"的特征贫瘠问题。
        """
        return FeatureBuilder._compute_basic_ma(df)

    # ── 兼容旧 API：保留 daily_report 简洁版 ──────────

    def daily_report(
        self,
        codes: list[str],
        trade_date: str,
        code_name_map: dict[str, str] | None = None,
        current_positions: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        sigs = self.advise_batch(codes, trade_date, code_name_map, current_positions)
        if not sigs:
            return pd.DataFrame()
        rows = []
        for s in sigs:
            rows.append({
                "代码": s.code,
                "名称": s.name,
                "操作": s.action_cn,
                "目标仓位": f"{s.target_position:.0%}",
                "当前仓位": f"{s.current_position:.0%}",
                "调仓": f"{s.delta_position:+.0%}" if s.delta_position else "0%",
                "score": f"{s.horizon_score:+.4f}",
                "安全边际": f"{s.safety_margin:.2f}",
                "pred_3d": f"{s.sub_scores.get('pred_3d', 0):+.4f}",
                "pred_5d": f"{s.sub_scores.get('pred_5d', 0):+.4f}",
                "pred_10d": f"{s.sub_scores.get('pred_10d', 0):+.4f}",
                "归因": s.attribution,
            })
        return pd.DataFrame(rows)
