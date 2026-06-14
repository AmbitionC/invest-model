"""ClosedLoop：把截面多因子系统串成自主闭环。

阶段（mode）：
  update    增量更新数据（生产/Tushare 路径；合成或离线环境跳过）
  universe  逐调仓日构建投资域
  factors   逐调仓日计算并落因子暴露
  train     计算因子 IC + 注册模型版本
  predict   逐调仓日生成截面打分
  backtest  滚动回测 + 落库 + 导出
  all       依次执行 universe→factors→train→predict→backtest
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

from invest_model.backtest.cs_engine import CSBacktestConfig, CSBacktestEngine
from invest_model.config import get_project_root
from invest_model.factors import FACTORS, FactorPipeline
from invest_model.logger import get_logger
from invest_model.model import CSPredictor
from invest_model.model.dataset import compute_factor_ic, rebalance_dates
from invest_model.portfolio import MarketTiming, PortfolioConfig, build_targets
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.portfolio_repo import (
    ModelRegistryRepository,
    PortfolioRepository,
)
from invest_model.repositories.prediction_repo import PredictionRepository
from invest_model.repositories.universe_repo import UniverseRepository
from invest_model.universe import UniverseBuilder, UniverseConfig

logger = get_logger()


@dataclass
class LoopConfig:
    start: str = "20210101"
    end: str = ""
    version: str = "ic_v1"
    rebalance: str = "monthly"
    benchmark: str = "000300.SH"
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    ic_window: int = 12
    ic_mode: str = "icir"            # icir | ic
    timing_enabled: bool = True
    timing_floor: float = 0.5


class ClosedLoop:
    def __init__(self, engine, config: LoopConfig | None = None):
        self.engine = engine
        self.cfg = config or LoopConfig()
        if not self.cfg.end:
            self.cfg.end = datetime.now().strftime("%Y%m%d")
        self.repo = BaseRepository(engine)
        self.ub = UniverseBuilder(engine, self.cfg.universe)
        self.fp = FactorPipeline(engine)
        self.pred = CSPredictor(engine, version=self.cfg.version,
                                window=self.cfg.ic_window, mode=self.cfg.ic_mode)
        self.mt = MarketTiming(engine, benchmark=self.cfg.benchmark,
                               floor=self.cfg.timing_floor, enabled=self.cfg.timing_enabled)
        self.uni_repo = UniverseRepository(engine)
        self.pred_repo = PredictionRepository(engine)
        self.pf_repo = PortfolioRepository(engine)
        self.reg_repo = ModelRegistryRepository(engine)
        self._reb: list[str] = []
        self._ind_map: dict[str, str] = {}

    # ── 调仓日历 / 行业映射 ──
    def reb(self) -> list[str]:
        if not self._reb:
            self._reb = rebalance_dates(self.engine, self.cfg.start, self.cfg.end, self.cfg.rebalance)
        return self._reb

    def industry_map(self) -> dict[str, str]:
        if not self._ind_map:
            df = self.repo.read_sql("SELECT ts_code AS code, industry FROM stock_info")
            self._ind_map = dict(zip(df["code"], df["industry"]))
        return self._ind_map

    # ── 阶段 ──
    def build_universe(self) -> dict[str, list[str]]:
        out = {d: self.ub.build(d, persist=True) for d in self.reb()}
        logger.info(f"universe 阶段完成：{len(out)} 个调仓日")
        return out

    def build_factors(self) -> int:
        dc = {d: self.uni_repo.get_universe(d, self.cfg.universe.method) for d in self.reb()}
        dc = {d: c for d, c in dc.items() if c}
        return self.fp.compute_dates(dc)

    def train(self) -> pd.DataFrame:
        ic = compute_factor_ic(self.engine, self.reb(), persist=True)
        if not ic.empty:
            ic["rank_ic"] = pd.to_numeric(ic["rank_ic"], errors="coerce")
            mean_ic = ic.groupby("trade_date")["rank_ic"].mean()
            self.reg_repo.register({
                "version": self.cfg.version, "model_type": "ic_combiner",
                "train_start": self.cfg.start, "train_end": self.cfg.end,
                "n_samples": int(len(ic)), "n_factors": len(FACTORS),
                "factor_cols": FACTORS,
                "cv_ic_mean": float(mean_ic.mean()),
                "cv_ic_ir": float(mean_ic.mean() / mean_ic.std()) if mean_ic.std() else None,
                "cv_hit_rate": float((mean_ic > 0).mean()),
                "model_path": "",
            })
        return ic

    def predict_all(self) -> int:
        return self.pred.predict_dates(self.reb())

    def _target_provider(self, dt: str, _cur: dict[str, float]) -> dict[str, float]:
        p = self.pred_repo.get_predictions(dt, self.cfg.version)
        if p.empty:
            p = self.pred.predict(dt, persist=True)
        if p.empty:
            return {}
        u = set(self.uni_repo.get_universe(dt, self.cfg.universe.method))
        if u:
            p = p[p["code"].isin(u)]
        gross = self.mt.gross_exposure(dt, list(u) if u else None)
        targets = build_targets(p[["code", "score", "rank_pct"]], self.cfg.portfolio,
                                gross=gross, industry_map=self.industry_map())
        # 落 portfolio_target
        if targets:
            rows = [{"trade_date": dt, "version": self.cfg.version, "code": c,
                     "weight": w, "rank": i + 1, "gross_exposure": gross}
                    for i, (c, w) in enumerate(sorted(targets.items(), key=lambda x: -x[1]))]
            self.pf_repo.save_targets(pd.DataFrame(rows))
        return targets

    def backtest(self) -> dict:
        cfg = CSBacktestConfig(name=f"cs_{self.cfg.version}", start_date=self.cfg.start,
                               end_date=self.cfg.end, benchmark_code=self.cfg.benchmark)
        res = CSBacktestEngine(self.engine, cfg, self._target_provider, self.reb()).run()
        run_id = self._persist_backtest(res)
        self._export(res, run_id)
        logger.info(f"回测完成 run_id={run_id}：{json.dumps(res.metrics, ensure_ascii=False)}")
        return res.metrics

    def run(self, mode: str = "all") -> dict:
        if mode in ("universe", "all"):
            self.build_universe()
        if mode in ("factors", "all"):
            self.build_factors()
        if mode in ("train", "all"):
            self.train()
        if mode in ("predict", "all"):
            self.predict_all()
        if mode in ("backtest", "all"):
            return self.backtest()
        return {}

    # ── 持久化 / 导出 ──
    def _persist_backtest(self, res) -> int:
        run_row = {
            "name": res.config.name, "strategy": res.config.strategy,
            "start_date": res.config.start_date, "end_date": res.config.end_date,
            "rebalance_days": 0, "top_k": self.cfg.portfolio.top_n,
            "params": json.dumps({"version": self.cfg.version, "rebalance": self.cfg.rebalance},
                                 ensure_ascii=False),
            "metrics": json.dumps(res.metrics, ensure_ascii=False),
        }
        self.repo.execute_sql(
            "INSERT INTO backtest_run (name,strategy,start_date,end_date,rebalance_days,top_k,params,metrics) "
            "VALUES (:name,:strategy,:start_date,:end_date,:rebalance_days,:top_k,:params,:metrics)",
            run_row,
        )
        run_id = int(self.repo.read_sql("SELECT MAX(run_id) AS m FROM backtest_run")["m"].iloc[0])
        nav = res.nav_df.copy()
        nav.insert(0, "run_id", run_id)
        self.repo.bulk_insert("backtest_nav", nav[["run_id", "trade_date", "nav", "ret", "turnover", "position_count"]])
        if res.trades:
            tr = pd.DataFrame(res.trades)
            tr.insert(0, "run_id", run_id)
            self.repo.bulk_insert("backtest_trades", tr[["run_id", "trade_date", "code", "action", "weight", "price"]])
        return run_id

    def _export(self, res, run_id: int) -> None:
        out_dir = get_project_root() / "results"
        out_dir.mkdir(exist_ok=True)
        from invest_model.orchestration.health import compute_health

        last_reb = self.reb()[-1] if self.reb() else None
        targets = self.pf_repo.get_targets(last_reb, self.cfg.version) if last_reb else pd.DataFrame()
        health = compute_health(self.engine, self.cfg.version, self.cfg.universe.method)
        payload = {
            "generated_at": datetime.now().isoformat(),
            "version": self.cfg.version,
            "run_id": run_id,
            "metrics": res.metrics,
            "health": health,
            "rebalance": self.cfg.rebalance,
            "latest_rebalance_date": last_reb,
            "latest_portfolio": targets.to_dict("records") if not targets.empty else [],
        }
        (out_dir / "latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        res.nav_df.to_json(out_dir / "backtest_nav_latest.json", orient="records", force_ascii=False)
        logger.info(f"已导出 results/latest.json（最新组合 {len(payload['latest_portfolio'])} 只）")
