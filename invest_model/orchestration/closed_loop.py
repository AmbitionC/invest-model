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
import os
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
from invest_model.portfolio import (
    MarketTiming,
    PortfolioConfig,
    RiskConfig,
    build_targets,
    fuse_targets,
)
from invest_model.repositories.advisor_repo import AdvisorRepo
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.portfolio_repo import (
    ModelRegistryRepository,
    PortfolioRepository,
)
from invest_model.repositories.prediction_repo import PredictionRepository
from invest_model.repositories.universe_repo import UniverseRepository
from invest_model.signals.trend import trend_ok
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
    model_kind: str = "ic"           # ic（多因子合成，默认） | ranker（ML 截面排序）
    timing_enabled: bool = True
    timing_floor: float = 0.5
    risk: RiskConfig = field(default_factory=lambda: RiskConfig(enabled=False))
    arb: "ArbConfig" = field(default_factory=lambda: __import__(
        "invest_model.arb.config", fromlist=["ArbConfig"]).ArbConfig.from_env())


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
        self.adv_repo = AdvisorRepo(engine)
        self._reb: list[str] = []
        self._ind_map: dict[str, str] = {}
        self._ranker = None

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

    def ranker(self):
        if self._ranker is None:
            from invest_model.model.ranker import CSRanker
            self._ranker = CSRanker(self.engine, self.reb(), version=self.cfg.version,
                                    min_train_periods=6)
        return self._ranker

    def _predict_one(self, dt: str) -> pd.DataFrame:
        """ranker 模式优先用 ML 排序；历史不足时回退 IC 合成（同 version 落库）。"""
        if self.cfg.model_kind == "ranker":
            p = self.ranker().predict(dt, persist=True)
            if not p.empty:
                return p
        return self.pred.predict(dt, persist=True)

    def predict_all(self) -> int:
        n = sum(1 for d in self.reb() if not self._predict_one(d).empty)
        logger.info(f"预测完成：{n}/{len(self.reb())} 个调仓日 "
                    f"(version={self.cfg.version}, model={self.cfg.model_kind})")
        return n

    def _theme_industries(self, dt: str) -> set[str]:
        """投顾看多主题 → 行业名集合（子串匹配）。theme_boost==1.0 时跳过。"""
        if self.cfg.portfolio.theme_boost == 1.0:
            return set()
        themes = self.adv_repo.get_active_theme(dt)
        if themes.empty:
            return set()
        names = set(themes.loc[themes["direction"] == "long", "theme"].astype(str))
        inds = set(self.industry_map().values())
        return {ind for ind in inds for t in names if t and (t in ind or ind in t)}

    def _vol20_map(self, dt: str, codes: list[str]) -> dict[str, float]:
        """近 20 交易日日收益波动（pct_chg 标准差，小数）。scheme=inv_vol 加权用。"""
        if not codes:
            return {}
        start = (pd.to_datetime(dt) - pd.Timedelta(days=45)).strftime("%Y%m%d")
        out: dict[str, float] = {}
        for i in range(0, len(codes), 800):
            batch = codes[i:i + 800]
            ph = ",".join(f":c{j}" for j in range(len(batch)))
            params = {f"c{j}": c for j, c in enumerate(batch)}
            params.update(s=start, d=dt)
            df = self.repo.read_sql(
                f"SELECT code, trade_date, pct_chg FROM stock_daily "
                f"WHERE trade_date>=:s AND trade_date<=:d AND code IN ({ph})", params)
            if df.empty:
                continue
            df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
            for c, g in df.sort_values("trade_date").groupby("code"):
                v = g["pct_chg"].dropna().tail(20)
                if len(v) >= 10:
                    out[str(c)] = float(v.std()) / 100.0
        return out

    def _build_targets(self, dt: str, p: pd.DataFrame, gross: float,
                       cur_codes: set[str] | None = None) -> tuple[dict, dict]:
        """构建 dt 目标组合，返回 (weights, meta)。投顾为主或纯量化由 portfolio.advisor_led 决定。

        重构为独立方法，供回测 target_provider 与实盘 action_plan 共用。
        cur_codes：现持仓代码（hold_buffer 换手抑制用；回测传当前权重键、实盘传真实持仓）。
        """
        pcfg = self.cfg.portfolio
        scores = p[["code", "score", "rank_pct"]] if not p.empty else p
        # 趋势闸只需对「可能买入」的票判定 → 限定到 top 候选，避免扫全 universe（性能关键）。
        topk = pcfg.top_n * 4

        def _top_codes(df: pd.DataFrame) -> list[str]:
            if df.empty:
                return []
            return list(df.sort_values("score", ascending=False).head(topk)["code"])

        vol_map = None
        if pcfg.scheme == "inv_vol" and not p.empty:
            vol_map = self._vol20_map(dt, _top_codes(scores) + sorted(cur_codes or set()))

        if pcfg.advisor_led:
            advisor_df = self.adv_repo.get_active_reco(dt)
            exit_codes = self.adv_repo.get_exit_codes(dt)
            trend_codes = None
            if self.cfg.risk.trend_filter:
                cand = set(_top_codes(scores))
                if not advisor_df.empty:
                    cand |= set(advisor_df["code"])
                trend_codes = trend_ok(self.engine, dt, list(cand), self.cfg.risk)
            # 三水表倾斜（影子默认关）：把高 flow_score 行业并入投顾主题倾斜通道，
            # 复用 theme_boost 乘子 —— ARB_WATERTILT=0 时 flow_industries() 返回空集。
            theme_inds = self._theme_industries(dt)
            if getattr(self.cfg, "arb", None) and self.cfg.arb.watermeter_tilt:
                try:
                    from invest_model.arb.watermeter import flow_industries
                    theme_inds = set(theme_inds) | flow_industries(self.engine, dt, self.cfg.arb)
                except Exception:  # noqa: BLE001
                    pass
            return fuse_targets(scores, pcfg, advisor_df, gross=gross,
                                trend_ok_codes=trend_codes, exit_codes=exit_codes,
                                theme_industries=theme_inds,
                                industry_map=self.industry_map(),
                                current_codes=cur_codes, vol_map=vol_map)
        # 纯量化
        if self.cfg.risk.trend_filter and not p.empty:
            ok = trend_ok(self.engine, dt, _top_codes(scores), self.cfg.risk)
            scores = scores[scores["code"].isin(ok)]
        targets = build_targets(scores, pcfg, gross=gross, industry_map=self.industry_map(),
                                current_codes=cur_codes, vol_map=vol_map)
        meta = {c: {"grade": None, "source": "quant"} for c in targets}
        return targets, meta

    def _target_provider(self, dt: str, cur: dict[str, float]) -> dict[str, float]:
        p = self.pred_repo.get_predictions(dt, self.cfg.version)
        if p.empty:
            p = self._predict_one(dt)
        u = set(self.uni_repo.get_universe(dt, self.cfg.universe.method))
        if u and not p.empty:
            p = p[p["code"].isin(u)]
        if p.empty and not self.cfg.portfolio.advisor_led:
            return {}
        gross = self.mt.gross_exposure(dt, list(u) if u else None)
        targets, meta = self._build_targets(dt, p, gross, cur_codes=set(cur or {}))
        # 落 portfolio_target（带 grade/source）
        if targets:
            rows = [{"trade_date": dt, "version": self.cfg.version, "code": c,
                     "weight": w, "rank": i + 1, "gross_exposure": gross,
                     "grade": meta.get(c, {}).get("grade"),
                     "source": meta.get(c, {}).get("source")}
                    for i, (c, w) in enumerate(sorted(targets.items(), key=lambda x: -x[1]))]
            self.pf_repo.save_targets(pd.DataFrame(rows))
        return targets

    def backtest(self) -> dict:
        cfg = CSBacktestConfig(name=f"cs_{self.cfg.version}", start_date=self.cfg.start,
                               end_date=self.cfg.end, benchmark_code=self.cfg.benchmark,
                               risk=self.cfg.risk)
        exit_provider = self.adv_repo.get_exit_codes if self.cfg.risk.enabled else None
        res = CSBacktestEngine(self.engine, cfg, self._target_provider, self.reb(),
                               exit_codes_provider=exit_provider).run()
        run_id = self._persist_backtest(res)
        self._export(res, run_id)
        logger.info(f"回测完成 run_id={run_id}：{json.dumps(res.metrics, ensure_ascii=False)}")
        return res.metrics

    def backtest_arb(self, arb_cfg=None) -> dict:
        """统一资金账本回测：offense(引擎B) + defense_A(carry) + alpha 合成一条净值。

        各 sleeve 与合成结果按 version 独立落库（版本隔离）；生产 cs_ic_v1 路径不动。
        数据缺失的 sleeve 退化为平坦净值（现金），零杠杆不变式无条件成立。
        """
        from invest_model.arb.carry import build_carry_signals
        from invest_model.arb.config import (ArbConfig, VERSION_ALPHA, VERSION_CB,
                                             VERSION_DIV, VERSION_LEDGER, VERSION_REPO)
        from invest_model.arb.watermeter import build_flow_scores
        from invest_model.backtest.carry_engine import CarryBacktestEngine
        from invest_model.backtest.ledger_backtest import compose_ledger, persist_arb_result
        from invest_model.repositories.arb_repo import AlphaRepo, CarryRepo, LedgerRepo

        acfg = arb_cfg or ArbConfig.from_env()
        base = CSBacktestConfig(start_date=self.cfg.start, end_date=self.cfg.end,
                                benchmark_code=self.cfg.benchmark)

        # 1) 引擎 B（进攻）= 现有多因子系统净值
        off_cfg = CSBacktestConfig(name=f"arb_offense_{self.cfg.version}",
                                   start_date=self.cfg.start, end_date=self.cfg.end,
                                   benchmark_code=self.cfg.benchmark)
        offense = CSBacktestEngine(self.engine, off_cfg, self._target_provider,
                                   self.reb()).run()

        # 2) 防守 A = 逆回购 + 可转债（现金 carry），blend 为一条 defense 净值
        rr = CarryBacktestEngine(self.engine, CSBacktestConfig(
            name="arb_repo", start_date=self.cfg.start, end_date=self.cfg.end),
            mode="reverse_repo", arb_cfg=acfg).run()
        cb = CarryBacktestEngine(self.engine, CSBacktestConfig(
            name="arb_cb", start_date=self.cfg.start, end_date=self.cfg.end),
            mode="convertible", arb_cfg=acfg).run()
        defense = self._blend_navs([rr.nav_df, cb.nav_df])

        # 3) 盲区 α（有候选才建；否则平坦=现金）
        alpha_nav = self._alpha_nav(base)

        # 落各 sleeve 明细净值（版本隔离）
        persist_arb_result(self.engine, rr, VERSION_REPO)
        persist_arb_result(self.engine, cb, VERSION_CB)
        persist_arb_result(self.engine, offense, f"arb_offense_{self.cfg.version}",
                           top_k=self.cfg.portfolio.top_n)

        # 4) 合成账本净值
        ledger = compose_ledger(
            {"defense_A": defense, "offense_B": offense.nav_df, "alpha": alpha_nav},
            acfg, self.cfg.start, self.cfg.end)
        run_id = persist_arb_result(self.engine, ledger, VERSION_LEDGER,
                                    top_k=self.cfg.portfolio.top_n)

        # 5) 账本 sleeve_target（回测口径，写各 sleeve 期末净值）
        try:
            from invest_model.arb.ledger import allocate_sleeves
            w = allocate_sleeves(acfg, fear_score=None)
            bounds = acfg.sleeve_bounds()
            nav_end = {
                "defense_A": float(defense["nav"].iloc[-1]) if not defense.empty else 1.0,
                "offense_B": float(offense.nav_df["nav"].iloc[-1]) if not offense.nav_df.empty else 1.0,
                "alpha": float(alpha_nav["nav"].iloc[-1]) if not alpha_nav.empty else 1.0,
            }
            rows = []
            for s in ("defense_A", "offense_B", "alpha", "cash"):
                lo, hi = bounds.get(s, (0.0, 1.0))
                rows.append({"plan_date": self.cfg.end, "sleeve": s,
                             "version": VERSION_LEDGER, "target_pct": w.get(s, 0.0),
                             "actual_pct": w.get(s, 0.0), "min_pct": lo, "max_pct": hi,
                             "nav": nav_end.get(s), "note": "backtest"})
            LedgerRepo(self.engine).save(pd.DataFrame(rows))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"sleeve_target 落库失败（不阻断）：{e}")

        logger.info(f"套利账本回测完成 run_id={run_id}："
                    f"{json.dumps(ledger.metrics, ensure_ascii=False)}")
        return {"ledger": ledger.metrics, "offense": offense.metrics,
                "reverse_repo": rr.metrics, "convertible": cb.metrics}

    def _blend_navs(self, nav_dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """把多条 sleeve 净值等权 blend 为一条（对齐并集日期，缺失日 ret=0）。"""
        rets, all_dates = [], set()
        for nav in nav_dfs:
            if nav is None or nav.empty:
                continue
            s = nav.set_index("trade_date")["ret"].astype(float)
            # 仅纳入非全零（有真实数据）的 sleeve；全平坦=无数据则不参与
            if s.abs().sum() > 0:
                rets.append(s)
                all_dates |= set(s.index)
        dates = sorted(all_dates)
        if not dates:
            return pd.DataFrame({"trade_date": [self.cfg.end], "nav": [1.0], "ret": [0.0],
                                 "turnover": [0.0], "position_count": [0], "invested": [0.0]})
        blended, prev = [], 1.0
        rvals = []
        for d in dates:
            vals = [s.loc[d] for s in rets if d in s.index]
            r = float(sum(vals) / len(vals)) if vals else 0.0
            prev *= (1.0 + r)
            blended.append(prev); rvals.append(r)
        return pd.DataFrame({"trade_date": dates, "nav": blended, "ret": rvals,
                             "turnover": 0.0, "position_count": len(rets), "invested": 1.0})

    def _alpha_nav(self, base: CSBacktestConfig) -> pd.DataFrame:
        """盲区 α 篮子净值：等权持有当期 α 候选（cs_engine 目标提供者）。无候选=平坦。"""
        from invest_model.arb.config import VERSION_ALPHA
        from invest_model.repositories.arb_repo import AlphaRepo
        repo = BaseRepository(self.engine)
        if not repo.table_exists("alpha_candidate"):
            return pd.DataFrame()
        arepo = AlphaRepo(self.engine)

        def provider(dt: str, cur: dict[str, float]) -> dict[str, float]:
            df = arepo.get_active(dt, VERSION_ALPHA)
            if df is None or df.empty:
                return {}
            df = df[df.get("falsified", -1) != 1] if "falsified" in df.columns else df
            codes = list(df["code"])[:20]
            if not codes:
                return {}
            w = min(0.05, 0.15 / len(codes))
            return {c: w for c in codes}

        acfg = CSBacktestConfig(name="arb_alpha", start_date=self.cfg.start,
                                end_date=self.cfg.end, benchmark_code=self.cfg.benchmark)
        res = CSBacktestEngine(self.engine, acfg, provider, self.reb()).run()
        # 全空则视为平坦
        if res.nav_df.empty or res.nav_df["ret"].abs().sum() == 0:
            return pd.DataFrame()
        return res.nav_df

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
        if mode == "arb":
            # 套利账本回测：先自动补三水表 + flow/carry 派生信号，再合成账本净值。
            from invest_model.arb.carry import build_carry_signals
            from invest_model.arb.watermeter import build_flow_scores
            from invest_model.arb.watermeter_auto import build_watermeter_auto
            try:
                build_watermeter_auto(self.engine, self.cfg.end)
                build_flow_scores(self.engine, self.cfg.end)
                build_carry_signals(self.engine, self.cfg.end)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"arb 派生信号构建失败（不阻断回测）：{e}")
            return self.backtest_arb()
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
        from sqlalchemy import text
        with self.repo.engine.begin() as conn:
            cursor = conn.execute(text(
                "INSERT INTO backtest_run (name,strategy,start_date,end_date,rebalance_days,top_k,params,metrics) "
                "VALUES (:name,:strategy,:start_date,:end_date,:rebalance_days,:top_k,:params,:metrics)"
            ), run_row)
            # lastrowid 取本次插入的自增 ID；SELECT MAX() 在并发写入下有竞态
            run_id = int(cursor.lastrowid)
        nav = res.nav_df.copy()
        nav.insert(0, "run_id", run_id)
        self.repo.bulk_insert("backtest_nav", nav[["run_id", "trade_date", "nav", "ret", "turnover", "position_count"]])
        if res.trades:
            tr = pd.DataFrame(res.trades)
            tr.insert(0, "run_id", run_id)
            self.repo.bulk_insert("backtest_trades", tr[["run_id", "trade_date", "code", "action", "weight", "price"]])
        return run_id

    def _export(self, res, run_id: int) -> None:
        # INVEST_RESULTS_DIR：FaaS 等代码目录只读的环境把结果指到 /tmp；
        # 目录不可写时跳过导出（指标已入库 backtest_run，文件仅是本地便览）。
        out_dir = Path(os.getenv("INVEST_RESULTS_DIR") or get_project_root() / "results")
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.warning(f"results 目录 {out_dir} 不可写，跳过 latest.json 导出：{e}")
            return
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
        for w in health.get("warnings", []):
            logger.warning(f"⚠️ 回测可信度告警：{w}")
