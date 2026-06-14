"""
每日增量更新管线

支持两个时段：
  - morning  (早盘后 11:35)：行情快照 + 实时数据
  - afternoon(午盘后 15:05)：全量增量 + 财务/事件 + 校验

使用方式：
    python3 -m invest_model.pipeline.daily_pipeline              # 完整管线
    python3 -m invest_model.pipeline.daily_pipeline morning      # 早盘管线
    python3 -m invest_model.pipeline.daily_pipeline afternoon    # 午盘管线

cron 配置：
    35 11 * * 1-5  cd /home/admin/Code/invest-journey/invest-model && python3 -m invest_model.pipeline.daily_pipeline morning   >> logs/cron.log 2>&1
     5 15 * * 1-5  cd /home/admin/Code/invest-journey/invest-model && python3 -m invest_model.pipeline.daily_pipeline afternoon >> logs/cron.log 2>&1
"""

import sys
import time
from datetime import datetime

from invest_model.config import load_config
from invest_model.db import get_engine
from invest_model.logger import get_logger
from invest_model.sources.tushare_client import TushareClient
from invest_model.collectors.calendar_collector import CalendarCollector
from invest_model.collectors.stock_list_collector import StockListCollector
from invest_model.collectors.stock_daily_collector import StockDailyCollector
from invest_model.collectors.etf_collector import ETFCollector
from invest_model.collectors.fundamental_collector import FundamentalCollector
from invest_model.collectors.event_collector import EventCollector
from invest_model.collectors.market_collector import MarketCollector
from invest_model.collectors.technical_collector import TechnicalCollector
from invest_model.collectors.macro_collector import MacroCollector
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.validators.data_validator import DataValidator

logger = get_logger()

# ── 两个时段各自执行的步骤 ──
# morning:   早盘收盘后，拉行情快照（日线/指数此时为半日数据，午盘会覆盖更新）
# afternoon: 午盘收盘后，拉完整日数据 + 财务/事件/融资融券 + 校验
MORNING_STEPS = [
    "calendar",
    "stock_daily",
    "etf_daily",
    "index_daily",
]

AFTERNOON_STEPS = [
    "calendar",
    "stock_list",
    "stock_daily",
    "etf_daily",
    "index_daily",
    "daily_basic",
    "technical",
    "margin",
    "cashflow",
    "northbound",
    "holder_trade",
    "holder_count",
    "macro",               # 宏观数据（M1/M2/PMI/CPI/PPI/LPR/北向Top10）
    "data_freshness",      # 数据新鲜度检查（在信号生成前最后把关）
    "signal_generation",
    "model_health_check",
    "validation",
]


class DailyPipeline:
    """每日增量更新管线"""

    def __init__(self):
        self.engine = get_engine()
        self.source = TushareClient()
        self.pool_repo = StockPoolRepository(self.engine)
        self._base_repo = BaseRepository(self.engine)

        self._step_registry = {
            "calendar":          ("交易日历",     self._sync_calendar),
            "stock_list":        ("股票列表",     self._sync_stock_list),
            "stock_daily":       ("个股日线",     self._sync_stock_daily),
            "etf_daily":         ("ETF日线",      self._sync_etf_daily),
            "index_daily":       ("指数日线",     self._sync_index_daily),
            "daily_basic":       ("每日估值",     self._sync_daily_basic),
            "technical":         ("技术指标",     self._sync_technical),
            "margin":            ("融资融券",     self._sync_margin),
            "cashflow":          ("资金流向",     self._sync_cashflow),
            "northbound":        ("北向资金",     self._sync_northbound),
            "holder_trade":      ("股东增减持",   self._sync_holder_trade),
            "holder_count":      ("股东户数",     self._sync_holder_count),
            "macro":             ("宏观数据",     self._sync_macro),
            "data_freshness":    ("数据新鲜度",   self._check_data_freshness),
            "signal_generation": ("综合评分",     self._run_signal_generation),
            "model_health_check":("模型健康检查", self._check_model_health),
            "validation":        ("数据校验",     self._run_validation),
        }

    def run(self, session: str = "full") -> dict:
        """
        运行管线。

        Args:
            session: "morning" / "afternoon" / "full"（全量）
        """
        if session == "morning":
            steps = MORNING_STEPS
        elif session == "afternoon":
            steps = AFTERNOON_STEPS
        else:
            steps = list(self._step_registry.keys())

        start_time = time.time()
        results = {}

        logger.info("=" * 60)
        logger.info(f"每日管线启动 [{session}]: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"执行步骤: {steps}")
        logger.info("=" * 60)

        for key in steps:
            if key not in self._step_registry:
                logger.warning(f"未知步骤: {key}, 跳过")
                continue
            label, func = self._step_registry[key]
            results[key] = self._run_step(label, func)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"每日管线完成 [{session}]，耗时 {elapsed:.1f}s")
        for name, status in results.items():
            logger.info(f"  {name}: {status}")
        logger.info("=" * 60)

        return results

    def _run_step(self, name: str, func) -> str:
        """执行单个步骤，捕获异常"""
        try:
            logger.info(f"--- [{name}] 开始 ---")
            result = func()
            logger.info(f"--- [{name}] 完成: {result} ---")
            return f"OK ({result})"
        except Exception as e:
            logger.error(f"--- [{name}] 失败: {e} ---")
            return f"FAILED ({e})"

    # ── 各步骤实现 ──

    def _sync_calendar(self):
        c = CalendarCollector(self.source, self.engine)
        return c.collect()

    def _sync_stock_list(self):
        c = StockListCollector(self.source, self.engine)
        stocks = c.collect_stock_list()
        etfs = c.collect_etf_list()
        return f"stocks={len(stocks)}, etfs={len(etfs)}"

    def _get_all_tracked_stock_codes(self) -> list[str]:
        """获取需要维护日线数据的完整股票列表：pool 中的 + stock_daily 中已有数据的。"""
        pool_codes = set(self.pool_repo.get_pool_codes("core"))
        existing = self._base_repo.read_sql(
            "SELECT DISTINCT code FROM stock_daily"
        )
        all_codes = pool_codes | set(existing["code"].tolist())
        return sorted(all_codes)

    def _sync_stock_daily(self):
        codes = self._get_all_tracked_stock_codes()
        if not codes:
            return "无个股标的"
        c = StockDailyCollector(self.source, self.engine)
        results = c.collect_incremental(codes)
        total = sum(v for v in results.values() if v > 0)
        return f"{len(codes)} 只, 新增 {total} 条"

    def _sync_etf_daily(self):
        codes = self.pool_repo.get_pool_codes("etf")
        if not codes:
            return "无ETF标的"
        c = ETFCollector(self.source, self.engine)
        results = c.collect_daily_incremental(codes)
        total = sum(v for v in results.values() if v > 0)
        return f"{len(codes)} 只, 新增 {total} 条"

    def _sync_index_daily(self):
        c = MarketCollector(self.source, self.engine)
        results = c.collect_index_incremental()
        total = sum(v for v in results.values() if v > 0)
        return f"{len(results)} 指数, 新增 {total} 条"

    def _sync_daily_basic(self):
        c = FundamentalCollector(self.source, self.engine)
        return c.collect_daily_basic_incremental()

    def _sync_technical(self):
        codes = self._get_all_tracked_stock_codes()
        if not codes:
            return "无个股标的"
        c = TechnicalCollector(engine=self.engine)
        results = c.collect_incremental(codes)
        total = sum(v for v in results.values() if v > 0)
        return f"{len(codes)} 只, 新增 {total} 条"

    def _sync_margin(self):
        c = MarketCollector(self.source, self.engine)
        return c.collect_margin_incremental()

    def _sync_cashflow(self):
        codes = self.pool_repo.get_pool_codes("core")
        if not codes:
            return "无标的"
        c = FundamentalCollector(self.source, self.engine)
        total = c.collect_cashflow_incremental(codes)
        return f"{len(codes)} 只, 新增 {total} 条"

    def _sync_northbound(self):
        c = FundamentalCollector(self.source, self.engine)
        return c.collect_northbound_incremental()

    def _sync_holder_trade(self):
        codes = self.pool_repo.get_pool_codes("core")
        if not codes:
            return "无标的"
        c = EventCollector(self.source, self.engine)
        return c.collect_holder_trade(codes)

    def _sync_holder_count(self):
        codes = self.pool_repo.get_pool_codes("core")
        if not codes:
            return "无标的"
        c = EventCollector(self.source, self.engine)
        return c.collect_holder_count(codes)

    def _sync_macro(self):
        c = MacroCollector(self.source, self.engine)
        results = c.collect_all_incremental()
        total = sum(results.values())
        return f"新增 {total} 条: {results}"

    def _check_model_health(self):
        """检查 ML 模型 OOS IC，低于阈值发出警告，并持久化健康日志。

        - 每日将检查结果写入 model_health_log 表
        - 若某标的连续 >= 10 天 degraded，升级为 error 级别告警并注明"强烈建议重训"
        """
        from invest_model.scoring.ic_monitor import ICMonitor
        from invest_model.repositories.stock_daily_repo import StockDailyRepository

        codes = self.pool_repo.get_pool_codes("core")
        if not codes:
            return "无标的"

        VERSION = "v1_oos"
        RETRAIN_THRESHOLD = 10   # 连续 N 天 degraded → 强警告

        monitor = ICMonitor(self.engine)
        report = monitor.check_health(
            codes=codes,
            version=VERSION,
            ic_warn_threshold=0.03,
        )

        # 取今日检查日期（用最新交易日）
        daily_repo = StockDailyRepository(self.engine)
        latest_dates = [d for d in (daily_repo.get_latest_date(code=c) for c in codes) if d]
        check_date = max(latest_dates) if latest_dates else datetime.now().strftime("%Y%m%d")

        # 持久化健康日志
        monitor.log_health_check(report, version=VERSION, check_date=check_date)

        degraded = [c for c, s in report.items() if s == "degraded"]
        missing  = [c for c, s in report.items() if s == "missing"]

        for code in degraded:
            consecutive = monitor.get_consecutive_degraded_days(code, version=VERSION)
            if consecutive >= RETRAIN_THRESHOLD:
                logger.error(
                    f"[IC监控] {code} 连续 {consecutive} 天质量下降 (IC < 0.03)，强烈建议重训"
                )
            else:
                logger.warning(
                    f"[IC监控] {code} 模型质量下降 (IC < 0.03)，已持续 {consecutive} 天，建议重训"
                )
        for code in missing:
            logger.warning(f"[IC监控] {code} 无已训练模型（version={VERSION}），建议训练")

        return (
            f"ok={len(codes) - len(degraded) - len(missing)}, "
            f"degraded={len(degraded)}, missing={len(missing)}, "
            f"check_date={check_date}"
        )

    def _check_data_freshness(self) -> str:
        """在信号生成前检查核心数据表的新鲜度，数据过旧时发出警告。

        检查项：
        - stock_daily   最新日期 ≤ 今日前 2 个交易日 → WARNING
        - stock_cashflow 最新日期 ≤ 今日前 5 个交易日 → WARNING（资金流数据允许略有延迟）
        - index_daily   3 个核心指数（沪深300/创业板/中证500）有数据 → ERROR 若缺
        """
        from invest_model.repositories.calendar_repo import CalendarRepository

        today = datetime.now().strftime("%Y%m%d")
        cal_repo = CalendarRepository(self.engine)
        recent_trade_dates = cal_repo.get_trade_dates(
            (datetime.now().replace(day=1)).strftime("%Y%m01"), today
        )
        # 取最近 5 个交易日备用
        recent_5 = recent_trade_dates[-5:] if len(recent_trade_dates) >= 5 else recent_trade_dates

        issues: list[str] = []

        # 1. stock_daily 新鲜度
        try:
            df = self._base_repo.read_sql(
                "SELECT MAX(trade_date) AS d FROM stock_daily"
            )
            latest_daily = str(df["d"].iloc[0]) if not df.empty and df["d"].iloc[0] else None
            if latest_daily and recent_5:
                threshold = recent_5[-2] if len(recent_5) >= 2 else recent_5[-1]
                if latest_daily < threshold:
                    msg = f"stock_daily 最新日期={latest_daily}，落后于 {threshold}"
                    logger.warning(f"[数据新鲜度] {msg}")
                    issues.append(f"daily_stale({latest_daily})")
        except Exception as e:
            logger.debug(f"[数据新鲜度] stock_daily 查询失败: {e}")

        # 2. stock_cashflow 新鲜度（允许最多落后 5 个交易日）
        try:
            df = self._base_repo.read_sql(
                "SELECT MAX(trade_date) AS d FROM stock_cashflow"
            )
            latest_cf = str(df["d"].iloc[0]) if not df.empty and df["d"].iloc[0] else None
            if latest_cf and recent_5:
                threshold = recent_5[0]  # 5 个交易日前
                if latest_cf < threshold:
                    msg = f"stock_cashflow 最新日期={latest_cf}，落后超过5个交易日"
                    logger.warning(f"[数据新鲜度] {msg}")
                    issues.append(f"cashflow_stale({latest_cf})")
        except Exception as e:
            logger.debug(f"[数据新鲜度] stock_cashflow 查询失败: {e}")

        # 3. 关键指数数据检查（MarketRegimeDetector 依赖）
        REQUIRED_INDICES = ["000300.SH", "399006.SZ", "000905.SH"]
        try:
            for idx_code in REQUIRED_INDICES:
                df = self._base_repo.read_sql(
                    "SELECT MAX(trade_date) AS d FROM index_daily WHERE code = :c",
                    {"c": idx_code},
                )
                latest_idx = str(df["d"].iloc[0]) if not df.empty and df["d"].iloc[0] else None
                if not latest_idx:
                    logger.error(f"[数据新鲜度] 指数 {idx_code} 无数据，市场状态检测将失效")
                    issues.append(f"index_missing({idx_code})")
                elif recent_5 and latest_idx < recent_5[-2]:
                    logger.warning(f"[数据新鲜度] 指数 {idx_code} 数据过旧={latest_idx}")
                    issues.append(f"index_stale({idx_code})")
        except Exception as e:
            logger.debug(f"[数据新鲜度] index_daily 查询失败: {e}")

        if not issues:
            return f"OK，所有核心数据新鲜"
        return f"警告: {', '.join(issues)}"

    def _run_validation(self):
        codes = self.pool_repo.get_pool_codes("core")
        if not codes:
            return "无标的"
        v = DataValidator(self.engine)
        report = v.validate_stock_daily(codes)
        return f"checks={report.total_checks}, passed={report.passed}, warnings={report.warnings}"

    def _run_signal_generation(self):
        """生成综合评分 + 逐票操作建议并落库。"""
        from invest_model.repositories.stock_daily_repo import StockDailyRepository
        from invest_model.scoring import (
            CompositeScorer,
            save_composite_scores,
            save_signal_snapshots,
        )
        from invest_model.scoring.market_regime import MarketRegimeDetector
        from invest_model.advisor import StockAdvisor
        from invest_model.advisor.persistence import save_advisor_signals

        codes = self.pool_repo.get_pool_codes("core")
        if not codes:
            return "无标的"

        daily_repo = StockDailyRepository(self.engine)
        latest_dates = [
            d for d in (daily_repo.get_latest_date(code=c) for c in codes) if d
        ]
        if not latest_dates:
            return "无日线数据"
        trade_date = max(latest_dates)

        # 0) 组合回撤熔断检查（超过 -15% 时屏蔽新开仓）
        circuit_break = self._check_portfolio_drawdown(threshold=-0.15)
        if circuit_break:
            logger.warning("[熔断] 组合近期回撤超 -15%，本次信号生成将屏蔽买入操作")

        # 1) 综合评分（保留原有落库逻辑）
        scorer = CompositeScorer(self.engine)
        df, snapshots = scorer.score_batch(codes, trade_date)
        n_sigs = 0
        n_comp = 0
        if not df.empty:
            n_sigs = save_signal_snapshots(self.engine, snapshots)
            n_comp = save_composite_scores(self.engine, df)

        # 2) 市场状态检测，获取仓位乘数
        try:
            regime_detector = MarketRegimeDetector(self.engine)
            regime_info = regime_detector.detect(trade_date)
            regime = regime_info["regime"]
            multiplier = regime_info["multiplier"]
        except Exception as e:
            logger.warning(f"[MarketRegime] 检测失败，使用默认乘数 1.0: {e}")
            regime = "NEUTRAL"
            multiplier = 1.0

        # 熔断时进一步压低乘数（屏蔽开仓）
        if circuit_break:
            multiplier = min(multiplier, 0.0)

        # 3) 逐票操作建议（透传 regime_multiplier 给 advisor）
        pool_df = self.pool_repo.get_pool("core")
        code_name_map = dict(zip(pool_df["code"], pool_df["name"]))
        advisor = StockAdvisor(self.engine)
        signals = advisor.advise_batch(
            codes, trade_date, code_name_map,
            regime_multiplier=multiplier,
        )
        n_adv = save_advisor_signals(self.engine, signals)

        return (
            f"date={trade_date}, codes={len(codes)}, "
            f"regime={regime}(×{multiplier:.2f}), "
            f"signals={n_sigs}, composite={n_comp}, advisor={n_adv}"
        )

    def _check_portfolio_drawdown(self, threshold: float = -0.15) -> bool:
        """检查最近 backtest_nav 记录中的组合回撤是否超过阈值。"""
        try:
            nav_df = self._base_repo.read_sql(
                """
                SELECT trade_date, nav
                FROM backtest_nav
                ORDER BY trade_date DESC
                LIMIT 90
                """
            )
            if nav_df.empty or len(nav_df) < 5:
                return False
            nav = nav_df.sort_values("trade_date")["nav"].astype(float)
            rolling_max = nav.expanding().max()
            drawdown = float((nav / rolling_max - 1).min())
            if drawdown < threshold:
                logger.warning(f"[熔断] 组合最大回撤={drawdown:.1%} < 阈值={threshold:.1%}")
                return True
        except Exception as e:
            logger.debug(f"[熔断] 回撤检查失败: {e}")
        return False


def main():
    session = sys.argv[1] if len(sys.argv) > 1 else "full"
    if session not in ("morning", "afternoon", "full"):
        print(f"用法: python3 -m invest_model.pipeline.daily_pipeline [morning|afternoon|full]")
        sys.exit(1)

    pipeline = DailyPipeline()
    pipeline.run(session)


if __name__ == "__main__":
    main()
