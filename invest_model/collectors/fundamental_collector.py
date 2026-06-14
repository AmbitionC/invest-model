"""财务数据采集器"""

from datetime import datetime, timedelta

import pandas as pd

from invest_model.collectors.base import BaseCollector, logger
from invest_model.config import load_config
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.repositories.fundamental_repo import FundamentalRepository


class FundamentalCollector(BaseCollector):
    """
    采集财务相关数据：
    1. daily_basic: 每日估值指标（PE/PB/PS/总市值）—— 按日期拉全市场
    2. fina_indicator: 季度财务指标（ROE/EPS 等）—— 按股票拉
    3. moneyflow: 资金流向 —— 按股票+日期拉
    """

    def collect_daily_basic(self, trade_dates: list[str]) -> int:
        """按交易日拉取全市场每日估值指标（PE/PB 等）"""
        repo = FundamentalRepository(self.engine)
        total = 0

        for i, td in enumerate(trade_dates, 1):
            try:
                df = self.source.get_daily_basic(td)
                if not df.empty:
                    n = repo.save_daily_basic(df)
                    total += n
            except Exception as e:
                logger.error(f"daily_basic {td} 失败: {e}")

            if i % 20 == 0:
                self._log_progress(i, len(trade_dates), "每日估值")

        logger.info(f"每日估值采集完成: {len(trade_dates)} 天, 共 {total} 条")
        return total

    def collect_daily_basic_incremental(self) -> int:
        """增量采集每日估值：找出 stock_fundamental 中缺失的交易日"""
        cal_repo = CalendarRepository(self.engine)
        repo = FundamentalRepository(self.engine)

        latest_df = repo.read_sql(
            "SELECT MAX(trade_date) as max_date FROM stock_fundamental"
        )
        latest = latest_df["max_date"].iloc[0] if not latest_df.empty and latest_df["max_date"].iloc[0] else None

        if latest is None:
            cfg = load_config()
            years = cfg.get("collection", {}).get("history_years", 5)
            start_dt = datetime.now() - timedelta(days=365 * years)
            start_date = start_dt.strftime("%Y%m%d")
        else:
            start_date = latest

        end_date = datetime.now().strftime("%Y%m%d")
        all_trade_dates = cal_repo.get_trade_dates(start_date, end_date)

        if latest:
            existing_df = repo.read_sql(
                "SELECT DISTINCT trade_date FROM stock_fundamental WHERE trade_date >= :s",
                {"s": start_date},
            )
            existing_dates = set(existing_df["trade_date"].tolist())
            missing_dates = [d for d in all_trade_dates if d not in existing_dates]
        else:
            missing_dates = all_trade_dates

        if not missing_dates:
            logger.info("每日估值已是最新")
            return 0

        logger.info(f"每日估值缺失 {len(missing_dates)} 天，开始增量采集")
        return self.collect_daily_basic(missing_dates)

    def collect_fina_indicator(self, codes: list[str]) -> int:
        """采集季度财务指标"""
        repo = FundamentalRepository(self.engine)
        total = 0

        # 生成最近 5 年的报告期
        current_year = datetime.now().year
        periods = []
        for y in range(current_year - 5, current_year + 1):
            for q in ["0331", "0630", "0930", "1231"]:
                periods.append(f"{y}{q}")

        for i, code in enumerate(codes, 1):
            code_total = 0
            for period in periods:
                try:
                    df = self.source.get_stock_fundamental(code, period)
                    if not df.empty:
                        n = repo.save_fina_indicator(df)
                        code_total += n
                except Exception as e:
                    logger.warning(f"{code} period={period} 失败: {e}")

            total += code_total
            if i % 5 == 0:
                self._log_progress(i, len(codes), "季度财务")

        logger.info(f"季度财务采集完成: {len(codes)} 只, 共 {total} 条")
        return total

    def collect_cashflow(self, codes: list[str], trade_dates: list[str]) -> int:
        """采集资金流向"""
        repo = FundamentalRepository(self.engine)
        total = 0

        failed = 0
        for i, code in enumerate(codes, 1):
            for td in trade_dates:
                try:
                    df = self.source.get_cashflow(code, td)
                    if not df.empty:
                        n = repo.save_cashflow(df)
                        total += n
                except Exception as e:
                    failed += 1
                    logger.warning(f"资金流向 {code} {td} 失败: {e}")

            if i % 5 == 0:
                self._log_progress(i, len(codes), "资金流向")

        logger.info(f"资金流向采集完成: {len(codes)} 只, 共 {total} 条, 失败 {failed} 次")
        return total

    def collect_cashflow_incremental(
        self,
        codes: list[str],
        recent_days: int = 30,
    ) -> int:
        """增量采集资金流向：对每只股票，找出最近 recent_days 个交易日中
        stock_cashflow 里缺失的日期，再逐个补齐。

        recent_days 默认 30 交易日（约6周），足以覆盖长假或连续失败后的补数窗口。
        """
        repo = FundamentalRepository(self.engine)
        cal_repo = CalendarRepository(self.engine)

        end_date = datetime.now().strftime("%Y%m%d")
        start_dt = datetime.now() - timedelta(days=recent_days * 2 + 10)
        start_date = start_dt.strftime("%Y%m%d")
        all_trade_dates = cal_repo.get_trade_dates(start_date, end_date)
        if not all_trade_dates:
            return 0
        recent_trade_dates = all_trade_dates[-recent_days:]

        total = 0
        failed = 0
        for i, code in enumerate(codes, 1):
            existing = repo.read_sql(
                "SELECT trade_date FROM stock_cashflow WHERE code = :c AND trade_date >= :s",
                {"c": code, "s": recent_trade_dates[0]},
            )
            existing_dates = set(existing["trade_date"].tolist())
            missing = [d for d in recent_trade_dates if d not in existing_dates]

            for td in missing:
                try:
                    df = self.source.get_cashflow(code, td)
                    if not df.empty:
                        n = repo.save_cashflow(df)
                        total += n
                except Exception as e:
                    failed += 1
                    logger.warning(f"资金流向 {code} {td} 失败: {e}")

            if i % 5 == 0:
                self._log_progress(i, len(codes), "资金流向增量")

        logger.info(
            f"资金流向增量完成: {len(codes)} 只, 共 {total} 条, 失败 {failed} 次"
        )
        return total

    # ── 北向资金 ──────────────────────────────────────

    def collect_northbound_flow(self, trade_dates: list[str]) -> int:
        """按交易日拉取沪深港通资金流向"""
        from invest_model.repositories.base import BaseRepository

        base = BaseRepository(self.engine)
        total = 0

        for i, td in enumerate(trade_dates, 1):
            try:
                df = self.source.get_hsgt_flow(td)
                if df is not None and not df.empty:
                    n = base.upsert(
                        "stock_northbound_flow", df,
                        unique_keys=["trade_date"],
                    )
                    total += n
            except Exception as e:
                logger.warning(f"北向资金 {td} 失败: {e}")

            if i % 20 == 0:
                self._log_progress(i, len(trade_dates), "北向资金")

        logger.info(f"北向资金采集完成: {len(trade_dates)} 天, 共 {total} 条")
        return total

    def collect_northbound_incremental(self) -> int:
        """增量采集北向资金：找出缺失的交易日"""
        from invest_model.repositories.base import BaseRepository

        cal_repo = CalendarRepository(self.engine)
        base = BaseRepository(self.engine)

        latest_df = base.read_sql(
            "SELECT MAX(trade_date) as max_date FROM stock_northbound_flow"
        )
        latest = (
            latest_df["max_date"].iloc[0]
            if not latest_df.empty and latest_df["max_date"].iloc[0]
            else None
        )

        if latest is None:
            start_dt = datetime.now() - timedelta(days=400)
            start_date = start_dt.strftime("%Y%m%d")
        else:
            start_date = latest

        end_date = datetime.now().strftime("%Y%m%d")
        all_trade_dates = cal_repo.get_trade_dates(start_date, end_date)

        if latest:
            existing_df = base.read_sql(
                "SELECT trade_date FROM stock_northbound_flow WHERE trade_date >= :s",
                {"s": start_date},
            )
            existing_dates = set(existing_df["trade_date"].tolist())
            missing = [d for d in all_trade_dates if d not in existing_dates]
        else:
            missing = all_trade_dates

        if not missing:
            logger.info("北向资金已是最新")
            return 0

        logger.info(f"北向资金缺失 {len(missing)} 天，开始增量采集")
        return self.collect_northbound_flow(missing)
