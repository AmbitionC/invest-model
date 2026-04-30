"""市场数据采集器（融资融券 + 指数日线）"""

from datetime import datetime, timedelta

from invest_model.collectors.base import BaseCollector, logger
from invest_model.config import load_config
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.repositories.market_repo import MarketRepository

DEFAULT_INDICES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
]


class MarketCollector(BaseCollector):
    """融资融券 + 指数日线采集"""

    def collect_margin(self, trade_dates: list[str]) -> int:
        """按交易日采集全市场融资融券数据"""
        repo = MarketRepository(self.engine)
        total = 0

        for i, td in enumerate(trade_dates, 1):
            try:
                df = self.source.get_margin(td)
                if not df.empty:
                    n = repo.save_margin(df)
                    total += n
            except Exception as e:
                logger.warning(f"融资融券 {td}: {e}")

            if i % 20 == 0:
                self._log_progress(i, len(trade_dates), "融资融券")

        logger.info(f"融资融券采集完成: {len(trade_dates)} 天, 共 {total} 条")
        return total

    def collect_margin_incremental(self) -> int:
        """增量采集融资融券"""
        cal_repo = CalendarRepository(self.engine)
        repo = MarketRepository(self.engine)

        latest_df = repo.read_sql("SELECT MAX(trade_date) as max_date FROM stock_margin")
        latest = latest_df["max_date"].iloc[0] if not latest_df.empty and latest_df["max_date"].iloc[0] else None

        if latest is None:
            cfg = load_config()
            years = cfg.get("collection", {}).get("history_years", 5)
            start_dt = datetime.now() - timedelta(days=365 * years)
            start_date = start_dt.strftime("%Y%m%d")
        else:
            start_date = latest

        end_date = datetime.now().strftime("%Y%m%d")
        all_dates = cal_repo.get_trade_dates(start_date, end_date)

        if latest:
            existing_df = repo.read_sql(
                "SELECT DISTINCT trade_date FROM stock_margin WHERE trade_date >= :s",
                {"s": start_date},
            )
            existing = set(existing_df["trade_date"].tolist())
            missing = [d for d in all_dates if d not in existing]
        else:
            missing = all_dates

        if not missing:
            logger.info("融资融券已是最新")
            return 0

        logger.info(f"融资融券缺失 {len(missing)} 天")

        # 只采集最近 5 天，避免历史回溯耗时过长
        if len(missing) > 5:
            logger.info(f"截取最近 5 天采集: {missing[-5:]}")
            missing = missing[-5:]

        return self.collect_margin(missing)

    def collect_index_daily(self, index_codes: list[str] | None = None,
                            start_date: str | None = None,
                            end_date: str | None = None) -> dict[str, int]:
        """采集指数日线（历史）"""
        if index_codes is None:
            index_codes = DEFAULT_INDICES

        cfg = load_config()
        years = cfg.get("collection", {}).get("history_years", 5)
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_dt = datetime.now() - timedelta(days=365 * years)
            start_date = start_dt.strftime("%Y%m%d")

        repo = MarketRepository(self.engine)
        results = {}

        for code in index_codes:
            try:
                df = self.source.get_index_daily(code, start_date, end_date)
                if df.empty:
                    results[code] = 0
                    continue
                n = repo.save_index_daily(df)
                results[code] = n
                logger.info(f"指数 {code}: {n} 条")
            except Exception as e:
                logger.error(f"指数 {code}: {e}")
                results[code] = -1

        return results

    def collect_index_incremental(self, index_codes: list[str] | None = None) -> dict[str, int]:
        """指数日线增量"""
        if index_codes is None:
            index_codes = DEFAULT_INDICES

        end_date = datetime.now().strftime("%Y%m%d")
        cal_repo = CalendarRepository(self.engine)
        repo = MarketRepository(self.engine)
        results = {}

        for code in index_codes:
            try:
                latest = repo.get_index_latest_date(code)
                if latest is None:
                    cfg = load_config()
                    years = cfg.get("collection", {}).get("history_years", 5)
                    start_dt = datetime.now() - timedelta(days=365 * years)
                    start_date = start_dt.strftime("%Y%m%d")
                else:
                    start_date = latest

                missing = cal_repo.get_missing_trade_dates("index_daily", code, start_date, end_date)
                if not missing:
                    results[code] = 0
                    continue

                df = self.source.get_index_daily(code, missing[0], missing[-1])
                if df.empty:
                    results[code] = 0
                    continue

                n = repo.save_index_daily(df)
                results[code] = n
            except Exception as e:
                logger.error(f"指数增量 {code}: {e}")
                results[code] = -1

        return results
