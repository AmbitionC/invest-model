"""个股日线采集器"""

from datetime import datetime, timedelta

import pandas as pd

from invest_model.collectors.base import BaseCollector, logger
from invest_model.config import load_config
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository


class StockDailyCollector(BaseCollector):
    """个股日线行情采集：支持历史初始化和增量更新"""

    def collect_history(self, codes: list[str], start_date: str | None = None,
                        end_date: str | None = None) -> dict[str, int]:
        """历史数据初始化采集"""
        cfg = load_config()
        years = cfg.get("collection", {}).get("history_years", 5)

        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_dt = datetime.now() - timedelta(days=365 * years)
            start_date = start_dt.strftime("%Y%m%d")

        repo = StockDailyRepository(self.engine)
        results = {}

        for i, code in enumerate(codes, 1):
            self._log_progress(i, len(codes), "日线历史")
            try:
                df = self.source.get_stock_daily(code, start_date, end_date)
                if df.empty:
                    logger.warning(f"{code}: 无数据")
                    results[code] = 0
                    continue

                n = repo.save(df)
                results[code] = n
                logger.info(f"{code}: 写入 {n} 条日线 ({start_date}~{end_date})")
            except Exception as e:
                logger.error(f"{code}: 采集失败 - {e}")
                results[code] = -1

        total = sum(v for v in results.values() if v > 0)
        logger.info(f"日线历史采集完成: {len(codes)} 只股票, 共 {total} 条")
        return results

    def collect_incremental(self, codes: list[str],
                            end_date: str | None = None) -> dict[str, int]:
        """
        增量采集：基于交易日历找出缺失日期，只拉缺失部分。
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        cal_repo = CalendarRepository(self.engine)
        daily_repo = StockDailyRepository(self.engine)
        results = {}

        for i, code in enumerate(codes, 1):
            try:
                latest = daily_repo.get_latest_date(code=code)
                if latest is None:
                    cfg = load_config()
                    years = cfg.get("collection", {}).get("history_years", 5)
                    start_dt = datetime.now() - timedelta(days=365 * years)
                    start_date = start_dt.strftime("%Y%m%d")
                else:
                    start_date = latest

                missing = cal_repo.get_missing_trade_dates(
                    "stock_daily", code, start_date, end_date
                )

                if not missing:
                    results[code] = 0
                    continue

                fetch_start = missing[0]
                fetch_end = missing[-1]

                df = self.source.get_stock_daily(code, fetch_start, fetch_end)
                if df.empty:
                    results[code] = 0
                    continue

                n = daily_repo.save(df)
                results[code] = n
                if n > 0:
                    logger.info(f"{code}: 增量写入 {n} 条 ({fetch_start}~{fetch_end})")
            except Exception as e:
                logger.error(f"{code}: 增量采集失败 - {e}")
                results[code] = -1

            if i % 10 == 0:
                self._log_progress(i, len(codes), "日线增量")

        total = sum(v for v in results.values() if v > 0)
        logger.info(f"日线增量完成: {len(codes)} 只, 新增 {total} 条")
        return results
