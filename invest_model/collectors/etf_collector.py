"""ETF 数据采集器"""

from datetime import datetime, timedelta

from invest_model.collectors.base import BaseCollector, logger
from invest_model.config import load_config
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.repositories.etf_repo import ETFRepository


class ETFCollector(BaseCollector):
    """ETF 数据采集：日线 + 重仓股持仓"""

    def collect_daily_history(self, codes: list[str], start_date: str | None = None,
                              end_date: str | None = None) -> dict[str, int]:
        """ETF 日线历史采集"""
        cfg = load_config()
        years = cfg.get("collection", {}).get("history_years", 5)

        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_dt = datetime.now() - timedelta(days=365 * years)
            start_date = start_dt.strftime("%Y%m%d")

        repo = ETFRepository(self.engine)
        results = {}

        for i, code in enumerate(codes, 1):
            try:
                df = self.source.get_etf_daily(code, start_date, end_date)
                if df.empty:
                    results[code] = 0
                    continue
                n = repo.save_daily(df)
                results[code] = n
                logger.info(f"ETF {code}: 写入 {n} 条日线")
            except Exception as e:
                logger.error(f"ETF {code}: 日线采集失败 - {e}")
                results[code] = -1

            if i % 5 == 0:
                self._log_progress(i, len(codes), "ETF日线")

        return results

    def collect_daily_incremental(self, codes: list[str]) -> dict[str, int]:
        """ETF 日线增量采集"""
        end_date = datetime.now().strftime("%Y%m%d")
        cal_repo = CalendarRepository(self.engine)
        etf_repo = ETFRepository(self.engine)
        results = {}

        for code in codes:
            try:
                latest = etf_repo.get_latest_date(code)
                if latest is None:
                    cfg = load_config()
                    years = cfg.get("collection", {}).get("history_years", 5)
                    start_dt = datetime.now() - timedelta(days=365 * years)
                    start_date = start_dt.strftime("%Y%m%d")
                else:
                    start_date = latest

                missing = cal_repo.get_missing_trade_dates("etf_daily", code, start_date, end_date)
                if not missing:
                    results[code] = 0
                    continue

                df = self.source.get_etf_daily(code, missing[0], missing[-1])
                if df.empty:
                    results[code] = 0
                    continue

                n = etf_repo.save_daily(df)
                results[code] = n
                if n > 0:
                    logger.info(f"ETF {code}: 增量 {n} 条")
            except Exception as e:
                logger.error(f"ETF {code}: 增量失败 - {e}")
                results[code] = -1

        return results

    def collect_holdings(self, codes: list[str], report_dates: list[str] | None = None) -> int:
        """采集 ETF 重仓股持仓"""
        if report_dates is None:
            current_year = datetime.now().year
            report_dates = []
            for y in range(current_year - 2, current_year + 1):
                for q in ["0331", "0630", "0930", "1231"]:
                    report_dates.append(f"{y}{q}")

        repo = ETFRepository(self.engine)
        total = 0

        for i, code in enumerate(codes, 1):
            for rd in report_dates:
                try:
                    df = self.source.get_etf_holding(code, rd)
                    if df.empty:
                        continue

                    df_save = df.rename(columns={
                        "symbol": "stock_code",
                        "mkv": "holding_mv",
                        "stk_mkv_ratio": "holding_ratio",
                        "amount": "holding_amount",
                    })
                    df_save["code"] = code
                    df_save["report_date"] = rd

                    if "stock_code" not in df_save.columns and "stk_code" in df_save.columns:
                        df_save = df_save.rename(columns={"stk_code": "stock_code"})
                    if "stock_name" not in df_save.columns and "stk_name" in df_save.columns:
                        df_save = df_save.rename(columns={"stk_name": "stock_name"})

                    n = repo.save_holding(df_save)
                    total += n
                except Exception as e:
                    logger.warning(f"ETF {code} 持仓 {rd}: {e}")

            if i % 5 == 0:
                self._log_progress(i, len(codes), "ETF持仓")

        logger.info(f"ETF 持仓采集完成: {total} 条")
        return total
