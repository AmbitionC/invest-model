"""技术指标采集器 — 从 stock_daily 读取 OHLCV，计算指标后写入 stock_technical

与其他采集器不同，TechnicalCollector 不依赖外部数据源（BaseSource），
而是从数据库已有日线数据中派生技术指标，构造时只需传入 engine。
"""

from datetime import datetime, timedelta

from invest_model.collectors.base import BaseCollector, logger
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.repositories.technical_repo import TechnicalRepository
from invest_model.technical.calculator import compute_technical, WARMUP_DAYS


def _load_st_set(engine) -> set[str]:
    """从 stock_info 中查询名称含 'ST' 的股票代码集合。

    stock_info.ts_code 与 stock_daily.code / stock_pool.code 格式一致（如 000001.SZ），
    可直接用于后续 code in st_set 比对。
    """
    repo = BaseRepository(engine)
    try:
        df = repo.read_sql(
            "SELECT ts_code FROM stock_info WHERE name LIKE '%ST%'"
        )
        st_set = set(df["ts_code"].tolist()) if not df.empty else set()
        if st_set:
            logger.debug(f"加载 ST 股票 {len(st_set)} 只")
        return st_set
    except Exception as e:
        logger.warning(f"ST 股票集合加载失败（涨跌停阈值将按非 ST 处理）: {e}")
        return set()


class TechnicalCollector(BaseCollector):
    """技术指标采集：从已有日线数据派生，无需外部数据源。"""

    def __init__(self, engine=None, **kwargs):
        super().__init__(source=None, engine=engine)

    def collect_history(self, codes: list[str], start_date: str | None = None,
                        end_date: str | None = None) -> dict[str, int]:
        """全量计算并落库"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y%m%d")

        warmup_start = (datetime.strptime(start_date, "%Y%m%d")
                        - timedelta(days=int(WARMUP_DAYS * 1.6))).strftime("%Y%m%d")

        daily_repo = StockDailyRepository(self.engine)
        tech_repo = TechnicalRepository(self.engine)
        st_set = _load_st_set(self.engine)
        results: dict[str, int] = {}

        for i, code in enumerate(codes, 1):
            self._log_progress(i, len(codes), "技术指标历史")
            try:
                df = daily_repo.get_daily(code, warmup_start, end_date)
                if df.empty or len(df) < WARMUP_DAYS:
                    results[code] = 0
                    continue

                computed = compute_technical(df, code=code, is_st=(code in st_set))
                to_save = computed[computed["trade_date"] >= start_date].copy()
                to_save = to_save.dropna(subset=["boll_mid"])

                n = tech_repo.save(to_save)
                results[code] = n
                if n > 0:
                    logger.info(f"{code}: 写入 {n} 条技术指标 ({start_date}~{end_date})")
            except Exception as e:
                logger.error(f"{code}: 技术指标历史计算失败 - {e}")
                results[code] = -1

        total = sum(v for v in results.values() if v > 0)
        logger.info(f"技术指标历史完成: {len(codes)} 只, 共 {total} 条")
        return results

    def collect_incremental(self, codes: list[str],
                            end_date: str | None = None) -> dict[str, int]:
        """
        增量计算：找出 stock_technical 的最新日期，向前多读 WARMUP_DAYS 天日线数据
        作为预热窗口，但只写入新增日期的指标。
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        daily_repo = StockDailyRepository(self.engine)
        tech_repo = TechnicalRepository(self.engine)
        st_set = _load_st_set(self.engine)
        results: dict[str, int] = {}

        for i, code in enumerate(codes, 1):
            try:
                latest_tech = tech_repo.get_latest_date(code=code)
                latest_daily = daily_repo.get_latest_date(code=code)

                if latest_daily is None:
                    results[code] = 0
                    continue

                if latest_tech is not None and latest_tech >= latest_daily:
                    results[code] = 0
                    continue

                fetch_start = (datetime.strptime(latest_daily, "%Y%m%d")
                               - timedelta(days=int(WARMUP_DAYS * 1.6))).strftime("%Y%m%d")

                df = daily_repo.get_daily(code, fetch_start, end_date)
                if df.empty or len(df) < WARMUP_DAYS:
                    results[code] = 0
                    continue

                computed = compute_technical(df, code=code, is_st=(code in st_set))

                if latest_tech is not None:
                    to_save = computed[computed["trade_date"] > latest_tech].copy()
                else:
                    to_save = computed.copy()

                to_save = to_save.dropna(subset=["boll_mid"])
                if to_save.empty:
                    results[code] = 0
                    continue

                n = tech_repo.save(to_save)
                results[code] = n
                if n > 0:
                    logger.info(f"{code}: 增量写入 {n} 条技术指标")
            except Exception as e:
                logger.error(f"{code}: 技术指标增量计算失败 - {e}")
                results[code] = -1

            if i % 10 == 0:
                self._log_progress(i, len(codes), "技术指标增量")

        total = sum(v for v in results.values() if v > 0)
        logger.info(f"技术指标增量完成: {len(codes)} 只, 新增 {total} 条")
        return results
