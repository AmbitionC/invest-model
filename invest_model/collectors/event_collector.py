"""事件数据采集器"""

from datetime import datetime, timedelta

from invest_model.collectors.base import BaseCollector, logger
from invest_model.config import load_config
from invest_model.repositories.event_repo import EventRepository


class EventCollector(BaseCollector):
    """股东增减持 + 股东户数采集"""

    def collect_holder_trade(self, codes: list[str], start_date: str | None = None,
                              end_date: str | None = None) -> int:
        """采集股东增减持（增量：按 code 检查已有数据，只拉新增部分）"""
        cfg = load_config()
        years = cfg.get("collection", {}).get("history_years", 5)

        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        default_start_dt = datetime.now() - timedelta(days=365 * years)
        default_start = default_start_dt.strftime("%Y%m%d")

        repo = EventRepository(self.engine)
        total = 0
        skipped = 0

        for i, code in enumerate(codes, 1):
            try:
                # 增量：查询该 code 在 DB 中最新的 ann_date，从该日期开始拉取
                code_start = start_date or default_start
                latest_df = repo.read_sql(
                    "SELECT MAX(ann_date) as max_date FROM stock_holder_trade WHERE code = :code",
                    {"code": code},
                )
                latest = latest_df["max_date"].iloc[0] if not latest_df.empty and latest_df["max_date"].iloc[0] else None
                if latest:
                    code_start = latest

                # 如果已有数据且最新日期就是今天，跳过
                if latest and latest >= end_date:
                    skipped += 1
                    continue

                df = self.source.get_holder_trade(code, code_start, end_date)
                if df.empty:
                    continue

                col_map = {
                    "in_de": "trade_type",
                    "holder_name": "holder_name",
                    "holder_type": "holder_type",
                }
                df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

                for col in ("begin_date", "close_date"):
                    if col not in df.columns:
                        df[col] = None

                n = repo.save_holder_trade(df)
                total += n
            except Exception as e:
                logger.warning(f"{code}: 股东增减持采集失败 - {e}")

            if i % 10 == 0:
                self._log_progress(i, len(codes), "股东增减持")

        logger.info(f"股东增减持采集完成: {len(codes)} 只, 新增 {total} 条, 跳过 {skipped} 只")
        return total

    def collect_holder_count(self, codes: list[str], start_date: str | None = None,
                              end_date: str | None = None) -> int:
        """采集股东户数（增量：按 code 检查已有数据，只拉新增部分）"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        default_start_dt = datetime.now() - timedelta(days=365 * 3)
        default_start = default_start_dt.strftime("%Y%m%d")

        repo = EventRepository(self.engine)
        total = 0
        skipped = 0

        for i, code in enumerate(codes, 1):
            try:
                # 增量：查询该 code 在 DB 中最新的 end_date，从该日期开始拉取
                code_start = start_date or default_start
                latest_df = repo.read_sql(
                    "SELECT MAX(end_date) as max_date FROM stock_holder_count WHERE code = :code",
                    {"code": code},
                )
                latest = latest_df["max_date"].iloc[0] if not latest_df.empty and latest_df["max_date"].iloc[0] else None
                if latest:
                    code_start = latest

                # 如果已有数据且最新日期就是今天，跳过
                if latest and latest >= end_date:
                    skipped += 1
                    continue

                df = self.source.get_holder_count(code, code_start, end_date)
                if df.empty:
                    continue
                n = repo.save_holder_count(df)
                total += n
            except Exception as e:
                logger.warning(f"{code}: 股东户数采集失败 - {e}")

            if i % 10 == 0:
                self._log_progress(i, len(codes), "股东户数")

        logger.info(f"股东户数采集完成: {len(codes)} 只, 新增 {total} 条, 跳过 {skipped} 只")
        return total
