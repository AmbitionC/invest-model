"""交易日历采集器"""

from datetime import datetime

from invest_model.collectors.base import BaseCollector, logger
from invest_model.repositories.calendar_repo import CalendarRepository


class CalendarCollector(BaseCollector):
    """采集并同步交易日历"""

    def collect(self, start_date: str | None = None, end_date: str | None = None) -> int:
        """
        采集交易日历。
        默认范围：从 5 年前到今年年底。
        增量逻辑：如果数据库中已有日历且覆盖到今年年底，则跳过。
        """
        if end_date is None:
            end_date = f"{datetime.now().year}1231"
        if start_date is None:
            start_year = datetime.now().year - 5
            start_date = f"{start_year}0101"

        # 增量检查：如果日历已覆盖到 end_date，则跳过
        if self.engine:
            repo = CalendarRepository(self.engine)
            latest = repo.get_latest_date()
            if latest and latest >= end_date:
                logger.info(f"交易日历已覆盖到 {latest}，无需更新")
                return 0
            # 只拉取缺失的部分
            if latest and latest >= start_date:
                start_date = latest

        logger.info(f"采集交易日历: {start_date} ~ {end_date}")
        df = self.source.get_trade_calendar(start_date, end_date)
        if df.empty:
            logger.warning("获取到空的交易日历")
            return 0

        logger.info(f"获取到 {len(df)} 条日历记录，其中交易日 {df['is_open'].sum()} 天")

        if self.engine:
            n = repo.save(df)
            logger.info(f"写入 trade_calendar: {n} 条")
            return n

        return len(df)
