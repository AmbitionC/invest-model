"""采集器基类"""

from __future__ import annotations

from typing import TYPE_CHECKING

from invest_model.logger import get_logger

if TYPE_CHECKING:
    from invest_model.sources.base import BaseSource

logger = get_logger()


class BaseCollector:
    """数据采集器基类，定义通用采集流程。

    source 为可选参数：需要外部数据源的采集器（日线、事件等）会传入，
    而纯派生型采集器（如 TechnicalCollector）仅需 engine 即可。
    """

    def __init__(self, source: BaseSource | None = None, engine=None):
        self.source = source
        self.engine = engine

    def _log_progress(self, current: int, total: int, label: str = "") -> None:
        if total > 0:
            pct = current / total * 100
            logger.info(f"[{label}] 进度: {current}/{total} ({pct:.0f}%)")
