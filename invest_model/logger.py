"""统一日志配置"""

import sys
from pathlib import Path

from loguru import logger

from invest_model.config import get_project_root, load_config

_configured = False


def setup_logger() -> None:
    """配置 loguru 日志：控制台 INFO + 文件 DEBUG + 错误文件"""
    global _configured
    if _configured:
        return

    cfg = load_config()
    log_cfg = cfg.get("logging", {})
    level = log_cfg.get("level", "INFO")
    log_dir = Path(log_cfg.get("dir", "./logs"))

    if not log_dir.is_absolute():
        log_dir = get_project_root() / log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.remove()

    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | <cyan>{message}</cyan>",
    )

    logger.add(
        log_dir / "invest-model-{time:YYYYMMDD}.log",
        level="DEBUG",
        rotation="1 day",
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {module}:{function}:{line} | {message}",
    )

    logger.add(
        log_dir / "error-{time:YYYYMMDD}.log",
        level="ERROR",
        rotation="1 day",
        retention="90 days",
        encoding="utf-8",
    )

    _configured = True


def get_logger():
    """获取已配置的 logger"""
    setup_logger()
    return logger
