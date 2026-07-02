"""统一日志配置"""

import os
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
    # INVEST_LOG_DIR：FaaS/容器等代码目录只读的环境把日志文件指到 /tmp
    log_dir = Path(os.getenv("INVEST_LOG_DIR") or log_cfg.get("dir", "./logs"))

    if not log_dir.is_absolute():
        log_dir = get_project_root() / log_dir

    logger.remove()

    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | <cyan>{message}</cyan>",
    )

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning(f"日志目录 {log_dir} 不可写，仅输出到控制台：{e}")
        _configured = True
        return

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
