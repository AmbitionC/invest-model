#!/usr/bin/env python3
"""
日志系统 - 记录所有运行状态和错误
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(name='invest-model', log_dir='logs'):
    """设置日志系统"""
    
    PROJECT_ROOT = Path(__file__).parent.parent
    log_path = PROJECT_ROOT / log_dir
    log_path.mkdir(parents=True, exist_ok=True)
    
    # 日志文件
    log_file = log_path / f"{name}-{datetime.now().strftime('%Y%m%d')}.log"
    error_file = log_path / f"{name}-errors-{datetime.now().strftime('%Y%m%d')}.log"
    
    # 创建 logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # 清除已有 handlers
    logger.handlers = []
    
    # 控制台 handler（INFO 级别）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 文件 handler（DEBUG 级别）
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(module)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # 错误文件 handler（ERROR 级别）
    error_handler = logging.FileHandler(error_file, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    logger.addHandler(error_handler)
    
    return logger

# 使用示例
if __name__ == '__main__':
    logger = setup_logger()
    logger.info("日志系统初始化完成")
    logger.debug("这是 debug 信息")
    logger.warning("这是 warning 信息")
    logger.error("这是 error 信息")
