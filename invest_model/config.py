"""配置加载：合并 config.yaml + .env 环境变量"""

from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import yaml
from dotenv import load_dotenv
import os

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_config_cache: dict | None = None


def get_project_root() -> Path:
    return _PROJECT_ROOT


def load_config(config_path: str | Path | None = None) -> dict:
    """加载 config.yaml，结果缓存到进程生命周期内"""
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    load_dotenv(_PROJECT_ROOT / ".env")

    if config_path is None:
        config_path = _PROJECT_ROOT / "config" / "config.yaml"
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    _config_cache = cfg
    return cfg


def get_env(key: str, default: str | None = None) -> str:
    """读取环境变量（每次强制重载 .env，确保拿到最新值）"""
    load_dotenv(_PROJECT_ROOT / ".env", override=True)
    val = os.getenv(key, default)
    if val is None:
        raise ValueError(f"环境变量 {key} 未设置")
    return val


def get_mysql_url() -> str:
    """构建 MySQL SQLAlchemy URL"""
    host = get_env("MYSQL_HOST", "localhost")
    port = get_env("MYSQL_PORT", "3306")
    user = get_env("MYSQL_USER", "root")
    password = get_env("MYSQL_PASSWORD", "")
    database = get_env("MYSQL_DATABASE", "invest")
    return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}?charset=utf8mb4"
