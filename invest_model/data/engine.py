"""引擎工厂：根据 db_url 创建 SQLAlchemy Engine，支持 MySQL 与 SQLite。

db_url 解析优先级：
1. 显式传入的 ``db_url`` 参数（CLI ``--db``）
2. 环境变量 ``INVEST_DB_URL``
3. 回退到 ``config.get_mysql_url()``（从 .env 拼 MySQL）

SQLite 形如 ``sqlite:///./data/local.db``（相对项目根）或 ``sqlite:////abs/path.db``。
"""

from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine

from invest_model.config import get_project_root
from invest_model.logger import get_logger

logger = get_logger()


def resolve_db_url(db_url: str | None = None) -> str:
    """解析最终使用的数据库 URL。"""
    if db_url:
        return db_url
    env_url = os.getenv("INVEST_DB_URL", "").strip()
    if env_url:
        return env_url
    # 回退到 MySQL（生产默认）
    from invest_model.config import get_mysql_url

    return get_mysql_url()


def make_engine(db_url: str | None = None) -> Engine:
    """创建 Engine。SQLite 自动建好父目录并开启外键/合理的并发参数。"""
    url = resolve_db_url(db_url)

    if url.startswith("sqlite"):
        # 解析文件路径，相对路径基于项目根
        # 形如 sqlite:///./data/local.db  -> ./data/local.db
        raw = url.split("sqlite:///", 1)[-1] if url.startswith("sqlite:///") else ""
        if raw and not raw.startswith("/"):
            abs_path = (get_project_root() / raw).resolve()
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            url = f"sqlite:///{abs_path}"
        elif raw:
            Path(raw).parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(url, future=True)

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _rec):  # noqa: ANN001
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.close()

        logger.info(f"使用 SQLite 后端: {url}")
        return engine

    # MySQL
    from invest_model.config import load_config

    db_cfg = load_config().get("database", {})
    engine = create_engine(
        url,
        pool_size=db_cfg.get("pool_size", 5),
        max_overflow=db_cfg.get("max_overflow", 10),
        echo=db_cfg.get("echo", False),
        pool_recycle=3600,
        pool_pre_ping=True,
        future=True,
    )
    logger.info("使用 MySQL 后端")
    return engine
