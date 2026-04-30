"""MySQL 数据库连接管理"""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from invest_model.config import get_mysql_url, load_config

_engine: Engine | None = None


def get_engine() -> Engine:
    """获取全局 SQLAlchemy Engine（单例）"""
    global _engine
    if _engine is not None:
        return _engine

    cfg = load_config()
    db_cfg = cfg.get("database", {})

    _engine = create_engine(
        get_mysql_url(),
        pool_size=db_cfg.get("pool_size", 5),
        max_overflow=db_cfg.get("max_overflow", 10),
        echo=db_cfg.get("echo", False),
        pool_recycle=3600,
        pool_pre_ping=True,
    )
    return _engine


def test_connection() -> bool:
    """测试数据库连接"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        return True
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return False


def reset_engine():
    """重置连接（测试用）"""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
