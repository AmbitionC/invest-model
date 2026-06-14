"""数据后端可移植层：统一的引擎工厂与 schema。

支持两种后端，代码完全一致：
- 生产：MySQL（阿里云 RDS，从 .env 读取连接信息）
- 验证/本地：SQLite 文件库（``sqlite:///./data/local.db``）

新系统的所有表通过 :mod:`invest_model.data.schema` 用 SQLAlchemy Core 定义，
``create_schema(engine)`` 在两种后端上行为一致（``checkfirst=True`` 只补建缺失表，
不会破坏生产库里已存在的历史表）。
"""

from invest_model.data.engine import make_engine, resolve_db_url
from invest_model.data.schema import create_schema, metadata

__all__ = ["make_engine", "resolve_db_url", "create_schema", "metadata"]
