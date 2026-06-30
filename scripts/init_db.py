"""在目标 MySQL 服务器上创建 invest 库（幂等）。仅 MySQL 后端有意义；SQLite 无需建库。

连接时去掉 URL 里的库名（连到服务器而非具体库），执行
CREATE DATABASE IF NOT EXISTS `<db>` CHARACTER SET utf8mb4。要求账号有 CREATE 权限。
不打印任何凭证明文。
"""

from __future__ import annotations

import sys

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from invest_model.data.engine import resolve_db_url


def main() -> None:
    url = make_url(resolve_db_url(None))
    if not url.get_backend_name().startswith("mysql"):
        print(f"[init-db] 跳过：非 MySQL 后端（{url.get_backend_name()}），无需建库。")
        sys.exit(0)

    db_name = url.database
    if not db_name:
        print("[init-db] FAIL 解析不到目标库名（INVEST_DB_URL/MYSQL_DATABASE）。")
        sys.exit(1)

    server_url = url._replace(database=None)  # 连到服务器，不指定库（set(None) 是 no-op，须用 _replace）
    try:
        eng = create_engine(server_url, future=True)
        with eng.connect() as c:
            c.execute(text(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
            ))
            c.commit()
            exists = c.execute(text(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:n"
            ), {"n": db_name}).scalar()
        if exists == db_name:
            print(f"[init-db] OK 库 `{db_name}` 已就绪（已存在则跳过创建）。")
            sys.exit(0)
        print(f"[init-db] FAIL 创建后仍查不到库 `{db_name}`。")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"[init-db] FAIL {type(e).__name__}: {e}")
        print("  常见原因：DB 账号无 CREATE 权限 → 用管理员账号，或在 RDS DMS 手动建库。")
        sys.exit(1)


if __name__ == "__main__":
    main()
