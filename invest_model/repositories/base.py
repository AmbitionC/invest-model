"""Repository 基类：通用数据库操作"""

import math
import numbers
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger

logger = get_logger()


def _dataframe_nulls_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """把 DataFrame 里各类 NA 换成 None，避免 to_dict('records') 仍带出 float NaN 导致 pymysql 报错。"""
    out = df.copy()
    return out.where(pd.notna(out), None)


def _records_coerce_nulls_for_mysql(records: list[dict]) -> None:
    """将 pandas/numpy 的 NA/NaN 及非有限浮点转为 None，避免 pymysql / DECIMAL 报错。"""
    for rec in records:
        for k, v in rec.items():
            if v is None:
                continue
            if pd.api.types.is_scalar(v) and pd.isna(v):
                rec[k] = None
                continue
            if isinstance(v, numbers.Real) and not isinstance(v, bool):
                try:
                    if not math.isfinite(float(v)):
                        rec[k] = None
                except (TypeError, OverflowError, ValueError):
                    rec[k] = None


class BaseRepository:
    """所有 Repository 的基类，提供通用 CRUD 操作"""

    def __init__(self, engine: Engine):
        self.engine = engine

    def execute_sql(self, sql: str, params: dict | None = None) -> None:
        """执行原始 SQL（DDL / DML）"""
        with self.engine.begin() as conn:
            conn.execute(text(sql), params or {})

    def read_sql(self, sql: str, params: dict | None = None) -> pd.DataFrame:
        """执行 SELECT 返回 DataFrame"""
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)

    def upsert(self, table: str, df: pd.DataFrame, unique_keys: list[str]) -> int:
        """
        批量 Upsert（INSERT ... ON DUPLICATE KEY UPDATE）。
        要求表已存在对应的 UNIQUE 索引。
        返回写入行数。
        """
        if df.empty:
            return 0

        # 校验：unique_keys 必须全部存在于 DataFrame 中，否则写入必定失败
        missing_keys = [k for k in unique_keys if k not in df.columns]
        if missing_keys:
            raise ValueError(
                f"Upsert {table}: DataFrame 缺少主键列 {missing_keys}，"
                f"现有列: {list(df.columns)}"
            )

        df = _dataframe_nulls_to_none(df)
        columns = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in columns])
        col_list = ", ".join([f"`{c}`" for c in columns])
        update_cols = [c for c in columns if c not in unique_keys]
        if update_cols:
            update_clause = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols])
            sql = f"""
                INSERT INTO {table} ({col_list})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
        else:
            # 无数据列可更新，改用 INSERT IGNORE
            sql = f"""
                INSERT IGNORE INTO {table} ({col_list})
                VALUES ({placeholders})
            """

        records = df.to_dict("records")
        _records_coerce_nulls_for_mysql(records)
        batch_size = 500
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            with self.engine.begin() as conn:
                conn.execute(text(sql), batch)
            total += len(batch)

        logger.debug(f"Upsert {table}: {total} rows")
        return total

    def bulk_insert(self, table: str, df: pd.DataFrame) -> int:
        """批量 INSERT IGNORE（忽略重复）"""
        if df.empty:
            return 0

        df = _dataframe_nulls_to_none(df)
        columns = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in columns])
        col_list = ", ".join([f"`{c}`" for c in columns])

        sql = f"INSERT IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"

        records = df.to_dict("records")
        _records_coerce_nulls_for_mysql(records)
        batch_size = 500
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            with self.engine.begin() as conn:
                conn.execute(text(sql), batch)
            total += len(batch)

        logger.debug(f"bulk_insert {table}: {total} rows")
        return total

    def get_latest_date(self, table: str, code: str, date_col: str = "trade_date") -> Optional[str]:
        """获取某只股票在表中的最新日期"""
        sql = f"SELECT MAX(`{date_col}`) as max_date FROM `{table}` WHERE code = :code"
        df = self.read_sql(sql, {"code": code})
        if df.empty or df["max_date"].iloc[0] is None:
            return None
        return str(df["max_date"].iloc[0])

    def get_row_count(self, table: str) -> int:
        """获取表行数"""
        df = self.read_sql(f"SELECT COUNT(*) as cnt FROM {table}")
        return int(df["cnt"].iloc[0])

    def table_exists(self, table: str) -> bool:
        """检查表是否存在"""
        sql = "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :tbl"
        df = self.read_sql(sql, {"tbl": table})
        return int(df["cnt"].iloc[0]) > 0
