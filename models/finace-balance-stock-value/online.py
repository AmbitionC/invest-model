"""
Executable script to fetch SSE/SZSE margin balance and float market cap,
align by date, and compute total metrics.

Usage:
  - Optionally set START_DATE and END_DATE as YYYYMMDD (default: last 18 years).
  - RDS config via env or .env (auto-loaded if python-dotenv is installed).
  - Run: python online.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import MetaData, Table

# Optional: load environment variables from .env if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def setup_tushare(token: Optional[str] = None) -> ts.pro_api:
    """Initialize tushare pro client using provided token or env var.

    Priority: function arg token > env var TUSHARE_TOKEN.
    """
    token = "d8339493f93227f3831389e97d8505bef834c036d09b0cd70cc4e6a8"
    ts.set_token(token)
    return ts.pro_api()


def get_default_date_range(years: int = 18) -> tuple[str, str]:
    """Return (start_date, end_date) as YYYYMMDD covering the last `years` years."""
    end_dt = datetime.today()
    start_dt = end_dt - timedelta(days=365 * years)
    return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")


def get_env_date_range() -> tuple[Optional[str], Optional[str]]:
    """Read START_DATE and END_DATE from environment if provided."""
    start_date = os.environ.get("START_DATE")
    end_date = os.environ.get("END_DATE")
    return start_date, end_date


def get_rds_config() -> dict:
    """User-editable RDS config inside code.

    请修改下面的占位值为你的真实连接信息：
      host, port, user, password, database, table, db_type, params
    若保持为 CHANGE_ME 则会自动回退到环境变量读取模式。
    """
    return {
        "host": "rm-bp18fm5u5c7uk47558o.mysql.rds.aliyuncs.com",
        "port": 3306,
        "user": "ch17394940726",
        "password": "Ch823147833",
        "database": "fe-journey",
        "table": "invest_finace_balance_stock_value",
        "db_type": "mysql",
        "params": "charset=utf8mb4",
        # 写入模式：replace_full（整表替换）或 upsert（按主键更新）
        "write_mode": "replace_full",
    }


def get_margin_mv_data(
    pro: ts.pro_api,
    exchange: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch margin balance and float market cap for a given exchange.

    exchange: 'SSE' or 'SZSE'
    Returns columns: trade_date (datetime64[ns]), rzye, float_mv
    """
    # 融资余额
    df_margin = pro.margin(exchange_id=exchange, start_date=start_date, end_date=end_date)
    df_margin = df_margin[["trade_date", "rzye"]].copy()
    df_margin["trade_date"] = pd.to_datetime(df_margin["trade_date"])
    df_margin.sort_values("trade_date", inplace=True)

    # 市值（按指数口径的流通市值）
    ts_code = "000001.SH" if exchange == "SSE" else "399001.SZ"
    df_mv = pro.index_dailybasic(ts_code=ts_code, start_date=start_date, end_date=end_date)
    df_mv = df_mv[["trade_date", "float_mv"]].copy()
    df_mv["trade_date"] = pd.to_datetime(df_mv["trade_date"])
    df_mv.sort_values("trade_date", inplace=True)

    # 严格按日期对齐（同日）
    df = pd.merge(df_margin, df_mv, on="trade_date", how="inner")
    df.dropna(inplace=True)
    return df


def get_both_exchanges_df(
    pro: ts.pro_api,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch SSE and SZSE data and compute totals and ratio.

    Returns columns:
      trade_date, rzye_sse, float_mv_sse, rzye_szse, float_mv_szse,
      rzye_total, float_mv_total, ratio_total
    """
    df_sse_raw = get_margin_mv_data(pro, "SSE", start_date, end_date).rename(
        columns={"rzye": "rzye_sse", "float_mv": "float_mv_sse"}
    )
    df_szse_raw = get_margin_mv_data(pro, "SZSE", start_date, end_date).rename(
        columns={"rzye": "rzye_szse", "float_mv": "float_mv_szse"}
    )

    df = pd.merge(df_sse_raw, df_szse_raw, on="trade_date", how="inner")
    df["rzye_total"] = df[["rzye_sse", "rzye_szse"]].sum(axis=1, skipna=True)
    df["float_mv_total"] = df[["float_mv_sse", "float_mv_szse"]].sum(axis=1, skipna=True)
    df["ratio_total"] = df["rzye_total"] / df["float_mv_total"]
    df.sort_values("trade_date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[
        [
            "trade_date",
            "rzye_sse",
            "float_mv_sse",
            "rzye_szse",
            "float_mv_szse",
            "rzye_total",
            "float_mv_total",
            "ratio_total",
        ]
    ]


def build_rds_engine_from_env() -> Engine:
    """Build SQLAlchemy engine from env vars for Aliyun RDS.

    Required (MySQL default): RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB
    Optional: RDS_PORT (default 3306), DB_TYPE (default mysql), RDS_PARAMS
    """
    db_type = os.environ.get("DB_TYPE", "mysql").lower()
    host = os.environ.get("RDS_HOST")
    user = os.environ.get("RDS_USER")
    password = os.environ.get("RDS_PASSWORD")
    database = os.environ.get("RDS_DB")
    port = os.environ.get("RDS_PORT", "3306")
    extra = os.environ.get("RDS_PARAMS", "charset=utf8mb4")

    if not all([host, user, password, database]):
        raise RuntimeError("Missing RDS config. Need RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB")

    if db_type == "mysql":
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?{extra}"
    else:
        # Allow other dialects if needed by user env (e.g., postgresql)
        url = os.environ.get("RDS_URL")
        if not url:
            raise RuntimeError("Non-MySQL DB_TYPE requires RDS_URL env for SQLAlchemy")

    engine = create_engine(url, pool_pre_ping=True)
    return engine


def _config_is_filled(cfg: dict) -> bool:
    required = ["host", "user", "password", "database"]
    for key in required:
        val = cfg.get(key)
        if val is None:
            return False
        sval = str(val).strip()
        if not sval or sval.upper().startswith("CHANGE_ME"):
            return False
    return True


def build_rds_engine_from_config(cfg: dict) -> Engine:
    db_type = str(cfg.get("db_type", "mysql")).lower()
    host = str(cfg["host"]).strip()
    user = str(cfg["user"]).strip()
    password = str(cfg["password"]).strip()
    database = str(cfg["database"]).strip()
    port = int(cfg.get("port", 3306))
    extra = str(cfg.get("params", "charset=utf8mb4")).strip()

    if db_type == "mysql":
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?{extra}"
    else:
        url = cfg.get("url")
        if not url:
            raise RuntimeError("Non-MySQL config requires 'url' in config dict")

    engine = create_engine(url, pool_pre_ping=True)
    return engine


def ensure_mysql_table(engine: Engine, table_name: str) -> None:
    """Create table if not exists with suitable schema for our dataset."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      `trade_date` date NOT NULL,
      `rzye_sse` DECIMAL(20,4) NULL,
      `float_mv_sse` DECIMAL(20,4) NULL,
      `rzye_szse` DECIMAL(20,4) NULL,
      `float_mv_szse` DECIMAL(20,4) NULL,
      `rzye_total` DECIMAL(20,4) NULL,
      `float_mv_total` DECIMAL(20,4) NULL,
      `ratio_total` DECIMAL(18,10) NULL,
      PRIMARY KEY (`trade_date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _reflect_mysql_table(engine: Engine, table_name: str) -> Table:
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    return table


def mysql_upsert_dataframe(engine: Engine, df: pd.DataFrame, table_name: str, chunk_size: int = 1000) -> None:
    """Upsert dataframe into MySQL using trade_date as PK."""
    if df.empty:
        print("[RDS] DataFrame is empty, skip upsert.")
        return

    # Ensure date type for key
    df_to_write = df.copy()
    df_to_write["trade_date"] = pd.to_datetime(df_to_write["trade_date"]).dt.date

    table = _reflect_mysql_table(engine, table_name)
    cols = [c.name for c in table.columns]
    cols_to_use = [c for c in df_to_write.columns if c in cols]

    # Chunked upsert
    with engine.begin() as conn:
        for start in range(0, len(df_to_write), chunk_size):
            chunk = df_to_write.iloc[start:start + chunk_size]
            records = chunk[cols_to_use].to_dict(orient="records")
            if not records:
                continue
            stmt = mysql_insert(table).values(records)
            update_map = {col: stmt.inserted[col] for col in cols_to_use if col != "trade_date"}
            upsert_stmt = stmt.on_duplicate_key_update(**update_map)
            conn.execute(upsert_stmt)


def mysql_replace_full_dataframe(engine: Engine, df: pd.DataFrame, table_name: str, chunk_size: int = 2000) -> None:
    """Replace whole table content using TRUNCATE + bulk insert.

    Use when each run produces full snapshot data.
    """
    if df.empty:
        print("[RDS] DataFrame is empty, skip replace_full.")
        return

    df_to_write = df.copy()
    df_to_write["trade_date"] = pd.to_datetime(df_to_write["trade_date"]).dt.date

    # Clear table then append all data
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE `{table_name}`"))

    df_to_write.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunk_size)

def main() -> None:
    # Resolve date range
    env_start, env_end = get_env_date_range()
    if env_start and env_end:
        start_date, end_date = env_start, env_end
    else:
        start_date, end_date = get_default_date_range(years=18)

    # Init tushare
    pro = setup_tushare()

    # Fetch and summarize
    df = get_both_exchanges_df(pro, start_date, end_date)

    # Basic preview
    print("Data shape:", df.shape)
    print("Columns:", list(df.columns))
    print("\nHead:")
    print(df.head(10).to_string(index=False))

    # Persist to RDS if configured (prefer in-code config, fallback to env)
    engine = None
    table_name = "margin_mv_daily"
    db_type = "mysql"

    cfg = get_rds_config()
    if _config_is_filled(cfg):
        try:
            engine = build_rds_engine_from_config(cfg)
            table_name = str(cfg.get("table", table_name))
            db_type = str(cfg.get("db_type", db_type)).lower()
            print("[RDS] Using in-code config.")
        except Exception as e:
            print(f"[RDS] In-code config invalid, fallback to env. Reason: {e}")

    if engine is None:
        try:
            engine = build_rds_engine_from_env()
            table_name = os.environ.get("RDS_TABLE", table_name)
            db_type = os.environ.get("DB_TYPE", db_type).lower()
            print("[RDS] Using environment config.")
        except Exception as e:
            print(f"[RDS] Skip writing (engine not configured): {e}")

    if engine is not None:
        # 选择写入模式：优先使用代码内配置的 write_mode，其次环境变量 WRITE_MODE
        write_mode = "replace_full"
        if _config_is_filled(cfg):
            write_mode = str(cfg.get("write_mode", write_mode)).lower()
        write_mode = os.environ.get("WRITE_MODE", write_mode).lower()

        if db_type == "mysql":
            ensure_mysql_table(engine, table_name)
            if write_mode == "replace_full":
                mysql_replace_full_dataframe(engine, df, table_name)
                print(f"[RDS] Replace-full completed into table: {table_name}")
            else:
                mysql_upsert_dataframe(engine, df, table_name)
                print(f"[RDS] Upsert completed into table: {table_name}")
        else:
            # Fallback for non-MySQL: simple append (no upsert)
            print(f"[RDS] Non-MySQL engine detected: {db_type}. Using append.")
            df_to_persist = df.copy()
            df_to_persist["trade_date"] = pd.to_datetime(df_to_persist["trade_date"]).dt.date
            df_to_persist.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"[RDS] Append completed into table: {table_name}")


if __name__ == "__main__":
    main()

 

