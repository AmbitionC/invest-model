"""股票池 Repository"""

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class StockPoolRepository(BaseRepository):
    """股票池管理：增删改查 + 股票/ETF 基础信息"""

    # ── stock_info / etf_info ──

    def save_stock_info(self, df: pd.DataFrame) -> int:
        return self.upsert("stock_info", df, unique_keys=["ts_code"])

    def save_etf_info(self, df: pd.DataFrame) -> int:
        return self.upsert("etf_info", df, unique_keys=["ts_code"])

    def get_all_stocks(self) -> pd.DataFrame:
        return self.read_sql("SELECT * FROM stock_info ORDER BY ts_code")

    def get_all_etfs(self) -> pd.DataFrame:
        return self.read_sql("SELECT * FROM etf_info ORDER BY ts_code")

    # ── stock_pool ──

    def add_to_pool(self, code: str, name: str | None = None,
                    pool_group: str = "default", tags: str | None = None,
                    notes: str | None = None) -> None:
        """添加单只股票到池"""
        df = pd.DataFrame([{
            "code": code, "name": name, "pool_group": pool_group,
            "tags": tags, "notes": notes,
        }])
        self.upsert("stock_pool", df, unique_keys=["code", "pool_group"])
        logger.info(f"股票池添加: {code} -> {pool_group}")

    def batch_add_to_pool(self, df: pd.DataFrame) -> int:
        """批量添加到股票池，df 需含 code 列，可选 name/pool_group/tags/notes"""
        if "pool_group" not in df.columns:
            df = df.copy()
            df["pool_group"] = "default"
        return self.upsert("stock_pool", df, unique_keys=["code", "pool_group"])

    def remove_from_pool(self, code: str, pool_group: str = "default") -> None:
        self.execute_sql(
            "DELETE FROM stock_pool WHERE code = :code AND pool_group = :grp",
            {"code": code, "grp": pool_group},
        )
        logger.info(f"股票池移除: {code} from {pool_group}")

    def get_pool(self, pool_group: str | None = None) -> pd.DataFrame:
        """获取股票池列表"""
        if pool_group:
            return self.read_sql(
                "SELECT * FROM stock_pool WHERE pool_group = :grp ORDER BY code",
                {"grp": pool_group},
            )
        return self.read_sql("SELECT * FROM stock_pool ORDER BY pool_group, code")

    def get_pool_codes(self, pool_group: str | None = None) -> list[str]:
        """获取股票池中的代码列表"""
        df = self.get_pool(pool_group)
        return df["code"].tolist()

    def get_pool_groups(self) -> list[str]:
        """获取所有分组名"""
        df = self.read_sql("SELECT DISTINCT pool_group FROM stock_pool ORDER BY pool_group")
        return df["pool_group"].tolist()
