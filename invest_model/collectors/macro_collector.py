"""宏观经济数据采集器"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from invest_model.collectors.base import BaseCollector, logger
from invest_model.repositories.base import BaseRepository


class MacroCollector(BaseCollector):
    """
    采集宏观经济数据：
    1. macro_money_supply  — M0/M1/M2 货币供应量（月度）
    2. macro_pmi           — PMI 制造业/非制造业/综合（月度）
    3. macro_cpi_ppi       — CPI / PPI（月度）
    4. macro_interest_rate — LPR 贷款市场报价利率（日度，变动时才有数据）
    5. northbound_top10    — 沪深港通十大成交股（日度）
    """

    def _repo(self) -> BaseRepository:
        return BaseRepository(self.engine)

    # ── M0/M1/M2 货币供应量 ──────────────────────────────

    def collect_money_supply(self, start_m: str, end_m: str) -> int:
        """拉取指定月份区间的 M0/M1/M2 数据并写入 macro_money_supply。

        Parameters
        ----------
        start_m / end_m : YYYYMM 格式，如 "202101"
        """
        repo = self._repo()
        try:
            df = self.source.get_money_supply(start_m, end_m)
        except Exception as e:
            logger.error(f"货币供应量拉取失败 {start_m}~{end_m}: {e}")
            return 0

        if df is None or df.empty:
            logger.info(f"货币供应量无新数据: {start_m}~{end_m}")
            return 0

        df = df.rename(columns={"month": "period_month"}) if "month" in df.columns else df
        cols_keep = [c for c in ["period_month", "m0", "m1", "m2",
                                  "m0_yoy", "m1_yoy", "m2_yoy",
                                  "m0_mom", "m1_mom", "m2_mom"] if c in df.columns]
        df = df[cols_keep].copy()
        n = repo.upsert("macro_money_supply", df, unique_keys=["period_month"])
        logger.info(f"货币供应量写入 {n} 条")
        return n

    def collect_money_supply_incremental(self) -> int:
        """增量采集：从上次最新月份到当月"""
        repo = self._repo()
        latest_df = repo.read_sql("SELECT MAX(period_month) AS m FROM macro_money_supply")
        latest = latest_df["m"].iloc[0] if not latest_df.empty and latest_df["m"].iloc[0] else None

        now = datetime.now()
        end_m = now.strftime("%Y%m")
        if latest is None:
            start_dt = now - timedelta(days=365 * 6)
            start_m = start_dt.strftime("%Y%m")
        else:
            start_m = latest

        return self.collect_money_supply(start_m, end_m)

    # ── PMI ──────────────────────────────────────────────

    def collect_pmi(self, start_m: str, end_m: str) -> int:
        """PMI 月度数据"""
        repo = self._repo()
        try:
            df = self.source.get_pmi(start_m, end_m)
        except Exception as e:
            logger.error(f"PMI 拉取失败 {start_m}~{end_m}: {e}")
            return 0

        if df is None or df.empty:
            logger.info(f"PMI 无新数据: {start_m}~{end_m}")
            return 0

        df = df.rename(columns={"month": "period_month"}) if "month" in df.columns else df
        cols_keep = [c for c in ["period_month", "pmi_mfg", "pmi_service", "pmi_composite"]
                     if c in df.columns]
        if not cols_keep or "period_month" not in cols_keep:
            # 尝试兼容 Tushare 返回字段名
            rename_map = {}
            for col in df.columns:
                if "month" in col.lower():
                    rename_map[col] = "period_month"
            df = df.rename(columns=rename_map)
            cols_keep = [c for c in df.columns if c in
                         ["period_month", "pmi_mfg", "pmi_service", "pmi_composite",
                          "pmi010", "pmi020", "pmi009"]]

        df = df[cols_keep].copy()
        # 规范化为标准列名
        col_map = {"pmi010": "pmi_mfg", "pmi020": "pmi_service", "pmi009": "pmi_composite"}
        df = df.rename(columns=col_map)

        n = repo.upsert("macro_pmi", df, unique_keys=["period_month"])
        logger.info(f"PMI 写入 {n} 条")
        return n

    def collect_pmi_incremental(self) -> int:
        repo = self._repo()
        latest_df = repo.read_sql("SELECT MAX(period_month) AS m FROM macro_pmi")
        latest = latest_df["m"].iloc[0] if not latest_df.empty and latest_df["m"].iloc[0] else None

        now = datetime.now()
        end_m = now.strftime("%Y%m")
        if latest is None:
            start_dt = now - timedelta(days=365 * 6)
            start_m = start_dt.strftime("%Y%m")
        else:
            start_m = latest

        return self.collect_pmi(start_m, end_m)

    # ── CPI / PPI ─────────────────────────────────────────

    def collect_cpi_ppi(self, start_m: str, end_m: str) -> int:
        """CPI + PPI 月度数据，合并写入 macro_cpi_ppi"""
        repo = self._repo()
        rows: dict[str, dict] = {}

        try:
            cpi_df = self.source.get_cpi(start_m, end_m)
            if cpi_df is not None and not cpi_df.empty:
                cpi_df = cpi_df.rename(columns={"month": "period_month"}) \
                    if "month" in cpi_df.columns else cpi_df
                for _, r in cpi_df.iterrows():
                    pm = str(r.get("period_month", ""))
                    if pm:
                        rows.setdefault(pm, {"period_month": pm})
                        for col in ["nt_val", "nt_yoy", "nt_mom",
                                    "town_val", "town_yoy", "town_mom",
                                    "cnt_val", "cnt_yoy", "cnt_mom"]:
                            if col in r.index:
                                rows[pm][f"cpi_{col}"] = r[col]
        except Exception as e:
            logger.warning(f"CPI 拉取失败: {e}")

        try:
            ppi_df = self.source.get_ppi(start_m, end_m)
            if ppi_df is not None and not ppi_df.empty:
                ppi_df = ppi_df.rename(columns={"month": "period_month"}) \
                    if "month" in ppi_df.columns else ppi_df
                for _, r in ppi_df.iterrows():
                    pm = str(r.get("period_month", ""))
                    if pm:
                        rows.setdefault(pm, {"period_month": pm})
                        for col in ["ppi_yoy", "ppi_mp_yoy", "ppi_mp_qm_yoy",
                                    "ppi_mp_rm_yoy", "ppi_mp_p_yoy", "ppi_cg_yoy",
                                    "ppi_cg_f_yoy", "ppi_cg_c_yoy"]:
                            if col in r.index:
                                rows[pm][col] = r[col]
        except Exception as e:
            logger.warning(f"PPI 拉取失败: {e}")

        if not rows:
            logger.info(f"CPI/PPI 无新数据: {start_m}~{end_m}")
            return 0

        df = pd.DataFrame(list(rows.values()))
        n = repo.upsert("macro_cpi_ppi", df, unique_keys=["period_month"])
        logger.info(f"CPI/PPI 写入 {n} 条")
        return n

    def collect_cpi_ppi_incremental(self) -> int:
        repo = self._repo()
        latest_df = repo.read_sql("SELECT MAX(period_month) AS m FROM macro_cpi_ppi")
        latest = latest_df["m"].iloc[0] if not latest_df.empty and latest_df["m"].iloc[0] else None

        now = datetime.now()
        end_m = now.strftime("%Y%m")
        if latest is None:
            start_dt = now - timedelta(days=365 * 6)
            start_m = start_dt.strftime("%Y%m")
        else:
            start_m = latest

        return self.collect_cpi_ppi(start_m, end_m)

    # ── LPR ──────────────────────────────────────────────

    def collect_lpr(self, start_date: str, end_date: str) -> int:
        """LPR 利率（日度，仅变动时有数据，通常每月15日前后）"""
        repo = self._repo()
        try:
            df = self.source.get_lpr(start_date, end_date)
        except Exception as e:
            logger.error(f"LPR 拉取失败 {start_date}~{end_date}: {e}")
            return 0

        if df is None or df.empty:
            logger.info(f"LPR 无新数据: {start_date}~{end_date}")
            return 0

        # 规范化列名：Tushare 返回 date/lpr1y/lpr5y 或 trade_date 等
        rename = {}
        for col in df.columns:
            if col.lower() in ("date", "ann_date", "trade_date"):
                rename[col] = "pub_date"
            elif col.lower() in ("lpr1y", "lpr_1y", "1y", "one_year"):
                rename[col] = "lpr_1y"
            elif col.lower() in ("lpr5y", "lpr_5y", "5y", "five_year"):
                rename[col] = "lpr_5y"
        df = df.rename(columns=rename)

        cols_keep = [c for c in ["pub_date", "lpr_1y", "lpr_5y"] if c in df.columns]
        df = df[cols_keep].copy()
        if "pub_date" not in df.columns:
            logger.warning("LPR 数据无日期列，跳过")
            return 0

        n = repo.upsert("macro_interest_rate", df, unique_keys=["pub_date"])
        logger.info(f"LPR 写入 {n} 条")
        return n

    def collect_lpr_incremental(self) -> int:
        repo = self._repo()
        latest_df = repo.read_sql("SELECT MAX(pub_date) AS d FROM macro_interest_rate")
        latest = latest_df["d"].iloc[0] if not latest_df.empty and latest_df["d"].iloc[0] else None

        end_date = datetime.now().strftime("%Y%m%d")
        if latest is None:
            start_dt = datetime.now() - timedelta(days=365 * 6)
            start_date = start_dt.strftime("%Y%m%d")
        else:
            start_date = latest

        return self.collect_lpr(start_date, end_date)

    # ── 北向 Top10 ────────────────────────────────────────

    def collect_northbound_top10(self, trade_dates: list[str]) -> int:
        """按交易日拉取沪深港通十大成交股（沪股通+深股通）"""
        repo = self._repo()
        total = 0

        for i, td in enumerate(trade_dates, 1):
            for market_type in ("1", "3"):  # 1=沪股通 3=深股通
                try:
                    df = self.source.get_hsgt_top10(td, market_type=market_type)
                    if df is not None and not df.empty:
                        n = repo.upsert(
                            "northbound_top10", df,
                            unique_keys=["trade_date", "market_type", "code"],
                        )
                        total += n
                except Exception as e:
                    logger.warning(f"北向Top10 {td} market={market_type} 失败: {e}")

            if i % 20 == 0:
                self._log_progress(i, len(trade_dates), "北向Top10")

        logger.info(f"北向Top10 采集完成: {len(trade_dates)} 天, 共 {total} 条")
        return total

    def collect_northbound_top10_incremental(self, recent_days: int = 30) -> int:
        """增量采集北向Top10：补齐最近 recent_days 个交易日的缺失数据"""
        from invest_model.repositories.calendar_repo import CalendarRepository

        cal_repo = CalendarRepository(self.engine)
        repo = self._repo()

        end_date = datetime.now().strftime("%Y%m%d")
        start_dt = datetime.now() - timedelta(days=recent_days * 2 + 10)
        start_date = start_dt.strftime("%Y%m%d")

        all_dates = cal_repo.get_trade_dates(start_date, end_date)
        if not all_dates:
            return 0
        recent = all_dates[-recent_days:]

        existing_df = repo.read_sql(
            "SELECT DISTINCT trade_date FROM northbound_top10 WHERE trade_date >= :s",
            {"s": recent[0]},
        )
        existing = set(existing_df["trade_date"].tolist())
        missing = [d for d in recent if d not in existing]

        if not missing:
            logger.info("北向Top10 已是最新")
            return 0

        logger.info(f"北向Top10 缺失 {len(missing)} 天，开始增量采集")
        return self.collect_northbound_top10(missing)

    # ── 统一增量入口 ──────────────────────────────────────

    def collect_all_incremental(self) -> dict[str, int]:
        """一键增量更新全部宏观数据"""
        results = {}
        results["money_supply"] = self.collect_money_supply_incremental()
        results["pmi"] = self.collect_pmi_incremental()
        results["cpi_ppi"] = self.collect_cpi_ppi_incremental()
        results["lpr"] = self.collect_lpr_incremental()
        results["northbound_top10"] = self.collect_northbound_top10_incremental()
        total = sum(results.values())
        logger.info(f"宏观数据增量采集完成，合计 {total} 条: {results}")
        return results
