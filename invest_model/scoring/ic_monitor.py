"""因子 IC 监控模块。

计算每个因子信号与未来 N 日收益的 Spearman Rank IC，
支持滚动窗口计算、因子有效性诊断和权重建议。

典型用法::

    from invest_model.scoring.ic_monitor import ICMonitor
    monitor = ICMonitor(engine)
    report = monitor.compute_rolling_ic(
        codes, start_date="20250601", end_date="20260430",
        forward_days=5, window=60,
    )
    print(report.summary())
    suggested = report.suggest_weights()
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger

logger = get_logger()


@dataclass
class ICReport:
    """IC 计算结果报告。"""

    factor_ic: pd.DataFrame
    rolling_ic: pd.DataFrame | None = None
    forward_days: int = 5

    def summary(self) -> pd.DataFrame:
        """每个因子的平均 IC、IC 标准差、IC_IR (IC均值/IC标准差)、正IC占比。"""
        if self.rolling_ic is None or self.rolling_ic.empty:
            return self.factor_ic

        factors = [c for c in self.rolling_ic.columns if c != "trade_date"]
        rows = []
        for f in factors:
            s = self.rolling_ic[f].dropna()
            if len(s) < 3:
                rows.append({"factor": f, "mean_ic": np.nan, "std_ic": np.nan,
                             "ic_ir": np.nan, "positive_pct": np.nan, "n_obs": len(s)})
                continue
            mean_ic = s.mean()
            std_ic = s.std()
            ic_ir = mean_ic / std_ic if std_ic > 0 else 0.0
            pos_pct = (s > 0).mean()
            rows.append({
                "factor": f,
                "mean_ic": round(mean_ic, 4),
                "std_ic": round(std_ic, 4),
                "ic_ir": round(ic_ir, 4),
                "positive_pct": round(pos_pct, 4),
                "n_obs": len(s),
            })
        return pd.DataFrame(rows).sort_values("ic_ir", ascending=False, key=abs)

    def suggest_weights(self) -> dict[str, float]:
        """根据 IC 正负和 IC_IR 建议类别权重。

        规则:
        - mean_ic < 0 的因子类别权重设为 0（反向信号不如不用）
        - mean_ic > 0 的按 |IC_IR| 归一化分配权重
        """
        from invest_model.scoring.scorer import _SIGNAL_TO_CATEGORY, DEFAULT_CATEGORY_WEIGHTS

        summary = self.summary()
        if summary.empty:
            return dict(DEFAULT_CATEGORY_WEIGHTS)

        cat_ics: dict[str, list[float]] = {}
        for _, row in summary.iterrows():
            factor = row["factor"]
            cat = _SIGNAL_TO_CATEGORY.get(factor)
            if cat is None:
                continue
            if pd.notna(row["ic_ir"]):
                cat_ics.setdefault(cat, []).append(row["ic_ir"])

        cat_mean_ir: dict[str, float] = {}
        for cat, irs in cat_ics.items():
            cat_mean_ir[cat] = np.mean(irs)

        weights = dict(DEFAULT_CATEGORY_WEIGHTS)
        positive_cats = {c: ir for c, ir in cat_mean_ir.items() if ir > 0}
        total_ir = sum(abs(v) for v in positive_cats.values()) if positive_cats else 1.0

        for cat in weights:
            if cat in positive_cats and total_ir > 0:
                weights[cat] = round(abs(positive_cats[cat]) / total_ir, 3)
            elif cat in cat_mean_ir and cat_mean_ir[cat] <= 0:
                weights[cat] = 0.0

        return weights


class ICMonitor:
    """因子 IC 计算器。"""

    def __init__(self, engine: Engine):
        self.engine = engine

    def compute_factor_ic(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        forward_days: int = 5,
    ) -> ICReport:
        """计算每个因子与未来 forward_days 日收益的截面 Spearman IC。

        Returns ICReport，其中 factor_ic 为每个交易日 × 每个因子的 IC 矩阵。
        """
        from invest_model.repositories.base import BaseRepository

        base = BaseRepository(self.engine)
        if not codes:
            return ICReport(factor_ic=pd.DataFrame(), forward_days=forward_days)

        placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
        code_params = {f"c{i}": c for i, c in enumerate(codes)}

        # 1. 加载信号快照
        signals_df = base.read_sql(
            f"""
            SELECT code, trade_date, signal_name, score
            FROM stock_signal_snapshot
            WHERE code IN ({placeholders})
              AND trade_date BETWEEN :start AND :end
            ORDER BY trade_date, code
            """,
            {**code_params, "start": start_date, "end": end_date},
        )
        if signals_df.empty:
            logger.warning("IC 计算：无信号快照数据")
            return ICReport(factor_ic=pd.DataFrame(), forward_days=forward_days)

        # 2. 加载日线收益（需要 trade_date + forward_days 之后的数据）
        returns_df = base.read_sql(
            f"""
            SELECT code, trade_date, close
            FROM stock_daily
            WHERE code IN ({placeholders})
              AND trade_date >= :start
            ORDER BY code, trade_date
            """,
            {**code_params, "start": start_date},
        )
        if returns_df.empty:
            logger.warning("IC 计算：无日线数据")
            return ICReport(factor_ic=pd.DataFrame(), forward_days=forward_days)

        returns_df["close"] = pd.to_numeric(returns_df["close"], errors="coerce")
        fwd_returns = self._compute_forward_returns(returns_df, forward_days)

        # 3. Pivot 信号表：行 = (code, trade_date)，列 = signal_name
        signals_df["score"] = pd.to_numeric(signals_df["score"], errors="coerce")
        pivot = signals_df.pivot_table(
            index=["code", "trade_date"],
            columns="signal_name",
            values="score",
            aggfunc="first",
        )
        pivot = pivot.reset_index()

        # 4. Merge
        merged = pivot.merge(fwd_returns, on=["code", "trade_date"], how="inner")
        if merged.empty:
            return ICReport(factor_ic=pd.DataFrame(), forward_days=forward_days)

        # 5. 逐日截面 IC
        factor_names = [c for c in pivot.columns if c not in ("code", "trade_date")]
        dates = sorted(merged["trade_date"].unique())
        ic_rows = []
        for td in dates:
            sub = merged[merged["trade_date"] == td]
            if len(sub) < 3:
                continue
            row = {"trade_date": td}
            for f in factor_names:
                vals = sub[f].dropna()
                rets = sub.loc[vals.index, "fwd_return"]
                if len(vals) < 3 or vals.std() == 0:
                    row[f] = np.nan
                    continue
                ic, _ = spearmanr(vals, rets)
                row[f] = ic
            ic_rows.append(row)

        ic_df = pd.DataFrame(ic_rows)
        return ICReport(factor_ic=ic_df, rolling_ic=ic_df, forward_days=forward_days)

    def compute_rolling_ic(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        forward_days: int = 5,
        window: int = 60,
    ) -> ICReport:
        """计算滚动窗口 IC。

        先算逐日截面 IC，再对每个因子做 rolling mean(window)。
        """
        report = self.compute_factor_ic(codes, start_date, end_date, forward_days)
        if report.factor_ic.empty:
            return report

        factor_cols = [c for c in report.factor_ic.columns if c != "trade_date"]
        rolling_df = report.factor_ic[["trade_date"]].copy()
        for f in factor_cols:
            rolling_df[f] = report.factor_ic[f].rolling(window, min_periods=max(window // 3, 5)).mean()

        report.rolling_ic = rolling_df
        return report

    def check_health(
        self,
        codes: list[str],
        version: str = "v1_oos",
        window: int = 30,
        ic_warn_threshold: float = 0.03,
    ) -> dict[str, str]:
        """检查模型健康状态，返回 {code: "ok" | "degraded" | "missing"}。

        从 ml_model_registry 读取 cv_avg_ic；低于 ic_warn_threshold 视为 degraded。
        window 参数保留作未来接入滚动 OOS IC 时使用。
        """
        from invest_model.ml.persistence import list_registry

        result: dict[str, str] = {}
        if not codes:
            return result

        try:
            registry = list_registry(self.engine, codes=codes, version=version)
        except Exception as e:
            logger.warning(f"[IC监控] 读取模型注册表失败: {e}")
            return {code: "missing" for code in codes}

        for code in codes:
            if registry.empty:
                result[code] = "missing"
                continue
            rows = registry[registry["code"] == code]
            if rows.empty:
                result[code] = "missing"
                continue
            avg_ic = float(rows["cv_avg_ic"].mean())
            result[code] = "ok" if avg_ic >= ic_warn_threshold else "degraded"

        return result

    @staticmethod
    def _compute_forward_returns(
        daily_df: pd.DataFrame, forward_days: int
    ) -> pd.DataFrame:
        """计算每只股票每日的 T+N 收益率。"""
        result_rows = []
        for code, group in daily_df.groupby("code"):
            g = group.sort_values("trade_date").reset_index(drop=True)
            g["fwd_close"] = g["close"].shift(-forward_days)
            g["fwd_return"] = (g["fwd_close"] / g["close"] - 1.0).where(g["close"] > 0)
            valid = g[["code", "trade_date", "fwd_return"]].dropna()
            result_rows.append(valid)

        if not result_rows:
            return pd.DataFrame(columns=["code", "trade_date", "fwd_return"])
        return pd.concat(result_rows, ignore_index=True)
