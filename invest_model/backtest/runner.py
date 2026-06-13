"""BacktestRunner：基于 stock_advisor_signal 的组合回测引擎。

信号来源：stock_advisor_signal（已计算好的逐票操作建议）
价格来源：stock_daily + etf_daily（收盘价）
换仓逻辑：每 rebalance_days 个交易日换仓一次
仓位逻辑：bullish 均等权重 / bearish 清空，可按 top_k 筛选
成本模型：单边手续费 commission + 滑点 slippage
结果落库：backtest_run / backtest_nav / backtest_trades
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

_BULLISH_ACTIONS = {"strong_buy", "buy"}
_BEARISH_ACTIONS = {"reduce", "clear"}


class BacktestRunner:
    """基于 advisor 信号的简单组合回测引擎。"""

    def __init__(
        self,
        engine: Engine,
        commission: float = 0.001,   # 单边手续费（含印花税）
        slippage: float = 0.0005,    # 滑点（单边）
    ):
        self.engine = engine
        self.commission = commission
        self.slippage = slippage
        self._repo = BaseRepository(engine)

    # ── 主入口 ──────────────────────────────────────────────

    def run(
        self,
        name: str,
        codes: list[str],
        start_date: str,
        end_date: str,
        rebalance_days: int = 5,
        top_k: int | None = None,
        strategy: str = "advisor_signal",
    ) -> int:
        """执行一次回测，返回 run_id。

        Parameters
        ----------
        name          回测名称（唯一标识）
        codes         参与回测的股票/ETF 代码列表
        start_date    回测开始日期 YYYYMMDD
        end_date      回测结束日期 YYYYMMDD
        rebalance_days 换仓周期（交易日数，默认5）
        top_k         只取信号最强的 top_k 只建仓；None 表示全部 bullish
        strategy      策略名称标签
        """
        logger.info(f"[回测] 开始 name={name} codes={codes} {start_date}~{end_date} "
                    f"rebal={rebalance_days} top_k={top_k}")

        trading_days = self._get_trading_days(start_date, end_date)
        if not trading_days:
            raise ValueError(f"trade_calendar 中 {start_date}~{end_date} 无交易日")

        signals_df = self._load_signals(codes, start_date, end_date)
        prices_df = self._load_prices(codes, start_date, end_date)

        if signals_df.empty:
            raise ValueError("stock_advisor_signal 无该 codes+日期区间的记录，请先运行信号回填")
        if prices_df.empty:
            raise ValueError("stock_daily/etf_daily 无价格数据，请先采集数据")

        nav_records, trade_records, metrics = self._simulate(
            trading_days, signals_df, prices_df, rebalance_days, top_k
        )

        run_id = self._save_run(
            name, strategy, codes, start_date, end_date,
            rebalance_days, top_k, metrics
        )
        self._save_nav(run_id, nav_records)
        self._save_trades(run_id, trade_records)

        logger.info(
            f"[回测] 完成 run_id={run_id} | "
            f"CAGR={metrics.get('cagr', 0):.2%} "
            f"MaxDD={metrics.get('max_drawdown', 0):.2%} "
            f"Sharpe={metrics.get('sharpe', 0):.2f}"
        )
        return run_id

    def get_report(self, run_id: int) -> dict[str, Any]:
        """读取一次回测的汇总报告。"""
        run = self._repo.read_sql(
            "SELECT * FROM backtest_run WHERE run_id = :rid", {"rid": run_id}
        )
        if run.empty:
            raise ValueError(f"run_id={run_id} 不存在")
        metrics = json.loads(run.iloc[0]["metrics"] or "{}")
        nav = self._repo.read_sql(
            "SELECT * FROM backtest_nav WHERE run_id = :rid ORDER BY trade_date",
            {"rid": run_id},
        )
        return {
            "run_id": run_id,
            "name": run.iloc[0]["name"],
            "start_date": run.iloc[0]["start_date"],
            "end_date": run.iloc[0]["end_date"],
            "metrics": metrics,
            "nav": nav,
        }

    def print_report(self, run_id: int) -> None:
        """在终端打印回测报告。"""
        rpt = self.get_report(run_id)
        m = rpt["metrics"]
        print(f"\n{'='*55}")
        print(f"  回测报告: {rpt['name']}  (run_id={run_id})")
        print(f"  区间: {rpt['start_date']} ~ {rpt['end_date']}")
        print(f"{'='*55}")
        print(f"  总收益率:    {m.get('total_return', 0):>+8.2%}")
        print(f"  年化收益率:  {m.get('cagr', 0):>+8.2%}")
        print(f"  最大回撤:    {m.get('max_drawdown', 0):>8.2%}")
        print(f"  夏普比率:    {m.get('sharpe', 0):>8.2f}")
        print(f"  卡玛比率:    {m.get('calmar', 0):>8.2f}")
        print(f"  交易天数:    {m.get('trading_days', 0):>8d}")
        print(f"{'='*55}\n")

    # ── 模拟核心 ────────────────────────────────────────────

    def _simulate(
        self,
        trading_days: list[str],
        signals_df: pd.DataFrame,   # columns: trade_date, code, action, composite
        prices_df: pd.DataFrame,    # pivot: index=trade_date, columns=code, values=close
        rebalance_days: int,
        top_k: int | None,
    ) -> tuple[list[dict], list[dict], dict]:

        # 建立最近信号索引：每个交易日 → 最近已知信号（避免前视偏差）
        signal_pivot = self._build_signal_pivot(signals_df, trading_days)

        rebalance_set = set(trading_days[::rebalance_days])
        positions: dict[str, float] = {}   # code -> portfolio weight
        nav = 1.0
        prev_nav = 1.0
        nav_records: list[dict] = []
        trade_records: list[dict] = []

        for i, date in enumerate(trading_days):
            turnover = 0.0

            # 按前一日收盘价更新当日 NAV
            if i > 0:
                prev_date = trading_days[i - 1]
                daily_ret = 0.0
                for code, w in positions.items():
                    try:
                        p_cur = prices_df.loc[date, code] if date in prices_df.index else None
                        p_prv = prices_df.loc[prev_date, code] if prev_date in prices_df.index else None
                        if p_cur and p_prv and p_prv > 0 and not pd.isna(p_cur) and not pd.isna(p_prv):
                            daily_ret += w * (float(p_cur) / float(p_prv) - 1)
                    except (KeyError, TypeError):
                        pass
                nav *= (1 + daily_ret)

            # 换仓
            if date in rebalance_set:
                day_signals = signal_pivot.get(date, {})
                target = self._build_target_weights(day_signals, top_k)
                all_codes = set(target) | set(positions)
                turnover = sum(abs(target.get(c, 0) - positions.get(c, 0)) for c in all_codes)
                nav *= max(0, 1 - turnover * (self.commission + self.slippage))

                for code in all_codes:
                    t = target.get(code, 0)
                    p = positions.get(code, 0)
                    if abs(t - p) > 0.001:
                        try:
                            px = float(prices_df.loc[date, code]) if date in prices_df.index and code in prices_df.columns else None
                        except (KeyError, TypeError):
                            px = None
                        trade_records.append({
                            "trade_date": date,
                            "code": code,
                            "action": "buy" if t > p else "sell",
                            "weight": round(t, 6),
                            "price": round(px, 4) if px and not pd.isna(px) else None,
                        })
                positions = target

            ret = nav / prev_nav - 1 if i > 0 else 0.0
            prev_nav = nav
            nav_records.append({
                "trade_date": date,
                "nav": round(nav, 6),
                "ret": round(float(ret), 6),
                "turnover": round(float(turnover), 6),
                "position_count": sum(1 for w in positions.values() if w > 0.001),
            })

        nav_series = pd.Series(
            [r["nav"] for r in nav_records],
            index=pd.to_datetime(trading_days, format="%Y%m%d"),
        )
        metrics = self._calc_metrics(nav_series)
        return nav_records, trade_records, metrics

    def _build_signal_pivot(
        self,
        signals_df: pd.DataFrame,
        trading_days: list[str],
    ) -> dict[str, dict[str, str]]:
        """构建 {trade_date -> {code -> action}} 映射（向前填充，避免前视偏差）。"""
        if signals_df.empty:
            return {}
        pivot: dict[str, dict[str, str]] = {}
        all_signal_dates = sorted(signals_df["trade_date"].unique())
        signal_map: dict[str, dict[str, str]] = {}
        for d in all_signal_dates:
            day = signals_df[signals_df["trade_date"] == d]
            signal_map[d] = dict(zip(day["code"], day["action"]))

        last_known: dict[str, str] = {}
        for date in trading_days:
            if date in signal_map:
                last_known = signal_map[date]
            pivot[date] = dict(last_known)
        return pivot

    @staticmethod
    def _build_target_weights(
        day_signals: dict[str, str],
        top_k: int | None,
    ) -> dict[str, float]:
        """根据操作信号构建目标权重（等权 / top_k）。"""
        bullish = {
            code: (1.0 if action == "strong_buy" else 0.6)
            for code, action in day_signals.items()
            if action in _BULLISH_ACTIONS
        }
        if not bullish:
            return {}
        if top_k and len(bullish) > top_k:
            bullish = dict(sorted(bullish.items(), key=lambda x: x[1], reverse=True)[:top_k])
        total = sum(bullish.values())
        return {c: w / total for c, w in bullish.items()}

    @staticmethod
    def _calc_metrics(nav_series: pd.Series) -> dict:
        """计算标准回测指标。"""
        if len(nav_series) < 2:
            return {}
        years = len(nav_series) / 252
        final_nav = float(nav_series.iloc[-1])
        cagr = (final_nav ** (1 / years) - 1) if years > 0 else 0.0

        rolling_max = nav_series.expanding().max()
        drawdowns = nav_series / rolling_max - 1
        max_dd = float(drawdowns.min())

        daily_rets = nav_series.pct_change().dropna()
        rf_daily = 0.02 / 252
        excess = daily_rets - rf_daily
        sharpe = (float(excess.mean()) / float(excess.std()) * np.sqrt(252)
                  if float(excess.std()) > 0 else 0.0)
        calmar = cagr / abs(max_dd) if max_dd < 0 else 0.0

        win_mask = daily_rets > 0
        win_rate = float(win_mask.mean())
        avg_gain = float(daily_rets[win_mask].mean()) if win_mask.any() else 0.0
        avg_loss = float(daily_rets[~win_mask].mean()) if (~win_mask).any() else 0.0
        profit_factor = abs(avg_gain / avg_loss) if avg_loss != 0 else 0.0

        return {
            "total_return": round(final_nav - 1, 4),
            "cagr": round(cagr, 4),
            "max_drawdown": round(max_dd, 4),
            "sharpe": round(sharpe, 4),
            "calmar": round(calmar, 4),
            "win_rate": round(win_rate, 4),
            "profit_factor": round(profit_factor, 4),
            "trading_days": len(nav_series),
        }

    # ── 数据加载 ─────────────────────────────────────────────

    def _get_trading_days(self, start_date: str, end_date: str) -> list[str]:
        df = self._repo.read_sql(
            "SELECT cal_date FROM trade_calendar WHERE is_open=1 AND cal_date BETWEEN :s AND :e ORDER BY cal_date",
            {"s": start_date, "e": end_date},
        )
        if df.empty:
            return []
        return df["cal_date"].astype(str).tolist()

    def _load_signals(
        self, codes: list[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        if not codes:
            return pd.DataFrame()
        ph = ", ".join(f":c{i}" for i in range(len(codes)))
        params: dict = {f"c{i}": c for i, c in enumerate(codes)}
        params.update({"s": start_date, "e": end_date})
        df = self._repo.read_sql(
            f"""
            SELECT code, trade_date, action, composite
            FROM stock_advisor_signal
            WHERE code IN ({ph}) AND trade_date BETWEEN :s AND :e
            ORDER BY trade_date, code
            """,
            params,
        )
        df["trade_date"] = df["trade_date"].astype(str)
        return df

    def _load_prices(
        self, codes: list[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """加载 close 价格，stock_daily 优先，没有则从 etf_daily 补。"""
        if not codes:
            return pd.DataFrame()
        ph = ", ".join(f":c{i}" for i in range(len(codes)))
        params: dict = {f"c{i}": c for i, c in enumerate(codes)}
        params.update({"s": start_date, "e": end_date})

        stock_df = self._repo.read_sql(
            f"""
            SELECT code, trade_date, close FROM stock_daily
            WHERE code IN ({ph}) AND trade_date BETWEEN :s AND :e
            """,
            params,
        )
        etf_df = self._repo.read_sql(
            f"""
            SELECT code, trade_date, close FROM etf_daily
            WHERE code IN ({ph}) AND trade_date BETWEEN :s AND :e
            """,
            params,
        )
        combined = pd.concat([stock_df, etf_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["code", "trade_date"], keep="first")
        combined["trade_date"] = combined["trade_date"].astype(str)
        combined["close"] = pd.to_numeric(combined["close"], errors="coerce")

        if combined.empty:
            return pd.DataFrame()
        pivot = combined.pivot(index="trade_date", columns="code", values="close")
        return pivot

    # ── 数据库写入 ───────────────────────────────────────────

    def _save_run(
        self,
        name: str,
        strategy: str,
        codes: list[str],
        start_date: str,
        end_date: str,
        rebalance_days: int,
        top_k: int | None,
        metrics: dict,
    ) -> int:
        params_json = json.dumps(
            {"codes": codes, "commission": self.commission, "slippage": self.slippage},
            ensure_ascii=False,
        )
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO backtest_run
                        (name, strategy, start_date, end_date, rebalance_days, top_k, params, metrics)
                    VALUES
                        (:name, :strategy, :start, :end, :rbal, :topk, :params, :metrics)
                """),
                {
                    "name": name,
                    "strategy": strategy,
                    "start": start_date,
                    "end": end_date,
                    "rbal": rebalance_days,
                    "topk": top_k,
                    "params": params_json,
                    "metrics": json.dumps(metrics, ensure_ascii=False),
                },
            )
            run_id = result.lastrowid
        logger.info(f"[回测] 写入 backtest_run run_id={run_id}")
        return run_id

    def _save_nav(self, run_id: int, nav_records: list[dict]) -> None:
        if not nav_records:
            return
        df = pd.DataFrame(nav_records)
        df["run_id"] = run_id
        self._repo.upsert("backtest_nav", df, unique_keys=["run_id", "trade_date"])
        logger.info(f"[回测] 写入 backtest_nav {len(df)} 条 (run_id={run_id})")

    def _save_trades(self, run_id: int, trade_records: list[dict]) -> None:
        if not trade_records:
            return
        df = pd.DataFrame(trade_records)
        df["run_id"] = run_id
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    text("""
                        INSERT INTO backtest_trades (run_id, trade_date, code, action, weight, price)
                        VALUES (:rid, :td, :code, :action, :weight, :price)
                    """),
                    {
                        "rid": run_id,
                        "td": row["trade_date"],
                        "code": row["code"],
                        "action": row["action"],
                        "weight": row.get("weight"),
                        "price": row.get("price"),
                    },
                )
        logger.info(f"[回测] 写入 backtest_trades {len(df)} 条 (run_id={run_id})")
