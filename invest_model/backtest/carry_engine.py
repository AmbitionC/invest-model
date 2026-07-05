"""套利兄弟回测引擎：逆回购现金 carry / 可转债双低。

契约与 CSBacktestEngine 完全一致：``.run() -> CSBacktestResult(config, nav_df,
trades, metrics)``，nav_df 列 [trade_date, nav, ret, turnover, position_count,
invested]，落 backtest_run/nav/trades 按 version，下游复盘/看板零改动。

数据缺失时返回平坦净值（nav≡1，对应 sleeve 降级为现金），绝不加杠杆。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.arb.carry import double_low
from invest_model.arb.config import ArbConfig
from invest_model.backtest.cs_engine import CSBacktestConfig, CSBacktestResult
from invest_model.backtest.metrics import compute_metrics
from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


def _flat_result(cfg: CSBacktestConfig, dates: list[str]) -> CSBacktestResult:
    dates = dates or [cfg.start_date or "20210101"]
    nav = pd.DataFrame({
        "trade_date": dates, "nav": [1.0] * len(dates), "ret": [0.0] * len(dates),
        "turnover": [0.0] * len(dates), "position_count": [0] * len(dates),
        "invested": [0.0] * len(dates),
    })
    return CSBacktestResult(config=cfg, nav_df=nav, trades=[],
                            metrics=compute_metrics(nav).to_dict())


class CarryBacktestEngine:
    def __init__(self, engine, config: CSBacktestConfig, mode: str,
                 arb_cfg: ArbConfig | None = None,
                 benchmark_nav: pd.Series | None = None):
        self.engine = engine
        self.cfg = config
        self.mode = mode                       # reverse_repo | convertible
        self.arb = arb_cfg or ArbConfig()
        self.repo = BaseRepository(engine)
        self.benchmark_nav = benchmark_nav

    def _trade_dates(self) -> list[str]:
        df = self.repo.read_sql(
            "SELECT DISTINCT cal_date FROM trade_calendar WHERE is_open=1 "
            "AND cal_date>=:s AND cal_date<=:e ORDER BY cal_date",
            {"s": self.cfg.start_date, "e": self.cfg.end_date})
        return df["cal_date"].tolist() if not df.empty else []

    def run(self) -> CSBacktestResult:
        dates = self._trade_dates()
        if self.mode == "reverse_repo":
            return self._run_reverse_repo(dates)
        if self.mode == "convertible":
            return self._run_convertible(dates)
        return _flat_result(self.cfg, dates)

    # ── 逆回购现金 carry：每日按 rate*interest_days/365 计息 ──
    def _run_reverse_repo(self, dates: list[str]) -> CSBacktestResult:
        if not self.repo.table_exists("reverse_repo_daily"):
            return _flat_result(self.cfg, dates)
        rr = self.repo.read_sql(
            "SELECT trade_date, rate, interest_days FROM reverse_repo_daily "
            "WHERE code='204001.SH' AND trade_date>=:s AND trade_date<=:e ORDER BY trade_date",
            {"s": self.cfg.start_date, "e": self.cfg.end_date})
        if rr.empty:
            return _flat_result(self.cfg, dates)
        rr["rate"] = pd.to_numeric(rr["rate"], errors="coerce").fillna(0.0)
        rr["interest_days"] = pd.to_numeric(rr["interest_days"], errors="coerce").fillna(1)
        rr["ret"] = rr["rate"] / 100.0 * rr["interest_days"] / 365.0
        rr["nav"] = (1.0 + rr["ret"]).cumprod()
        nav = pd.DataFrame({
            "trade_date": rr["trade_date"], "nav": rr["nav"], "ret": rr["ret"],
            "turnover": 0.0, "position_count": 1, "invested": 1.0,
        }).reset_index(drop=True)
        return CSBacktestResult(config=self.cfg, nav_df=nav, trades=[],
                                metrics=compute_metrics(nav, self.benchmark_nav).to_dict())

    # ── 可转债双低：月频等权 top-N 篮子，cb_daily 逐日重估（无印花税/T+0）──
    def _run_convertible(self, dates: list[str]) -> CSBacktestResult:
        if not (self.repo.table_exists("cb_daily") and self.repo.table_exists("cb_basic")):
            return _flat_result(self.cfg, dates)
        px = self.repo.read_sql(
            "SELECT code, trade_date, close FROM cb_daily "
            "WHERE trade_date>=:s AND trade_date<=:e",
            {"s": self.cfg.start_date, "e": self.cfg.end_date})
        if px.empty:
            return _flat_result(self.cfg, dates)
        px["close"] = pd.to_numeric(px["close"], errors="coerce")
        wide = px.pivot_table(index="trade_date", columns="code", values="close").sort_index()
        # 月频调仓日
        idx = wide.index.tolist()
        reb = _month_starts(idx)
        basket = self._double_low_basket(reb[0]) if reb else []
        nav_vals, rets, dates_out, turns = [], [], [], []
        prev_nav = 1.0
        holdings = {c: (1.0 / len(basket)) for c in basket} if basket else {}
        for i, d in enumerate(idx):
            if d in reb and i > 0:
                new_basket = self._double_low_basket(d)
                if new_basket:
                    holdings = {c: (1.0 / len(new_basket)) for c in new_basket}
                    turns.append((d, 1.0))
            row = wide.loc[d]
            if i == 0:
                nav_vals.append(1.0); rets.append(0.0); dates_out.append(d); continue
            prow = wide.iloc[i - 1]
            port_ret = 0.0
            for c, w in holdings.items():
                p0, p1 = prow.get(c), row.get(c)
                if p0 and p1 and np.isfinite(p0) and np.isfinite(p1) and p0 > 0:
                    port_ret += w * (p1 / p0 - 1.0)
            nav = prev_nav * (1.0 + port_ret)
            nav_vals.append(nav); rets.append(port_ret); dates_out.append(d)
            prev_nav = nav
        turn_map = dict(turns)
        nav = pd.DataFrame({
            "trade_date": dates_out, "nav": nav_vals, "ret": rets,
            "turnover": [turn_map.get(d, 0.0) for d in dates_out],
            "position_count": len(holdings), "invested": 1.0,
        })
        return CSBacktestResult(config=self.cfg, nav_df=nav, trades=[],
                                metrics=compute_metrics(nav, self.benchmark_nav).to_dict())

    def _double_low_basket(self, dt: str) -> list[str]:
        cb = self.repo.read_sql(
            "SELECT d.code, d.close AS cb_close, b.conv_price, b.stk_code, b.call_status "
            "FROM cb_daily d JOIN cb_basic b ON d.code=b.ts_code WHERE d.trade_date=:d",
            {"d": dt})
        if cb.empty:
            return []
        stk = list(dict.fromkeys(cb["stk_code"].dropna().astype(str)))
        px_map = {}
        if stk:
            ph = ",".join(f":c{i}" for i in range(len(stk)))
            params = {f"c{i}": c for i, c in enumerate(stk)}
            params["d"] = dt
            px = self.repo.read_sql(
                f"SELECT code, close FROM stock_daily WHERE code IN ({ph}) AND trade_date=:d",
                params)
            px_map = dict(zip(px["code"], px["close"])) if not px.empty else {}
        scored = []
        for _, r in cb.iterrows():
            if r.get("call_status") and str(r["call_status"]) in ("已公告强赎", "强赎"):
                continue
            sc = px_map.get(str(r["stk_code"]))
            if sc is None:
                continue
            dl = double_low(r["cb_close"], r["conv_price"], sc)
            if np.isfinite(dl["score"]):
                scored.append((r["code"], dl["score"]))
        scored.sort(key=lambda x: x[1])
        return [c for c, _ in scored[: self.arb.double_low_top_n]]


def _month_starts(dates: list[str]) -> list[str]:
    out, seen = [], set()
    for d in dates:
        ym = d[:6]
        if ym not in seen:
            seen.add(ym)
            out.append(d)
    return out
