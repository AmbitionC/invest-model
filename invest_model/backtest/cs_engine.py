"""截面回测引擎：逐日 close-to-close 重估 + 调仓日按外部目标权重换仓。

与旧 ``BacktestEngine`` 的区别：universe 逐期变化、目标权重由外部 target_provider
给出（来自 portfolio 层），不绑定单票 advisor。成本模型：双边手续费 0.0003 +
卖出印花税 0.001（2023-08-28 起自动减半至 0.0005）+ 滑点 0.0005。
A 股拟真：涨停不买 / 跌停不卖（阈值按板块：主板 10% / 创业板科创板 20% / 北交所 30%）、
停牌不可交易、退市/永停按最后成交价强制清算，月频收盘换仓天然满足 T+1。
换仓先卖后买、买入受可用现金约束（Σ权重 ≤ 1，无隐性杠杆）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

from invest_model.backtest.metrics import compute_metrics
from invest_model.logger import get_logger
from invest_model.portfolio.risk import RiskConfig, evaluate_holding, ma_tail
from invest_model.repositories.base import BaseRepository

logger = get_logger()

TargetProvider = Callable[[str, dict[str, float]], dict[str, float]]
ExitCodesProvider = Callable[[str], set]


@dataclass
class CSBacktestConfig:
    name: str = "cs_multifactor_v1"
    strategy: str = "cross_sectional_multifactor"
    start_date: str = ""
    end_date: str = ""
    fee_rate: float = 0.0003
    stamp_tax: float = 0.001         # 卖出印花税基准税率；2023-08-28 起自动减半（见 _stamp）
    slippage: float = 0.0005
    min_trade: float = 0.01          # 换手带：权重变动 < 1% 跳过
    benchmark_code: str = "000300.SH"
    limit_pct: float = 9.8           # 主板涨跌停近似阈值（|pct_chg|）；创业板/科创板/北交所按板块放大（见 _limit_threshold）
    exec_lag: int = 1                # 成交滞后：1=调仓日出信号、次一交易日收盘成交（避免同日前视）；0=同日收盘成交
    delist_after_days: int = 60      # 无行情超过此交易日数 → 视为退市/永停，按最后成交价强制清算
    delist_recovery: float = 1.0     # 强制清算回收比例（1.0=按最后成交价全额变现；可设折价模拟老三板）
    risk: RiskConfig = field(default_factory=lambda: RiskConfig(enabled=False))


@dataclass
class CSBacktestResult:
    config: CSBacktestConfig
    nav_df: pd.DataFrame
    trades: list[dict] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


class CSBacktestEngine:
    def __init__(self, engine, config: CSBacktestConfig, target_provider: TargetProvider,
                 rebalance_dates: list[str], exit_codes_provider: ExitCodesProvider | None = None):
        self.engine = engine
        self.cfg = config
        self.target_provider = target_provider
        self.exit_codes_provider = exit_codes_provider
        self.reb_set = set(rebalance_dates)
        self.repo = BaseRepository(engine)
        self._close: pd.DataFrame = pd.DataFrame()
        self._pct: pd.DataFrame = pd.DataFrame()
        self._exit_cache: dict[str, set] = {}
        self._date_pos: dict[str, int] = {}
        self._last_pos: dict[str, int] = {}

    def _exit_codes(self, dt: str) -> set:
        if self.exit_codes_provider is None:
            return set()
        if dt not in self._exit_cache:
            try:
                self._exit_cache[dt] = set(self.exit_codes_provider(dt) or set())
            except Exception:  # noqa: BLE001
                self._exit_cache[dt] = set()
        return self._exit_cache[dt]

    def _load_prices(self) -> list[str]:
        # 取数起点前移 ~120 自然日做 MA 暖机（首月即可算 MA60/MA20）。
        warm_start = (pd.to_datetime(self.cfg.start_date) - pd.Timedelta(days=120)).strftime("%Y%m%d")
        df = self.repo.read_sql(
            "SELECT code, trade_date, close, pct_chg FROM stock_daily "
            "WHERE trade_date>=:s AND trade_date<=:d",
            {"s": warm_start, "d": self.cfg.end_date},
        )
        if df.empty:
            return []
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
        self._close = df.pivot(index="trade_date", columns="code", values="close").sort_index()
        self._pct = df.pivot(index="trade_date", columns="code", values="pct_chg").sort_index()
        # 每只票最后有价日在全日期序列中的位置（退市/永停判定用）
        notna = self._close.notna().to_numpy()
        self._date_pos = {d: i for i, d in enumerate(self._close.index)}
        self._last_pos = {c: int(np.where(notna[:, j])[0][-1])
                          for j, c in enumerate(self._close.columns) if notna[:, j].any()}
        # 暖机段只用于算 MA，不参与回测交易；交易日历从 start_date 起。
        all_dates = self._close.index.tolist()
        return [d for d in all_dates if d >= self.cfg.start_date]

    def _stamp(self, dt: str) -> float:
        """卖出印花税：2023-08-28 起由 0.1% 减半至 0.05%。"""
        return self.cfg.stamp_tax / 2 if dt >= "20230828" else self.cfg.stamp_tax

    def _limit_threshold(self, code: str, dt: str) -> float:
        """按板块近似涨跌停阈值：主板 ±10%，创业板 ±20%（2020-08-24 注册制改革起，
        此前 ±10%），科创板 ±20%（设立即如此），北交所 ±30%。"""
        if code.endswith(".BJ"):
            return 29.5
        if code.startswith(("688", "689")):
            return 19.6
        if code.startswith(("300", "301", "302")):
            return 19.6 if dt >= "20200824" else self.cfg.limit_pct
        return self.cfg.limit_pct

    def _tradable(self, code: str, dt: str, buying: bool) -> bool:
        try:
            p = self._close.at[dt, code]
            pc = self._pct.at[dt, code]
        except KeyError:
            return False
        if p is None or not np.isfinite(p) or p <= 0:
            return False
        if pd.notna(pc):
            lim = self._limit_threshold(code, dt)
            if buying and pc >= lim:
                return False           # 涨停不买
            if (not buying) and pc <= -lim:
                return False           # 跌停不卖
        return True

    def run(self) -> CSBacktestResult:
        dates = self._load_prices()
        if not dates:
            raise ValueError(f"回测区间无行情：{self.cfg.start_date}~{self.cfg.end_date}")

        weights: dict[str, float] = {}
        nav = 1.0
        nav_rows, trades = [], []
        prev_close = {}
        pending: dict[str, float] | None = None   # 待次日成交的目标权重
        # 风控状态
        state = {"entry_cost": {}, "trail_tier": {}}
        peak_nav = 1.0
        risk_off = False
        risk_on = self.cfg.risk.enabled

        for i, dt in enumerate(dates):
            day_ret = 0.0
            if i > 0 and weights:
                for c, w in list(weights.items()):
                    p0 = prev_close.get(c)
                    p1 = self._close.at[dt, c] if c in self._close.columns else None
                    if p0 and p1 and np.isfinite(p1) and p0 > 0:
                        day_ret += w * (p1 / p0 - 1.0)
                nav *= 1.0 + day_ret
                # 重估权重
                new_w = {}
                for c, w in weights.items():
                    p0 = prev_close.get(c)
                    p1 = self._close.at[dt, c] if c in self._close.columns else None
                    new_w[c] = w * (p1 / p0) if (p0 and p1 and np.isfinite(p1) and p0 > 0) else w
                tot = sum(new_w.values())
                cash = max(0.0, 1.0 - sum(weights.values()))
                denom = tot + cash
                if denom > 0:
                    weights = {c: v / denom for c, v in new_w.items()}

            turnover = 0.0
            # 退市/永停清算：无行情超过 delist_after_days 的持仓按最后成交价强制变现。
            # 否则退市股会永久冻结在最后价格上（卖不掉、日收益记 0），损失从不落地。
            if weights:
                t_dl, nav = self._liquidate_delisted(dt, weights, nav, trades, state)
                turnover += t_dl
            # 日频风控：硬止损 / 均线移动止盈 / 逻辑证伪 / 账户级回撤（在成交前）。
            if risk_on and weights:
                t_ex, nav, hit = self._apply_risk_exits(dt, weights, nav, trades, state, peak_nav)
                turnover += t_ex
                if hit:                       # 账户回撤达阈值(risk.account_dd_stop) → 清仓转现金，封锁买入到下个调仓日
                    risk_off = True
                    pending = None
            peak_nav = max(peak_nav, nav)

            # 成交：exec_lag=1 时，调仓日只出信号（pending），次一交易日收盘成交，
            # 杜绝「用当日收盘信号在当日收盘成交」的同日前视。
            if pending is not None and not risk_off:
                t, nav = self._apply_targets(pending, dt, weights, nav, trades, state)
                turnover += t
                pending = None
            if dt in self.reb_set:
                risk_off = False              # 调仓日重置：重新审视、可再建仓
                signal = self.target_provider(dt, dict(weights)) or {}
                if self.cfg.exec_lag <= 0:
                    t, nav = self._apply_targets(signal, dt, weights, nav, trades, state)
                    turnover += t
                else:
                    pending = signal

            n_pos = sum(1 for w in weights.values() if w > 1e-4)
            invested = sum(weights.values())
            nav_rows.append({"trade_date": dt, "nav": round(nav, 6),
                             "ret": round(day_ret, 6) if i > 0 else 0.0,
                             "turnover": round(turnover, 6),
                             "position_count": n_pos,
                             "invested": round(invested, 4)})

            for c in self._close.columns:
                p = self._close.at[dt, c]
                if p is not None and np.isfinite(p):
                    prev_close[c] = float(p)

        nav_df = pd.DataFrame(nav_rows)
        metrics = compute_metrics(nav_df, benchmark_nav=self._benchmark_nav(dates))
        md = metrics.to_dict()
        md["avg_invested"] = round(float(nav_df["invested"].mean()), 4)
        return CSBacktestResult(self.cfg, nav_df, trades, md)

    def _apply_targets(self, targets: dict[str, float], dt: str,
                       weights: dict[str, float], nav: float, trades: list,
                       state: dict | None = None) -> tuple[float, float]:
        """在 dt 收盘把组合调向 targets，扣成本、记录成交。返回 (turnover, nav)。

        先卖后买：卖出先执行释放资金，买入按卖出后的实际可用现金等比缩放。
        否则旧持仓因跌停/停牌卖不掉、新买入又全额执行时，权重和会超过 1
        （相当于免费加杠杆），系统性高估回测收益。
        """
        turnover = 0.0
        # 1) 卖出（减仓/清仓）
        for c in list(weights):
            cur = weights[c]
            tgt = targets.get(c, 0.0)
            delta = tgt - cur
            if delta >= 0 or abs(delta) < self.cfg.min_trade:
                continue
            if not self._tradable(c, dt, buying=False):
                continue
            fee = self.cfg.fee_rate + self.cfg.slippage + self._stamp(dt)
            nav *= 1.0 - abs(delta) * fee
            turnover += abs(delta)
            weights[c] = tgt
            trades.append({"trade_date": dt, "code": c, "action": "sell",
                           "weight": round(tgt, 6),
                           "price": float(self._close.at[dt, c])})
        # 2) 买入（新建/加仓）：受卖出后可用现金约束
        buys: dict[str, float] = {}
        for c, tgt in targets.items():
            delta = tgt - weights.get(c, 0.0)
            if delta < self.cfg.min_trade:
                continue
            if not self._tradable(c, dt, buying=True):
                continue
            buys[c] = delta
        cash = max(0.0, 1.0 - sum(weights.values()))
        total_buy = sum(buys.values())
        scale = min(1.0, cash / total_buy) if total_buy > 0 else 0.0
        for c, delta in buys.items():
            d = delta * scale
            if d <= 0:
                continue
            fee = self.cfg.fee_rate + self.cfg.slippage
            nav *= 1.0 - d * fee
            turnover += d
            weights[c] = weights.get(c, 0.0) + d
            if state is not None:
                # 买入：以成交价重置成本与移动止盈档位（加仓视为重新建仓）。
                state["entry_cost"][c] = float(self._close.at[dt, c])
                state["trail_tier"][c] = 0
            trades.append({"trade_date": dt, "code": c, "action": "buy",
                           "weight": round(weights[c], 6),
                           "price": float(self._close.at[dt, c])})
        # 清理空仓
        for c in [c for c, w in weights.items() if w <= 1e-6]:
            weights.pop(c, None)
            if state is not None:
                state["entry_cost"].pop(c, None)
                state["trail_tier"].pop(c, None)
        return turnover, nav

    def _execute_sell(self, c: str, tgt_w: float, dt: str, weights: dict[str, float],
                      nav: float, trades: list, state: dict) -> tuple[float, float]:
        """风控强制减仓/清仓到 tgt_w（绕过换手带；遵守跌停不卖）。返回 (turnover, nav)。"""
        cur = weights.get(c, 0.0)
        if tgt_w >= cur - 1e-9:
            return 0.0, nav
        if not self._tradable(c, dt, buying=False):
            return 0.0, nav
        delta = cur - tgt_w
        fee = self.cfg.fee_rate + self.cfg.slippage + self._stamp(dt)
        nav *= 1.0 - delta * fee
        weights[c] = tgt_w
        trades.append({"trade_date": dt, "code": c, "action": "sell",
                       "weight": round(tgt_w, 6), "price": float(self._close.at[dt, c])})
        if tgt_w <= 1e-6:
            weights.pop(c, None)
            state["entry_cost"].pop(c, None)
            state["trail_tier"].pop(c, None)
        return delta, nav

    def _liquidate_delisted(self, dt: str, weights: dict[str, float], nav: float,
                            trades: list, state: dict | None = None) -> tuple[float, float]:
        """强制清算已退市/超长停牌的持仓。返回 (turnover, nav)。

        以最后成交价 × delist_recovery 变现（2020 退市新规后有退市整理期，
        崩跌大多已体现在最后成交价里；如需模拟老三板折价可调低 recovery）。
        """
        j = self._date_pos.get(dt)
        if j is None:
            return 0.0, nav
        turnover = 0.0
        for c in list(weights):
            lp = self._last_pos.get(c)
            if lp is None or j - lp < self.cfg.delist_after_days:
                continue
            w = weights.pop(c)
            rec = self.cfg.delist_recovery
            fee = self.cfg.fee_rate + self.cfg.slippage + self._stamp(dt)
            nav *= 1.0 - w * (1.0 - rec) - w * rec * fee
            turnover += w
            last_px = float(self._close.iloc[lp][c])
            trades.append({"trade_date": dt, "code": c, "action": "delist",
                           "weight": 0.0, "price": last_px})
            if state is not None:
                state["entry_cost"].pop(c, None)
                state["trail_tier"].pop(c, None)
        return turnover, nav

    def _apply_risk_exits(self, dt: str, weights: dict[str, float], nav: float,
                          trades: list, state: dict, peak_nav: float) -> tuple[float, float, bool]:
        """日频风控：逐票评估硬止损/移动止盈/逻辑证伪，再判账户级回撤。

        返回 (turnover, nav, account_stop_hit)。
        """
        rc = self.cfg.risk
        exit_codes = self._exit_codes(dt)
        turnover = 0.0
        for c in list(weights.keys()):
            if c not in self._close.columns:
                continue
            hist = self._close[c].loc[:dt]
            if hist.dropna().empty:
                continue
            cost = state["entry_cost"].get(c, float("nan"))
            prev_tier = state["trail_tier"].get(c, 0)
            dec = evaluate_holding(hist, cost, rc, in_exit_codes=(c in exit_codes),
                                   prev_tier=prev_tier)
            state["trail_tier"][c] = dec.new_tier
            if dec.action == "exit":
                t, nav = self._execute_sell(c, 0.0, dt, weights, nav, trades, state)
                turnover += t
            elif dec.action == "trim":
                t, nav = self._execute_sell(c, weights[c] * dec.keep_frac, dt,
                                            weights, nav, trades, state)
                turnover += t
        # 账户级回撤止损：相对峰值回撤达阈值 → 全部清仓转现金。
        if rc.account_dd_stop and peak_nav > 0 and nav / peak_nav - 1.0 <= -rc.account_dd_stop:
            for c in list(weights.keys()):
                t, nav = self._execute_sell(c, 0.0, dt, weights, nav, trades, state)
                turnover += t
            return turnover, nav, True
        return turnover, nav, False

    def _benchmark_nav(self, dates: list[str]) -> pd.DataFrame | None:
        df = self.repo.read_sql(
            "SELECT trade_date, close FROM index_daily "
            "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
            {"c": self.cfg.benchmark_code, "s": self.cfg.start_date, "d": self.cfg.end_date},
        )
        if df.empty:
            return None
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.set_index("trade_date").reindex(dates).ffill().dropna()
        if df.empty:
            return None
        df["nav"] = df["close"] / df["close"].iloc[0]
        return df.reset_index().rename(columns={"index": "trade_date"})[["trade_date", "nav"]]
