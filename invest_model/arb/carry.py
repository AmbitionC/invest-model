"""引擎 A 防守 carry 信号：国债逆回购 / 红利 / 可转债双低。

纯函数计算 + 薄 IO 构建器（写 carry_signal，按 version）。所有函数不做择时、
不加杠杆——carry 是"稳吃利差"，对应文档引擎 A 底盘。
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from invest_model.arb.config import (
    VERSION_CB,
    VERSION_DIV,
    VERSION_REPO,
    ArbConfig,
)
from invest_model.logger import get_logger
from invest_model.repositories.arb_repo import CarryRepo
from invest_model.repositories.base import BaseRepository

logger = get_logger()


# ── 纯函数 ──────────────────────────────────────────────────

def reverse_repo_carry(rate: float, interest_days: int) -> dict:
    """逆回购有效收益：资金占用 1 日、计息 interest_days 天。

    rate 为年化利率(%)。effective_yield = rate/100 * interest_days/365。
    """
    r = float(rate) / 100.0 if rate is not None and np.isfinite(rate) else 0.0
    days = max(1, int(interest_days or 1))
    eff = r * days / 365.0
    return {"annualized": r, "interest_days": days, "effective_yield": eff}


def _dividend_tax(hold_days: int, cfg: ArbConfig) -> float:
    if hold_days < 30:
        return cfg.tax_short
    if hold_days < 365:
        return cfg.tax_mid
    return cfg.tax_long


def dividend_carry_net(dv_ratio: float, intended_hold_days: int, cfg: ArbConfig) -> dict:
    """红利 carry 税后预期：A 股差别化个税，防守取长持免税桶。

    dv_ratio 为股息率(%)。返回税后预期年化 + 提示（除权除息会压价，非白拿）。
    """
    dv = float(dv_ratio) / 100.0 if dv_ratio is not None and np.isfinite(dv_ratio) else 0.0
    tax = _dividend_tax(int(intended_hold_days or 400), cfg)
    net = dv * (1 - tax)
    return {
        "gross_dv": dv, "tax": tax, "expected_net": net,
        "price_gap_risk": "除权除息压价，需主业稳定、避免贴息填权失败",
    }


def double_low(cb_close: float, conv_price: float, stock_close: float) -> dict:
    """可转债双低：转股价值 + 转股溢价率 → 双低分（越低越好）。

    conv_value = 100/conv_price * stock_close；premium = cb_close/conv_value - 1；
    score = cb_close + premium*100。
    """
    try:
        cp = float(conv_price)
        sc = float(stock_close)
        cbc = float(cb_close)
        if cp <= 0 or sc <= 0 or cbc <= 0 or not all(np.isfinite([cp, sc, cbc])):
            return {"conv_value": float("nan"), "conv_premium": float("nan"),
                    "score": float("nan")}
        conv_value = 100.0 / cp * sc
        premium = cbc / conv_value - 1.0
        score = cbc + premium * 100.0
        return {"conv_value": conv_value, "conv_premium": premium, "score": score}
    except (TypeError, ValueError, ZeroDivisionError):
        return {"conv_value": float("nan"), "conv_premium": float("nan"),
                "score": float("nan")}


# ── 薄 IO 构建器（写 carry_signal）───────────────────────────

def build_carry_signals(engine, dt: str, cfg: ArbConfig | None = None,
                        persist: bool = True) -> dict[str, pd.DataFrame]:
    """在决策日 dt 生成三类 carry 信号。数据缺表即跳过该策略（降级为现金）。"""
    cfg = cfg or ArbConfig()
    repo = BaseRepository(engine)
    carry_repo = CarryRepo(engine)
    out: dict[str, pd.DataFrame] = {}

    # 逆回购
    if cfg.reverse_repo and repo.table_exists("reverse_repo_daily"):
        rr = repo.read_sql(
            "SELECT code, rate, interest_days FROM reverse_repo_daily "
            "WHERE trade_date=(SELECT MAX(trade_date) FROM reverse_repo_daily "
            "WHERE trade_date<=:d)", {"d": dt})
        rows = []
        for _, r in rr.iterrows():
            c = reverse_repo_carry(r["rate"], r["interest_days"])
            rows.append({"code": r["code"], "expected_carry": c["annualized"],
                         "horizon_days": c["interest_days"],
                         "metric": json.dumps({"effective_yield": c["effective_yield"]})})
        if rows:
            df = pd.DataFrame(rows).sort_values("expected_carry", ascending=False)
            df["rank"] = range(1, len(df) + 1)
            df = df.assign(trade_date=dt, sleeve="reverse_repo", version=VERSION_REPO)
            out["reverse_repo"] = df
            if persist:
                carry_repo.save(df[["trade_date", "sleeve", "code", "version",
                                    "expected_carry", "horizon_days", "rank", "metric"]])

    # 红利 carry（股息率来自 stock_fundamental.dv_ratio）
    # 可交易性硬闸（E14 判据②前置）：剔除 B 股（200xxx 深B / 900xxx 沪B——A 股账户不可交易，
    # 且其高股息常虚高误导排序）；有 stock_info 时再剔 ST/*ST（退市/基本面风险，不当防守底盘）。
    if cfg.dividend and repo.table_exists("stock_fundamental"):
        st_join, st_cond = "", ""
        if repo.table_exists("stock_info"):
            st_join = "JOIN stock_info i ON f.code=i.ts_code "
            st_cond = "AND i.name NOT LIKE '%%ST%%' "
        dv = repo.read_sql(
            f"SELECT f.code, f.dv_ratio FROM stock_fundamental f {st_join}"
            f"WHERE f.trade_date=(SELECT MAX(trade_date) FROM stock_fundamental "
            f"WHERE trade_date<=:d) AND f.dv_ratio>=:m "
            f"AND f.code NOT LIKE '200%%' AND f.code NOT LIKE '900%%' "
            f"{st_cond}ORDER BY f.dv_ratio DESC LIMIT :n",
            {"d": dt, "m": cfg.dividend_min_dv, "n": cfg.dividend_top_n})
        rows = []
        for _, r in dv.iterrows():
            c = dividend_carry_net(r["dv_ratio"], 400, cfg)
            rows.append({"code": r["code"], "expected_carry": c["expected_net"],
                         "horizon_days": 400,
                         "metric": json.dumps({"gross_dv": c["gross_dv"], "tax": c["tax"]})})
        if rows:
            df = pd.DataFrame(rows).sort_values("expected_carry", ascending=False)
            df["rank"] = range(1, len(df) + 1)
            df = df.assign(trade_date=dt, sleeve="dividend_carry", version=VERSION_DIV)
            out["dividend_carry"] = df
            if persist:
                carry_repo.save(df[["trade_date", "sleeve", "code", "version",
                                    "expected_carry", "horizon_days", "rank", "metric"]])

    # 可转债双低
    if cfg.convertible and repo.table_exists("cb_daily") and repo.table_exists("cb_basic"):
        cb = repo.read_sql(
            "SELECT d.code, d.close AS cb_close, b.conv_price, b.stk_code, b.call_status "
            "FROM cb_daily d JOIN cb_basic b ON d.code=b.ts_code "
            "WHERE d.trade_date=(SELECT MAX(trade_date) FROM cb_daily WHERE trade_date<=:d)",
            {"d": dt})
        rows = []
        if not cb.empty:
            stk_codes = list(dict.fromkeys(cb["stk_code"].dropna().astype(str)))
            px = pd.DataFrame()
            if stk_codes:
                ph = ",".join(f":c{i}" for i in range(len(stk_codes)))
                params = {f"c{i}": c for i, c in enumerate(stk_codes)}
                params["d"] = dt
                px = repo.read_sql(
                    f"SELECT code, close FROM stock_daily WHERE code IN ({ph}) "
                    f"AND trade_date=(SELECT MAX(trade_date) FROM stock_daily WHERE trade_date<=:d)",
                    params)
            px_map = dict(zip(px["code"], px["close"])) if not px.empty else {}
            for _, r in cb.iterrows():
                if r.get("call_status") and str(r["call_status"]) in ("已公告强赎", "强赎"):
                    continue
                sc = px_map.get(str(r["stk_code"]))
                if sc is None:
                    continue
                dl = double_low(r["cb_close"], r["conv_price"], sc)
                if not np.isfinite(dl["score"]):
                    continue
                rows.append({"code": r["code"], "expected_carry": None,
                             "horizon_days": None,
                             "metric": json.dumps({"score": dl["score"],
                                                   "premium": dl["conv_premium"],
                                                   "cb_close": float(r["cb_close"])}),
                             "_score": dl["score"]})
        if rows:
            df = pd.DataFrame(rows).sort_values("_score").head(cfg.double_low_top_n)
            df["rank"] = range(1, len(df) + 1)
            df = df.drop(columns=["_score"]).assign(
                trade_date=dt, sleeve="convertible", version=VERSION_CB)
            out["convertible"] = df
            if persist:
                carry_repo.save(df[["trade_date", "sleeve", "code", "version",
                                    "expected_carry", "horizon_days", "rank", "metric"]])

    logger.info(f"carry 信号 {dt}: " + ", ".join(f"{k}={len(v)}" for k, v in out.items())
                if out else f"carry 信号 {dt}: 无（数据缺失或全降级）")
    return out
