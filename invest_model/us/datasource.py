"""yfinance 数据客户端（美股模块专用；懒加载，失败逐级降级不抛穿）。

只在 GitHub Actions（美国 runner）上运行——阿里云 FC 访问 Yahoo 不可靠，
这也是美股链路与 A 股 FC 链路物理隔离的原因之一。
"""

from __future__ import annotations

import pandas as pd

from invest_model.logger import get_logger

logger = get_logger()


def _yf():
    import yfinance  # 懒加载：A 股链路/测试环境无需安装

    return yfinance


def fetch_daily(codes: list[str], period: str = "2y") -> pd.DataFrame:
    """批量日线（auto_adjust 复权收盘）。返回长表 code/trade_date/OHLCV。"""
    yf = _yf()
    frames: list[pd.DataFrame] = []
    data = yf.download(codes, period=period, interval="1d",
                       auto_adjust=True, group_by="ticker",
                       progress=False, threads=True)
    for code in codes:
        try:
            df = data[code] if len(codes) > 1 else data
            df = df.dropna(subset=["Close"]).reset_index()
            if df.empty:
                logger.warning(f"us daily {code}: 空数据，跳过")
                continue
            out = pd.DataFrame({
                "code": code,
                "trade_date": pd.to_datetime(df["Date"]).dt.strftime("%Y%m%d"),
                "open": df["Open"], "high": df["High"], "low": df["Low"],
                "close": df["Close"], "volume": df["Volume"],
            })
            frames.append(out)
        except Exception as e:  # noqa: BLE001 — 单票失败不拖累整批
            logger.warning(f"us daily {code} 解析失败（跳过）：{e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_info(code: str) -> dict:
    """标的静态信息（name/sector），失败回空 dict。"""
    try:
        info = _yf().Ticker(code).info or {}
        return {"name": (info.get("shortName") or code)[:64],
                "sector": (info.get("sector") or "")[:48],
                "kind": "etf" if info.get("quoteType") == "ETF" else
                        ("index" if code.startswith("^") else "stock")}
    except Exception as e:  # noqa: BLE001
        logger.warning(f"us info {code} 失败：{e}")
        return {}


def fetch_fundamentals_q(code: str) -> pd.DataFrame:
    """季度基本面（best-effort：yfinance 报表字段常缺，缺了就少列）。

    返回 code/quarter_end/revenue/net_income/gross_margin/fcf/net_debt。
    """
    try:
        t = _yf().Ticker(code)
        inc = t.quarterly_income_stmt
        if inc is None or inc.empty:
            return pd.DataFrame()
        rows = []
        bs = getattr(t, "quarterly_balance_sheet", None)
        cf = getattr(t, "quarterly_cash_flow", None)

        def _at(df, field, col):
            try:
                if df is not None and field in df.index:
                    v = df.loc[field, col]
                    return float(v) if pd.notna(v) else None
            except Exception:  # noqa: BLE001
                pass
            return None

        for col in inc.columns:
            rev = _at(inc, "Total Revenue", col)
            ni = _at(inc, "Net Income", col)
            gp = _at(inc, "Gross Profit", col)
            fcf = _at(cf, "Free Cash Flow", col)
            capex = _at(cf, "Capital Expenditure", col)   # yfinance 为负值（现金流出）
            ocf = _at(cf, "Operating Cash Flow", col)
            debt = _at(bs, "Total Debt", col)
            cash = _at(bs, "Cash And Cash Equivalents", col)
            rows.append({
                "code": code,
                "quarter_end": pd.to_datetime(col).strftime("%Y%m%d"),
                "revenue": rev, "net_income": ni,
                "gross_margin": (gp / rev) if (gp is not None and rev) else None,
                "fcf": fcf,
                "capex": abs(capex) if capex is not None else None,
                "ocf": ocf,
                "net_debt": (debt - cash) if (debt is not None and cash is not None) else None,
            })
        return pd.DataFrame(rows)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"us fundamentals {code} 失败（降级为纯价格信号）：{e}")
        return pd.DataFrame()


def fetch_option_chain(code: str, dte_min: int, dte_max: int) -> pd.DataFrame:
    """期权链快照：窗口内全部到期日的 puts+calls 长表（供打分器筛选）。

    列：code/strategy_side(put|call)/expiry/strike/bid/ask/iv/open_interest。
    仅当日快照——期权历史不可回溯，故造血模块定位是"建议输出"而非回测对象。
    """
    try:
        t = _yf().Ticker(code)
        today = pd.Timestamp.utcnow().tz_localize(None).normalize()
        rows = []
        for exp in (t.options or []):
            dte = (pd.to_datetime(exp) - today).days
            if not (dte_min <= dte <= dte_max):
                continue
            chain = t.option_chain(exp)
            for side, df in (("put", chain.puts), ("call", chain.calls)):
                if df is None or df.empty:
                    continue
                for _, r in df.iterrows():
                    rows.append({
                        "code": code, "strategy_side": side,
                        "expiry": pd.to_datetime(exp).strftime("%Y%m%d"),
                        "dte": int(dte),
                        "strike": float(r["strike"]),
                        "bid": float(r.get("bid") or 0),
                        "ask": float(r.get("ask") or 0),
                        "iv": float(r.get("impliedVolatility") or 0),
                        "open_interest": float(r.get("openInterest") or 0),
                    })
        return pd.DataFrame(rows)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"us options {code} 失败（本日无该标的期权候选）：{e}")
        return pd.DataFrame()


def fetch_valuation(code: str) -> dict:
    """估值快照（V2）：市值/净现金/TTM FCF/TTM 净利——回本周期引擎的原料。

    来源 yfinance info（TTM 口径），字段缺失逐项置 None，由 valuation.payback_years
    保守处理。ETF/指数返回空 dict（估值锚只适用于个股）。
    """
    try:
        info = _yf().Ticker(code).info or {}
        if info.get("quoteType") == "ETF" or code.startswith("^"):
            return {}
        cash = info.get("totalCash")
        debt = info.get("totalDebt")
        return {
            "market_cap": info.get("marketCap"),
            "net_cash": (cash - debt) if (cash is not None and debt is not None) else cash,
            "fcf_ttm": info.get("freeCashflow"),
            "ni_ttm": info.get("netIncomeToCommon"),
        }
    except Exception as e:  # noqa: BLE001
        logger.warning(f"us valuation {code} 失败（该标的按 unknown 档处理）：{e}")
        return {}


def fetch_next_earnings(code: str) -> str | None:
    """下一次财报日（YYYYMMDD），yfinance calendar 口径；取不到返 None（不阻断）。"""
    try:
        cal = _yf().Ticker(code).calendar or {}
        dates = cal.get("Earnings Date") or []
        if dates:
            return pd.to_datetime(dates[0]).strftime("%Y%m%d")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"us earnings {code} 获取失败（不标注跨财报）：{e}")
    return None
