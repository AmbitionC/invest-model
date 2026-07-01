"""实时行情：通过 Tushare 代理取盘中现价（免迁 MySQL）。

环境网络白名单只放行 Tushare 代理（minitick），新浪/东财/腾讯实时源被 403 拦截，
故走 ``pro.query`` 的实时端点：股票用 ``rt_k``、ETF 用 ``rt_etf_k``（rt_k 不返回 ETF）。
两端点字段一致，返回 {code: {price, pre_close, open, high, low, vol, name}}。
"""

from __future__ import annotations

import pandas as pd


def _query_rt(codes: list[str], endpoint: str) -> dict[str, dict]:
    """按 endpoint（rt_k / rt_etf_k）批量取实时价，字段统一。取数失败的批次跳过。"""
    from invest_model.sources.tushare_client import TushareClient

    codes = list(dict.fromkeys(codes))
    if not codes:
        return {}
    pro = TushareClient().pro
    out: dict[str, dict] = {}
    for i in range(0, len(codes), 50):
        batch = codes[i:i + 50]
        try:
            df = pro.query(endpoint, ts_code=",".join(batch))
        except Exception:  # noqa: BLE001
            continue
        if df is None or df.empty:
            continue
        for _, r in df.iterrows():
            out[r["ts_code"]] = {
                "price": float(pd.to_numeric(r.get("close"), errors="coerce")),
                "pre_close": float(pd.to_numeric(r.get("pre_close"), errors="coerce")),
                "open": float(pd.to_numeric(r.get("open"), errors="coerce")),
                "high": float(pd.to_numeric(r.get("high"), errors="coerce")),
                "low": float(pd.to_numeric(r.get("low"), errors="coerce")),
                "vol": float(pd.to_numeric(r.get("vol"), errors="coerce")),
                "name": r.get("name"),
            }
    return out


def get_realtime(codes: list[str]) -> dict[str, dict]:
    """股票实时价（rt_k）。"""
    return _query_rt(codes, "rt_k")


def get_realtime_etf(codes: list[str]) -> dict[str, dict]:
    """ETF 实时价（rt_etf_k）。rt_k 不返回 ETF，故 ETF 必须走此端点。"""
    return _query_rt(codes, "rt_etf_k")


def rt_etf_probe(code: str) -> tuple[int, str]:
    """诊断：直接调 rt_etf_k 并**不吞异常**，返回 (行数, 错误串)。

    用于区分「ETF 取不到价」到底是 token 无该接口权限/积分不足（有 error），
    还是接口正常但暂无数据（行数 0、error 空）。仅自检用，不进主链路。
    """
    from invest_model.sources.tushare_client import TushareClient

    try:
        df = TushareClient().pro.query("rt_etf_k", ts_code=code)
    except Exception as e:  # noqa: BLE001
        return (0, repr(e)[:220])
    return (0 if df is None else len(df), "")
