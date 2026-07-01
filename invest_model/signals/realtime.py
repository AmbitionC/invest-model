"""实时行情：盘中现价（免迁 MySQL）。

- 股票：走 Tushare 代理 ``pro.query('rt_k')``（本环境白名单放行 minitick 代理）。
- ETF：Tushare rt_k 不返回 ETF、rt_etf_k 需额外权限，故改用**腾讯免费源**
  ``qt.gtimg.cn``（覆盖 A 股+ETF，无需鉴权/Referer）。免费源在开放外网可达
  （GitHub Actions runner 即可；本地受限容器可能被代理 403，不影响 Actions 运行）。

两者字段统一：{code: {price, pre_close, open, high, low, vol, name}}。
"""

from __future__ import annotations

import pandas as pd


def _query_rt(codes: list[str], endpoint: str) -> dict[str, dict]:
    """按 endpoint（rt_k）批量取实时价（Tushare 代理），字段统一。失败批次跳过。"""
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
    """股票实时价（Tushare rt_k）。"""
    return _query_rt(codes, "rt_k")


def _to_qt_code(ts_code: str) -> str:
    """000833.SZ -> sz000833；516120.SH -> sh516120（腾讯/新浪的市场前缀格式）。"""
    num, _, ex = ts_code.partition(".")
    return ("sh" if ex.upper() == "SH" else "sz") + num


def get_realtime_etf(codes: list[str]) -> dict[str, dict]:
    """ETF 实时价：腾讯免费源 qt.gtimg.cn（无需鉴权）。

    返回体形如 ``v_sh516120="1~化工ETF~516120~现价~昨收~今开~量~…~最高~最低~…";``
    （~ 分隔，GBK 编码）。取现价/昨收即可满足风控（涨跌、浮盈亏、破MA20）。
    """
    codes = list(dict.fromkeys(codes))
    if not codes:
        return {}
    import requests

    qt = [_to_qt_code(c) for c in codes]
    rev = dict(zip(qt, codes))
    try:
        resp = requests.get("https://qt.gtimg.cn/q=" + ",".join(qt), timeout=15)
        resp.encoding = "gbk"                 # 腾讯返回 GBK（中文名）
        text = resp.text
    except Exception:  # noqa: BLE001
        return {}

    def _f(parts: list[str], i: int) -> float:
        try:
            return float(parts[i])
        except (ValueError, IndexError):
            return float("nan")

    out: dict[str, dict] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        var, _, payload = line.partition("=")
        qcode = var.strip().replace("v_", "")
        ts_code = rev.get(qcode)
        if not ts_code:
            continue
        parts = payload.strip().strip(";").strip('"').split("~")
        if len(parts) < 6:
            continue
        price = _f(parts, 3)
        if not price or price != price:       # 无有效现价（停牌/取数异常）跳过
            continue
        out[ts_code] = {
            "price": price,
            "pre_close": _f(parts, 4),
            "open": _f(parts, 5),
            "high": _f(parts, 33),
            "low": _f(parts, 34),
            "vol": _f(parts, 6),
            "name": parts[1],
        }
    return out
