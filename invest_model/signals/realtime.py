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
    """股票实时价：Tushare rt_k 为主，缺失部分依次用 腾讯 qt.gtimg → 东财 push2 补齐。

    背景（2026-07-23 盯盘失明事故）：rt_k 代理自 0721 午后对 FC 出口近乎全挂
    （诊断 rt命中=1/70），盯盘取不到价逐只静默跳过 → 两个交易日零预警零报错。
    单源不可靠，改三级回退；任一源部分成功都按 code 粒度合并。
    """
    codes = list(dict.fromkeys(codes))
    out = _query_rt(codes, "rt_k")
    missing = [c for c in codes if c not in out]
    if missing:
        out.update(get_realtime_market(missing))          # 腾讯（fear_intraday 同源）
    missing = [c for c in codes if c not in out]
    if missing:
        out.update(_eastmoney_rt(missing))                # 东财兜底
    return out


def _eastmoney_rt(codes: list[str]) -> dict[str, dict]:
    """东方财富 push2 批量现价（无鉴权 JSON）。fltt=2 → 价格为小数。

    secid：沪=1.代码 / 深=0.代码；字段 f2现价 f5总量 f12代码 f14名称
    f15高 f16低 f17开 f18昨收。失败/字段缺整体返回已得部分（fail-open）。
    """
    codes = list(dict.fromkeys(codes))
    if not codes:
        return {}
    import requests

    out: dict[str, dict] = {}
    for i in range(0, len(codes), 80):
        batch = codes[i:i + 80]
        secids = ",".join(("1." if c.endswith(".SH") else "0.") + c.split(".")[0]
                          for c in batch)
        rev = {("1." if c.endswith(".SH") else "0.") + c.split(".")[0]: c for c in batch}
        try:
            r = requests.get(
                "https://push2.eastmoney.com/api/qt/ulist.np/get",
                params={"fltt": 2, "secids": secids,
                        "fields": "f2,f5,f12,f13,f14,f15,f16,f17,f18"},
                timeout=15)
            rows = ((r.json() or {}).get("data") or {}).get("diff") or []
        except Exception:  # noqa: BLE001
            continue
        for d in rows:
            try:
                key = f"{int(d.get('f13'))}.{d.get('f12')}"
                ts = rev.get(key)
                px = float(d.get("f2"))
                if not ts or not px or px != px:
                    continue
                out[ts] = {
                    "price": px,
                    "pre_close": float(d.get("f18") or float("nan")),
                    "open": float(d.get("f17") or float("nan")),
                    "high": float(d.get("f15") or float("nan")),
                    "low": float(d.get("f16") or float("nan")),
                    "vol": float(d.get("f5") or float("nan")),
                    "name": d.get("f14"),
                }
            except (TypeError, ValueError):
                continue
    return out


def _to_qt_code(ts_code: str) -> str:
    """000833.SZ -> sz000833；516120.SH -> sh516120（腾讯/新浪的市场前缀格式）。"""
    num, _, ex = ts_code.partition(".")
    return ("sh" if ex.upper() == "SH" else "sz") + num


def _parse_qt(text: str, rev: dict[str, str]) -> dict[str, dict]:
    """解析腾讯 qt.gtimg.cn 返回体（~ 分隔，GBK）。rev: qt_code→ts_code。"""
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
        ts_code = rev.get(var.strip().replace("v_", ""))
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
        out = _parse_qt(resp.text, rev)
    except Exception:  # noqa: BLE001
        out = {}
    missing = [c for c in codes if c not in out]
    if missing:                               # 0723 事故同批加固：ETF 也给东财兜底
        out.update(_eastmoney_rt(missing))
    return out


def get_realtime_market(codes: list[str], chunk: int = 60) -> dict[str, dict]:
    """全市场股票盘中现价：腾讯免费源分批拉取（无鉴权、独立于 minitick/Tushare）。

    用于盘中恐慌指数全量重算（~5000 只 / 60 一批 ≈ 85 请求）。腾讯源覆盖 A 股+ETF，
    单批失败只跳过该批（fail-open）；返回 {ts_code: {price, pre_close, ...}}。
    受限容器可能被代理 403（本地测试请注入 price_map，勿依赖真实拉取）；
    FC/Actions runner 外网可达。
    """
    codes = list(dict.fromkeys(codes))
    if not codes:
        return {}
    import requests

    out: dict[str, dict] = {}
    for i in range(0, len(codes), chunk):
        batch = codes[i:i + chunk]
        qt = [_to_qt_code(c) for c in batch]
        rev = dict(zip(qt, batch))
        try:
            resp = requests.get("https://qt.gtimg.cn/q=" + ",".join(qt), timeout=15)
            resp.encoding = "gbk"
            out.update(_parse_qt(resp.text, rev))
        except Exception:  # noqa: BLE001
            continue                          # 单批失败跳过，不中断整轮
    return out
