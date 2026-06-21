"""实时行情：通过 Tushare 代理的 rt_k 接口取现价（盘中可用；免迁 MySQL）。

环境网络白名单只放行 Tushare 代理（minitick），新浪/东财/腾讯实时源被 403 拦截，
故统一走 ``pro.query('rt_k')``。返回 {code: {price, pre_close, open, high, low, vol}}。
"""

from __future__ import annotations

import pandas as pd


def get_realtime(codes: list[str]) -> dict[str, dict]:
    from invest_model.sources.tushare_client import TushareClient

    codes = list(dict.fromkeys(codes))
    if not codes:
        return {}
    pro = TushareClient().pro
    out: dict[str, dict] = {}
    for i in range(0, len(codes), 50):
        batch = codes[i:i + 50]
        try:
            df = pro.query("rt_k", ts_code=",".join(batch))
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
