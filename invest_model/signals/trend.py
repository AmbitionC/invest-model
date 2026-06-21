"""左侧趋势过滤闸：在调仓日按 MA60 走平/向上筛候选（投顾与量化候选共用）。

一次性查全部候选近 ~120 日收盘，向量化判定，避免逐票查询。
"""

from __future__ import annotations

import pandas as pd

from invest_model.portfolio.risk import RiskConfig, trend_ok_close
from invest_model.repositories.base import BaseRepository


def trend_ok(engine, dt: str, codes: list[str], cfg: RiskConfig | None = None) -> set[str]:
    """返回 codes 中通过左侧趋势过滤（MA60 走平/向上且站上 MA60）的子集。

    数据不足的票默认放行（交由其它闸/风控兜底）。
    """
    cfg = cfg or RiskConfig()
    codes = list(dict.fromkeys(codes))
    if not codes:
        return set()
    repo = BaseRepository(engine)
    start = (pd.to_datetime(dt) - pd.Timedelta(days=cfg.trend_ma * 2 + 30)).strftime("%Y%m%d")
    # IN 子句分批，避免超长参数
    frames = []
    for i in range(0, len(codes), 800):
        batch = codes[i:i + 800]
        ph = ",".join(f":c{j}" for j in range(len(batch)))
        params = {f"c{j}": c for j, c in enumerate(batch)}
        params.update(s=start, d=dt)
        frames.append(repo.read_sql(
            f"SELECT code, trade_date, close FROM stock_daily "
            f"WHERE trade_date>=:s AND trade_date<=:d AND code IN ({ph})",
            params,
        ))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return set(codes)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    ok: set[str] = set()
    for code, g in df.sort_values("trade_date").groupby("code"):
        if trend_ok_close(g["close"], cfg):
            ok.add(code)
    # 完全无行情的票（不在 df 里）默认放行
    ok |= {c for c in codes if c not in set(df["code"])}
    return ok
