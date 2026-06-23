"""A 股恐慌/情绪代理指数（0–100，越高越恐慌）。

对齐 CNN Fear&Greed 框架里"用我们数据能算"的部分（无期权/垃圾债/国债，故不含
put-call、信用利差、避险需求；A 股以涨跌停替代期权情绪）。按日（收盘）计算，
是慢变量、市场体温，不是 tick 实时——真·实时恐慌需期权 iVIX，我们无数据。

5 个等权分量（各 0–100，100=极恐慌）：
  1) 动量      指数距 MA125（在均线上方=贪婪/低恐慌）
  2) 波动率    指数 20 日年化已实现波动（高=恐慌；VIX 代理）
  3) 宽度      全市场站上 MA20 占比（低=超卖/恐慌）
  4) 涨跌停    跌停/(涨停+跌停)（跌停占比高=恐慌）
  5) 新高新低  120 日新低家数 vs 新高家数（新低多=恐慌）

合成分 = 5 项均值。分档：<25 极贪婪 / 25–45 偏热 / 45–55 中性 /
55–75 偏恐慌 / >75 极度恐慌（抄底观察区）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.repositories.base import BaseRepository


def _lin(x: float, lo: float, hi: float) -> float:
    """把 x 从 [lo,hi] 线性映射到 [0,100] 并截断（lo→0, hi→100）。"""
    if hi == lo:
        return 50.0
    return float(np.clip((x - lo) / (hi - lo) * 100.0, 0.0, 100.0))


def fear_gauge(engine, dt: str | None = None, benchmark: str = "000300.SH") -> dict:
    repo = BaseRepository(engine)
    if not dt:
        dt = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]

    # ── 指数：动量(距MA125) + 波动率(20日年化) ──
    istart = (pd.to_datetime(dt) - pd.Timedelta(days=420)).strftime("%Y%m%d")
    idx = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
        {"c": benchmark, "s": istart, "d": dt})
    c = pd.to_numeric(idx["close"], errors="coerce").dropna()
    ma125 = c.tail(125).mean() if len(c) >= 125 else c.mean()
    dev_mom = float(c.iloc[-1] / ma125 - 1) if ma125 else 0.0
    vol20 = float(c.pct_change().tail(20).std() * np.sqrt(250)) if len(c) >= 21 else 0.20
    idx_chg = float(c.pct_change().iloc[-1]) if len(c) >= 2 else 0.0

    # ── 全市场：宽度 + 涨跌停 + 新高新低 ──
    sstart = (pd.to_datetime(dt) - pd.Timedelta(days=200)).strftime("%Y%m%d")
    df = repo.read_sql(
        "SELECT code, trade_date, close, pct_chg FROM stock_daily WHERE trade_date>=:s AND trade_date<=:d",
        {"s": sstart, "d": dt})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
    today = df[df["trade_date"] == dt].dropna(subset=["pct_chg"])
    n = len(today)
    lu = int((today["pct_chg"] >= 9.8).sum()); ld = int((today["pct_chg"] <= -9.8).sum())
    up = int((today["pct_chg"] > 0).sum()); down = int((today["pct_chg"] < 0).sum())
    limit_ratio = ld / (lu + ld) if (lu + ld) else 0.0

    piv = df.pivot(index="trade_date", columns="code", values="close").sort_index()
    last = piv.iloc[-1]
    ma20 = piv.tail(20).mean()
    b20 = float((last >= ma20).mean())
    win = piv.tail(120)
    valid = last.notna() & (win.count() >= 60)
    nh = int(((last >= win.max()) & valid).sum())
    nl = int(((last <= win.min()) & valid).sum())
    nv = int(valid.sum()) or 1
    netlow = (nl - nh) / nv

    # ── 各分量 → 0–100 恐慌 ──
    f_mom = _lin(-dev_mom, -0.10, 0.15)          # 在MA125上方10%→0; 下方15%→100
    f_vol = _lin(vol20, 0.15, 0.35)              # 15%→0; 35%→100
    f_breadth = _lin(-b20, -0.70, -0.15)         # 70%站上→0; 15%站上→100
    f_limit = _lin(limit_ratio, 0.20, 0.80)      # 跌停占比20%→0; 80%→100
    f_hl = _lin(netlow, -0.10, 0.30)             # 新高多→0; 新低多30%→100
    comps = {"动量": f_mom, "波动率": f_vol, "宽度": f_breadth, "涨跌停": f_limit, "新高新低": f_hl}
    score = float(np.mean(list(comps.values())))

    if score >= 75:
        level = "极度恐慌（抄底观察区）"
    elif score >= 55:
        level = "偏恐慌"
    elif score >= 45:
        level = "中性"
    elif score >= 25:
        level = "偏热/贪婪"
    else:
        level = "极度贪婪（高拥挤，收紧）"

    return {
        "date": dt, "score": round(score, 1), "level": level,
        "components": {k: round(v, 1) for k, v in comps.items()},
        "raw": {"idx_chg": round(idx_chg, 4), "dev_ma125": round(dev_mom, 4),
                "vol20": round(vol20, 4), "breadth_ma20": round(b20, 4),
                "up": up, "down": down, "limit_up": lu, "limit_down": ld,
                "new_high": nh, "new_low": nl, "n": n},
    }


def format_fear(g: dict) -> str:
    c = g["components"]
    comp = " / ".join(f"{k}{v:.0f}" for k, v in c.items())
    r = g["raw"]
    return (f"🌡️ 恐慌指数 {g['score']}/100 — {g['level']}\n"
            f"   分量: {comp}（越高越恐慌）\n"
            f"   涨/跌停 {r['limit_up']}/{r['limit_down']} · 站上MA20 {r['breadth_ma20']:.0%} · "
            f"120日新高/新低 {r['new_high']}/{r['new_low']} · 指数20日波动 {r['vol20']:.0%}")
