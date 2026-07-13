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


def fear_gauge(engine, dt: str | None = None, benchmark: str = "000300.SH",
               stock_df: pd.DataFrame | None = None,
               idx_df: pd.DataFrame | None = None) -> dict:
    """恐慌指数（0–100，越高越恐慌）。

    stock_df / idx_df：可选预载全量数据（列同各自 SELECT）。传入时按日期窗口切片，
    不再查库——供 backfill 批量回填一次性载入、逐日复用，避免每日一次全市场查询。
    """
    repo = BaseRepository(engine)
    if not dt:
        dt = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]

    # ── 指数：动量(距MA125) + 波动率(20日年化) ──
    istart = (pd.to_datetime(dt) - pd.Timedelta(days=420)).strftime("%Y%m%d")
    if idx_df is None:
        idx = repo.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
            {"c": benchmark, "s": istart, "d": dt})
    else:
        idx = idx_df[(idx_df["code"] == benchmark) & (idx_df["trade_date"] >= istart)
                     & (idx_df["trade_date"] <= dt)].sort_values("trade_date")
    c = pd.to_numeric(idx["close"], errors="coerce").dropna()
    ma125 = c.tail(125).mean() if len(c) >= 125 else c.mean()
    dev_mom = float(c.iloc[-1] / ma125 - 1) if ma125 else 0.0
    vol20 = float(c.pct_change().tail(20).std() * np.sqrt(250)) if len(c) >= 21 else 0.20
    idx_chg = float(c.pct_change().iloc[-1]) if len(c) >= 2 else 0.0

    # ── 全市场：宽度 + 涨跌停 + 新高新低 ──
    sstart = (pd.to_datetime(dt) - pd.Timedelta(days=200)).strftime("%Y%m%d")
    if stock_df is None:
        df = repo.read_sql(
            "SELECT code, trade_date, close, pct_chg FROM stock_daily WHERE trade_date>=:s AND trade_date<=:d",
            {"s": sstart, "d": dt})
    else:
        df = stock_df[(stock_df["trade_date"] >= sstart)
                      & (stock_df["trade_date"] <= dt)].copy()
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


def fear_intraday(engine, price_map: dict, idx_price: float | None, dt: str,
                  benchmark: str = "000300.SH", min_stocks: int = 2000) -> dict | None:
    """盘中恐慌指数：用当前现价拼一行「今日」，喂给 fear_gauge 走**同一套分量公式**。

    与日频 fear_gauge 的唯一差别：今日那行收盘价换成盘中现价（price_map，腾讯全市场
    快照），历史行仍取自 stock_daily/index_daily。这样盘中值与收盘值口径完全一致、
    可直接比较，只是"当日"未定格。

      price_map：{ts_code: {price, pre_close, ...}}（get_realtime_market 结果）
      idx_price：基准指数盘中点位（None 则该指数动量/波动仍用最近收盘近似）
      dt：交易日 YYYYMMDD（今日）
      min_stocks：有效现价少于此值判定拉取降级 → 返回 None（前端退回日频兜底）

    返回结构同 fear_gauge()（date/score/level/components/raw），失败或降级返回 None。
    """
    repo = BaseRepository(engine)
    # 有效现价行（price>0 且有 pre_close 算涨跌幅）
    rows = []
    for code, q in (price_map or {}).items():
        px = q.get("price"); pc = q.get("pre_close")
        if not (px and px == px and px > 0):
            continue
        pct = (px / pc - 1) * 100 if (pc and pc == pc and pc > 0) else float("nan")
        rows.append({"code": code, "trade_date": dt, "close": px, "pct_chg": pct})
    if len(rows) < min_stocks:
        return None                            # 拉取不足 → 降级，不写脏数据

    sstart = (pd.to_datetime(dt) - pd.Timedelta(days=200)).strftime("%Y%m%d")
    hist = repo.read_sql(
        "SELECT code, trade_date, close, pct_chg FROM stock_daily "
        "WHERE trade_date>=:s AND trade_date<:d", {"s": sstart, "d": dt})
    stock_df = pd.concat([hist, pd.DataFrame(rows)], ignore_index=True)

    istart = (pd.to_datetime(dt) - pd.Timedelta(days=420)).strftime("%Y%m%d")
    ihist = repo.read_sql(
        "SELECT code, trade_date, close FROM index_daily "
        "WHERE code=:c AND trade_date>=:s AND trade_date<:d",
        {"c": benchmark, "s": istart, "d": dt})
    if idx_price and idx_price == idx_price and idx_price > 0:
        ihist = pd.concat(
            [ihist, pd.DataFrame([{"code": benchmark, "trade_date": dt, "close": idx_price}])],
            ignore_index=True)
    idx_df = ihist if not ihist.empty else None

    g = fear_gauge(engine, dt=dt, benchmark=benchmark, stock_df=stock_df, idx_df=idx_df)
    g["intraday"] = True
    return g


def format_fear(g: dict) -> str:
    c = g["components"]
    comp = " / ".join(f"{k}{v:.0f}" for k, v in c.items())
    r = g["raw"]
    return (f"🌡️ 恐慌指数 {g['score']}/100 — {g['level']}\n"
            f"   分量: {comp}（越高越恐慌）\n"
            f"   涨/跌停 {r['limit_up']}/{r['limit_down']} · 站上MA20 {r['breadth_ma20']:.0%} · "
            f"120日新高/新低 {r['new_high']}/{r['new_low']} · 指数20日波动 {r['vol20']:.0%}")
