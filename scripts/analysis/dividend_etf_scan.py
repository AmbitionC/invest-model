"""老登/红利防御 ETF 趋势与买点位扫描（2026-07-20 用户命题·只读不落库）。

对券商/银行/非银/红利/红利低波/煤炭/电力等防御方向 ETF，用 tushare 前复权
（fund_daily × fund_adj）拉近 ~130 交易日，计算 MA20/MA60、趋势排列、
相对 MA20 的位置，判定「回踩买点区 / 偏离上方(追高等回踩) / 破位下方」。

口径对齐系统 BuyPointConfig：趋势 MA60、回踩贴合带 pullback_pct=3%
（|close/MA20−1|≤3% 视为回踩买点区）。仅打印+artifact，不改任何生产数据。

用法（Actions，需 TUSHARE_TOKEN）：python scripts/analysis/dividend_etf_scan.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.sources.tushare_client import TushareClient  # noqa: E402

# 老登（大金融）+ 红利/公用事业防御篮子（投顾四源共振方向）
BASKET = {
    "512880.SH": "证券ETF·老登-券商",
    "512800.SH": "银行ETF·老登-银行",
    "512070.SH": "非银ETF·老登-券商+保险",
    "510880.SH": "红利ETF·上证红利",
    "512890.SH": "红利低波ETF·中证红利低波(≈E10 H30269)",
    "515220.SH": "煤炭ETF·高股息避险",
    "159611.SZ": "电力ETF·公用事业避险",
}
PULLBACK = 0.03  # 与 BuyPointConfig.pullback_pct 同源


def _qfq(cli: TushareClient, code: str, start: str, end: str) -> pd.DataFrame:
    """前复权日线：fund_daily 原始收盘 × (adj_factor / 最新 adj_factor)。"""
    raw = cli.get_etf_daily(code, start, end)
    if raw is None or raw.empty:
        return pd.DataFrame()
    raw = raw[["trade_date", "close"]].copy()
    raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    try:
        adj = cli.pro.fund_adj(ts_code=code, start_date=start, end_date=end)
    except Exception:  # noqa: BLE001
        adj = None
    df = raw
    if adj is not None and not adj.empty:
        adj = adj.rename(columns={"ts_code": "code"})[["trade_date", "adj_factor"]]
        adj["adj_factor"] = pd.to_numeric(adj["adj_factor"], errors="coerce")
        df = raw.merge(adj, on="trade_date", how="left")
        latest = df.sort_values("trade_date")["adj_factor"].dropna().iloc[-1]
        df["close"] = df["close"] * df["adj_factor"] / latest
    return df.sort_values("trade_date").reset_index(drop=True)


def main() -> None:
    cli = TushareClient()
    end = datetime.now(timezone.utc).strftime("%Y%m%d")
    start = (datetime.now(timezone.utc) - timedelta(days=260)).strftime("%Y%m%d")
    try:
        names = cli.get_etf_list()
        name_map = dict(zip(names["code"], names["name"])) if not names.empty else {}
    except Exception:  # noqa: BLE001
        name_map = {}

    L = ["## 老登/红利防御 ETF 趋势与买点位扫描",
         f"> 前复权(fund_adj)·截至 {end}·回踩带±{PULLBACK:.0%}(BuyPointConfig同源)·MA60趋势闸", "",
         "| 代码 | 名称 | 最新价 | MA20 | MA60 | 距MA20 | 距MA60 | 均线排列 | 位置判定 | 20日 | 60日 |",
         "|---|---|---:|---:|---:|---:|---:|---|---|---:|---:|"]
    for code, label in BASKET.items():
        df = _qfq(cli, code, start, end)
        nm = name_map.get(code, label.split("·")[0])
        if df.empty or len(df) < 60:
            L.append(f"| {code} | {nm} | — | — | — | — | — | 无数据/代码待核对 | — | — | — |")
            continue
        c = df["close"].dropna().reset_index(drop=True)
        last = float(c.iloc[-1])
        ma20 = float(c.tail(20).mean())
        ma60 = float(c.tail(60).mean())
        ma20_prev = float(c.tail(30).head(20).mean()) if len(c) >= 30 else ma20  # 10日前的MA20
        d20 = last / ma20 - 1
        d60 = last / ma60 - 1
        ma20_up = ma20 > ma20_prev
        arrange = ("多头(MA20>MA60↑)" if (ma20 > ma60 and ma20_up)
                   else "多头偏平" if ma20 > ma60
                   else "空头(MA20<MA60)")
        if abs(d20) <= PULLBACK:
            pos = "✅回踩买点区(贴MA20)"
        elif d20 > PULLBACK:
            pos = f"⚠️偏离上方(高MA20 {d20:+.1%}、等回踩)"
        else:
            pos = f"❌破位下方(低MA20 {d20:+.1%})"
        r20 = last / float(c.iloc[-21]) - 1 if len(c) > 21 else float("nan")
        r60 = last / float(c.iloc[-61]) - 1 if len(c) > 61 else float("nan")
        L.append(f"| {code} | {nm} | {last:.3f} | {ma20:.3f} | {ma60:.3f} "
                 f"| {d20:+.1%} | {d60:+.1%} | {arrange} | {pos} "
                 f"| {r20:+.1%} | {r60:+.1%} |")
    L += ["", "读法：**位置判定**是核心——回踩买点区=可分批进场；偏离上方=已追高、等回踩 MA20 再进；"
          "破位下方=趋势走坏、观望。均线排列多头+回踩=最优买点；空头排列即便回踩也需谨慎。",
          "口径限制：ETF 前复权已抹平分红台阶；趋势/买点为技术位参考，不含基本面与量能确认。"]
    md = "\n".join(L)
    print(md, flush=True)
    out = Path("results/dividend_etf_scan.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
