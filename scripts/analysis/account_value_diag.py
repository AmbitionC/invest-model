"""账户市值对账诊断（2026-07-17 用户报告网站持仓市值不对·只读不落库）。

逐票重算：current_holding × 最新可用收盘（标注每票实际用的价格日期与来源），
与 account_snapshot 最新行对账；同时打印 holding_snapshot 最新日，
用于区分四类可能根因：
  ① current_holding 落后于真实持仓（如清仓已执行但截图未上传）
  ② 个别票收盘价缺失/陈旧（如 ETF 行情 timer 丢失 → 回退旧收盘）
  ③ 现金沿用旧快照（卖出回笼未入账）
  ④ 重估口径缺口（转债固定并入等）

用法（Actions）：python scripts/analysis/account_value_diag.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


def main() -> None:
    repo = BaseRepository(make_engine())
    latest = str(repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0])
    print(f"## 账户市值对账诊断（stock_daily 最新行情日 = {latest}）\n")

    snaps = repo.read_sql(
        "SELECT snapshot_date, cash, market_value, total_asset FROM account_snapshot "
        "ORDER BY snapshot_date DESC LIMIT 6")
    print("### account_snapshot 最近 6 行（网站总资产/市值直接读第一行）\n")
    print(snaps.to_string(index=False), "\n")

    hs_date = repo.read_sql("SELECT MAX(snapshot_date) d FROM holding_snapshot")["d"].iloc[0]
    print(f"### holding_snapshot 最新上传日 = {hs_date}（现金/转债/持仓的人工权威源）\n")

    ch = repo.read_sql("SELECT code, shares, cost_price FROM current_holding ORDER BY code")
    if ch.empty:
        print("current_holding 为空")
        return
    info = repo.read_sql("SELECT ts_code, name FROM stock_info")
    nm = dict(zip(info["ts_code"].astype(str), info["name"]))

    rows, total = [], 0.0
    for _, h in ch.iterrows():
        c, sh = str(h["code"]), float(h["shares"] or 0)
        px_df = repo.read_sql(
            "SELECT trade_date, close FROM stock_daily WHERE code=:c AND close IS NOT NULL "
            "AND trade_date<=:d ORDER BY trade_date DESC LIMIT 1", {"c": c, "d": latest})
        if not px_df.empty:
            px, pxd, src = float(px_df["close"].iloc[0]), str(px_df["trade_date"].iloc[0]), "stock_daily"
        else:
            fb = repo.read_sql(
                "SELECT snapshot_date, last_price FROM holding_snapshot WHERE code=:c "
                "AND last_price IS NOT NULL ORDER BY snapshot_date DESC LIMIT 1", {"c": c})
            if not fb.empty:
                px, pxd, src = float(fb["last_price"].iloc[0]), str(fb["snapshot_date"].iloc[0]), "券商快照"
            else:
                px, pxd, src = 0.0, "-", "无价"
        v = px * sh
        total += v
        stale = "⚠️陈旧" if pxd != latest else ""
        rows.append((c, nm.get(c, "?"), sh, px, pxd + stale, src, v))

    print("### current_holding 逐票重算（网站市值应≈这些之和 + 转债 + 现金）\n")
    print(f"{'代码':<12}{'名称':<10}{'股数':>10}{'用价':>10}  {'价格日':<14}{'来源':<10}{'市值':>12}")
    for r in rows:
        print(f"{r[0]:<12}{r[1]:<10}{r[2]:>10.0f}{r[3]:>10.3f}  {r[4]:<14}{r[5]:<10}{r[6]:>12.0f}")
    bond = repo.read_sql(
        "SELECT SUM(market_value) v FROM holding_snapshot "
        "WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM holding_snapshot) "
        "AND LOWER(asset_type)='bond'")
    bond_v = float(bond["v"].iloc[0]) if bond["v"].iloc[0] is not None else 0.0
    cash = float(snaps["cash"].iloc[0]) if not snaps.empty else 0.0
    print(f"\n重算 stock+etf 市值 = {total:,.0f}")
    print(f"转债(最新快照固定) = {bond_v:,.0f} ｜ 现金(最新快照沿用) = {cash:,.0f}")
    print(f"重算总资产 = {total + bond_v + cash:,.0f}")
    if not snaps.empty:
        mv, ta = float(snaps["market_value"].iloc[0]), float(snaps["total_asset"].iloc[0])
        print(f"account_snapshot 最新行: market_value={mv:,.0f} total_asset={ta:,.0f}")
        print(f"**差异: market_value {total + bond_v - mv:+,.0f} ｜ total_asset "
              f"{total + bond_v + cash - ta:+,.0f}**")

    # ── 持仓风控对账：用生产 risk.py 逐票重放，解释"为什么给/没给清仓" ──
    from invest_model.data.adjust import qfq_close_hist
    from invest_model.portfolio.risk import RiskConfig, evaluate_holding, profit_protect
    import os
    cfg = RiskConfig()
    ch2 = repo.read_sql("SELECT code, shares, cost_price, entry_date FROM current_holding ORDER BY code")
    print("\n### 持仓风控逐票重放（生产 evaluate_holding + profit_protect，prev_tier=0 新鲜视角）\n")
    for _, h in ch2.iterrows():
        c, cost = str(h["code"]), float(h["cost_price"] or 0)
        hist = qfq_close_hist(repo, c, "20250801", latest)
        if hist.empty or not cost:
            print(f"{c}: 无历史/无成本，跳过")
            continue
        px = float(hist.iloc[-1])
        ma20 = float(hist.rolling(20).mean().iloc[-1])
        ma60 = float(hist.rolling(60).mean().iloc[-1]) if len(hist) >= 60 else float("nan")
        pnl = px / cost - 1
        dec = evaluate_holding(hist, cost, cfg, prev_tier=0)
        ed = str(h.get("entry_date") or "")
        hold_hist = hist.loc[ed:] if ed and ed in hist.index else hist
        pp = profit_protect(hold_hist, cost, cfg, prev_tier=0)
        print(f"{c} {nm.get(c,'?'):<8} 现价{px:.3f} 成本{cost:.3f} 盈亏{pnl:+.1%} "
              f"MA20={ma20:.3f}({'上方' if px >= ma20*0.995 else '下方'}) "
              f"MA60={ma60:.3f} → 动作={dec.action} 理由={dec.reason or '持有'} "
              f"盈利保护={pp.action + '/' + (pp.reason or '') if pp is not None else '未武装'}")


if __name__ == "__main__":
    main()
