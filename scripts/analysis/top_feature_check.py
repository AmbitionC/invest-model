"""当日持仓·顶部特征即时体检——确认 P16 自动减半在生产上是否/对谁触发。

对当前持仓逐票（前复权收盘 + 原始量）跑 top_feature_now，打印每票的分项判定：
浮盈是否达标、20日波动分位、5/60量比、是否触发。只读、不落库、不动仓。
需在有 DB 的环境（Actions/FC）跑：
  python scripts/analysis/top_feature_check.py [--db ...]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.data.adjust import qfq_close_hist  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.repositories.holding_repo import HoldingRepo  # noqa: E402
from invest_model.signals.top_feature import (  # noqa: E402
    top_feature_now, TOP_VOL_PCTL, TOP_VOL_WIN, TOP_VOL_LOOKBACK,
    TOP_VOLUME_RATIO, TOP_MIN_PROFIT, PEAK_FALLBACK_WIN,
)


def _diag(close: pd.Series, volume: pd.Series, cost: float, entry: str | None) -> dict:
    c = pd.to_numeric(close, errors="coerce").dropna()
    out = {"n": len(c), "fire": False, "peak_profit": None, "vol_rank": None, "vratio": None}
    if len(c) < 80 or cost <= 0:
        return out
    if entry:
        since = c[c.index >= str(entry)]
        peak = float(since.max()) if not since.empty else float(c.iloc[-PEAK_FALLBACK_WIN:].max())
    else:
        peak = float(c.iloc[-PEAK_FALLBACK_WIN:].max())
    out["peak_profit"] = peak / cost - 1
    ret = c.pct_change()
    vol20 = ret.rolling(TOP_VOL_WIN).std() * np.sqrt(250)
    look = vol20.iloc[-TOP_VOL_LOOKBACK:].dropna()
    if len(look) >= 60 and np.isfinite(vol20.iloc[-1]):
        out["vol_rank"] = float((look <= vol20.iloc[-1]).mean())
    v = pd.to_numeric(volume, errors="coerce")
    if v.notna().sum() > 60:
        v5, v60 = v.rolling(5).mean().iloc[-1], v.rolling(60).mean().iloc[-1]
        if np.isfinite(v5) and np.isfinite(v60) and v60 > 0:
            out["vratio"] = float(v5 / v60)
    out["fire"] = bool(top_feature_now(close, volume, cost, entry))
    return out


def run(repo: BaseRepository) -> str:
    dt = repo.read_sql("SELECT MAX(trade_date) AS d FROM stock_daily")["d"].iloc[0]
    hold = HoldingRepo(repo).get_all()
    L = [f"# 当日持仓·顶部特征体检（数据日 {dt}）", ""]
    L.append(f"判据：浮盈曾达 ≥{TOP_MIN_PROFIT:.0%} 且 20日波动≥近250日{TOP_VOL_PCTL:.0%}分位 且 5/60量比≥{TOP_VOLUME_RATIO}", )
    if hold is None or hold.empty:
        L.append("\n当前无持仓。")
        return "\n".join(L)
    start_lb = f"{int(str(dt)[:4]) - 2}{str(dt)[4:]}"
    L.append("\n| 代码 | 峰值浮盈 | 20日波动分位 | 5/60量比 | 触发减半? |")
    L.append("|---|---|---|---|---|")
    fired = []
    for _, h in hold.iterrows():
        code = str(h["code"])
        cost = float(h["cost_price"] or 0)
        entry = str(h["entry_date"] or "") or None
        close = qfq_close_hist(repo, code, start_lb, str(dt))
        vser = repo.read_sql(
            "SELECT trade_date, volume FROM stock_daily WHERE code=:c AND trade_date>=:s "
            "AND trade_date<=:d ORDER BY trade_date", {"c": code, "s": start_lb, "d": str(dt)})
        vol = (pd.to_numeric(vser.set_index("trade_date")["volume"], errors="coerce")
               if not vser.empty else pd.Series(dtype=float))
        vol.index = vol.index.astype(str)
        d = _diag(close, vol.reindex(close.index) if not close.empty else vol, cost, entry)
        pp = f"{d['peak_profit']:+.1%}" if d["peak_profit"] is not None else "—"
        vr = f"{d['vol_rank']:.0%}" if d["vol_rank"] is not None else "—"
        vt = f"{d['vratio']:.2f}" if d["vratio"] is not None else "—"
        L.append(f"| {code} | {pp} | {vr} | {vt} | {'✅ 是' if d['fire'] else '否'} |")
        if d["fire"]:
            fired.append(code)
    L.append(f"\n**今日触发自动减半：{('、'.join(fired)) if fired else '无（生产代码已跑通，只是当前无持仓命中顶部特征）'}**")
    L.append("（体检只读不动仓；实际减半由 build_action_plan 计划层执行，判据同此。）")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="当日持仓顶部特征体检")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())
    md = run(repo)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
