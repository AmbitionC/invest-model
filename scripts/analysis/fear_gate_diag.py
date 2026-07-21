"""恐慌放宽下"为何一个标的都没进"诊断（用户命题·只读不落库）。

用户观察：最近几天恐慌到极值、环境闸放宽了，却没有任何标的被判"建议买入"。
命题：是不是都卡在 MA60 以下未突破（买点前置闸①）？

本脚本对最近 N 个交易日，把**当日真实的观察池（投顾 A/B 级 long）+ watch_etf ETF +
当前持仓**一起喂给生产 `detect_buypoints`（同一函数、同一 fear 放松），逐票列出：
  收盘 / MA20 / MA60 / 是否站上MA60 / MA60是否上行 / 命中的闸（拒绝原因串）。
并做关键对照：**把环境闸完全打开（gross=1.0）再跑一遍**——若通过数仍为 0，则证明
"恐慌放松环境闸"根本不是约束点（恐慌只松①以外的环境闸④），真正卡点在①MA60/②买点。

买点闸链（detect_buypoints 内，顺序）：
  ① MA60 前置闸：c0≥MA60 且 MA60 上行，否则"左侧趋势不买"——**恐慌不放松这一闸**
  ② 买点：回踩 MA20(±3%)放量阳线 ∨ 突破新高阳线
  ③ 量化：rank_pct ≥ 阈值
  ④ 环境闸：gross ≥ 下限——**恐慌(≥75)只放松这一闸**（0.6→0.4）

只读 stock_daily/advisor_reco/预测表，不落库、不改生产。需在有 DB 的环境（Actions/FC）跑。
  python scripts/analysis/fear_gate_diag.py [--days 3] [--out results/fear_gate_diag.md]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.orchestration.action_plan import (  # noqa: E402
    _latest_data_date, _latest_pred_date, WATCH_POOL_CAP,
)
from invest_model.orchestration.closed_loop import ClosedLoop  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.repositories.holding_repo import HoldingRepo  # noqa: E402
from invest_model.signals.buypoint import BuyPointConfig, detect_buypoints  # noqa: E402
from invest_model.signals.fear import fear_gauge  # noqa: E402


def _watch_etf_codes() -> list[tuple[str, str]]:
    p = Path(__file__).resolve().parents[2] / "config" / "watch_etf.txt"
    if not p.exists():
        return []
    out = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        head = ln.split("#")[0].strip()
        if head:
            note = ln.split("#", 1)[1].strip() if "#" in ln else ""
            out.append((head.split()[0], note))
    return out


def _classify(reason: str) -> str:
    """把 BuyPoint.reason 归到闸。"""
    if "买点触发" in reason or "抄底买点" in reason:
        return "✅通过"
    if "样本不足" in reason:
        return "数据不足"
    if "MA60" in reason and "左侧" in reason:
        return "①MA60左侧"
    if "未现买点" in reason:
        return "②无买点"
    if "量化" in reason:
        return "③量化弱"
    if "环境" in reason:
        return "④环境闸"
    if "业绩驱动" in reason:
        return "P9业绩禁抄"
    return "其它"


def _pool(loop: ClosedLoop, dt: str) -> list[str]:
    reco = loop.adv_repo.get_active_reco(dt)
    if reco.empty:
        return []
    p = reco[(reco["direction"] == "long") & (reco["grade"].isin({"A", "B"}))].copy()
    if p.empty:
        return []
    if "rec_date" in p.columns:
        p = p.sort_values("rec_date", ascending=False)
    p = p.drop_duplicates("code")
    a = p[p["grade"] == "A"]["code"].tolist()
    b = p[p["grade"] == "B"]["code"].tolist()
    return a + b[: max(0, WATCH_POOL_CAP - len(a))]


def run(engine, days: int) -> str:
    loop = ClosedLoop(engine)
    repo = BaseRepository(engine)
    dt0 = _latest_data_date(loop)
    trade_days = repo.read_sql(
        "SELECT DISTINCT trade_date FROM stock_daily WHERE trade_date<=:d "
        "ORDER BY trade_date DESC LIMIT :n", {"d": dt0, "n": days}
    )["trade_date"].astype(str).tolist()
    trade_days = sorted(trade_days)

    etfs = _watch_etf_codes()
    etf_codes = [c for c, _ in etfs]
    etf_note = {c: n for c, n in etfs}
    held = HoldingRepo(engine).get_all()
    held_codes = [str(c) for c in held["code"]] if not held.empty else []

    L = ["# 恐慌放宽下「为何无标的进入」诊断", ""]
    L.append(f"- 观察池=投顾 A/B 级 long；ETF=watch_etf.txt（{len(etf_codes)} 只）；"
             f"持仓 {len(held_codes)} 只。买点闸链①MA60→②买点→③量化→④环境（恐慌只松④）。")

    # ── 预载：一次性拉市场数据，避免逐日全市场 fear 查询 / 逐票 MA N+1（远程 DB 慢）──
    d_lo = trade_days[0]
    mkt_start = (pd.to_datetime(d_lo) - pd.Timedelta(days=210)).strftime("%Y%m%d")
    idx_start = (pd.to_datetime(d_lo) - pd.Timedelta(days=430)).strftime("%Y%m%d")
    mkt = repo.read_sql(
        "SELECT code, trade_date, close, pct_chg FROM stock_daily "
        "WHERE trade_date>=:s AND trade_date<=:d", {"s": mkt_start, "d": dt0})
    mkt["trade_date"] = mkt["trade_date"].astype(str)
    idx = repo.read_sql(
        "SELECT code, trade_date, close FROM index_daily "
        "WHERE code='000300.SH' AND trade_date>=:s AND trade_date<=:d",
        {"s": idx_start, "d": dt0})
    idx["trade_date"] = idx["trade_date"].astype(str)

    # per-day 元数据先算（pool/gross/rank），收集 universe 并集做 MA 批量预载
    meta: dict[str, tuple] = {}
    uni_all: set[str] = set()
    for dt in trade_days:
        pred_date = _latest_pred_date(loop, dt)
        preds = loop.pred_repo.get_predictions(pred_date, loop.cfg.version) if pred_date else pd.DataFrame()
        u = set(loop.uni_repo.get_universe(pred_date, loop.cfg.universe.method)) if pred_date else set()
        if u and not preds.empty:
            preds = preds[preds["code"].isin(u)]
        rank_map = (dict(zip(preds["code"], pd.to_numeric(preds["rank_pct"], errors="coerce")))
                    if not preds.empty and "rank_pct" in preds.columns else {})
        try:
            gross = float(loop.mt.gross_exposure(dt, list(u) if u else None))
        except Exception:  # noqa: BLE001
            gross = float("nan")
        pool = _pool(loop, dt)
        universe_codes = list(dict.fromkeys(pool + etf_codes + held_codes))
        meta[dt] = (rank_map, gross, pool, universe_codes)
        uni_all.update(universe_codes)

    # MA 站位批量预载（并集所有候选，一个查询）
    ma_px: dict[str, pd.DataFrame] = {}
    if uni_all:
        ma_start = (pd.to_datetime(d_lo) - pd.Timedelta(days=140)).strftime("%Y%m%d")
        codes_l = list(uni_all)
        ph = ",".join(f":c{i}" for i in range(len(codes_l)))
        params = {f"c{i}": c for i, c in enumerate(codes_l)}
        params.update(s=ma_start, d=dt0)
        mp = repo.read_sql(
            f"SELECT code, trade_date, close FROM stock_daily "
            f"WHERE trade_date>=:s AND trade_date<=:d AND code IN ({ph})", params)
        if not mp.empty:
            mp["trade_date"] = mp["trade_date"].astype(str)
            mp["close"] = pd.to_numeric(mp["close"], errors="coerce")
            for c, g in mp.groupby("code"):
                ma_px[str(c)] = g.sort_values("trade_date")[["trade_date", "close"]]

    def _ma_stats(code: str, dt: str):
        s = ma_px.get(code)
        if s is None:
            return float("nan"), float("nan"), "?", "?"
        cl = s[s["trade_date"] <= dt]["close"].dropna().reset_index(drop=True)
        if len(cl) < 60:
            return float("nan"), float("nan"), "?", "?"
        ma20 = float(cl.tail(20).mean()); ma60 = float(cl.tail(60).mean())
        ma60_prev = float(cl.tail(65).head(60).mean()) if len(cl) >= 65 else ma60
        return ma20, ma60, ("是" if float(cl.iloc[-1]) >= ma60 else "否"), ("是" if ma60 > ma60_prev else "否")

    for dt in trade_days:
        rank_map, gross, pool, universe_codes = meta[dt]
        try:
            fear = float(fear_gauge(engine, dt, stock_df=mkt, idx_df=idx)["score"])
        except Exception:  # noqa: BLE001
            fear = None
        cfg = BuyPointConfig()
        panic = fear is not None and fear >= cfg.fear_buy

        # 真实 gross 与 gross=1.0（环境闸全开）两跑，证明④是否为约束
        bps = detect_buypoints(engine, dt, universe_codes, gross if np.isfinite(gross) else 0.6,
                               rank_map, cfg, fear=fear)
        bps_open = detect_buypoints(engine, dt, universe_codes, 1.0, rank_map, cfg, fear=fear)

        n_pass = sum(1 for b in bps.values() if b.is_buy)
        n_pass_open = sum(1 for b in bps_open.values() if b.is_buy)
        tally: dict[str, int] = {}
        for b in bps.values():
            k = _classify(b.reason)
            tally[k] = tally.get(k, 0) + 1

        fear_txt = ("%.0f" % fear) if fear is not None else "?"
        relax_txt = "≥75 抄底放松已生效" if panic else "<75 未触发放松"
        L.append(f"\n## {dt}　恐慌 {fear_txt}（{relax_txt}）· gross {gross:.2f}")
        L.append(f"- 候选 {len(universe_codes)} 只（池{len(pool)}+ETF{len(etf_codes)}+持仓{len(held_codes)}）"
                 f"→ **通过 {n_pass} 只**；把环境闸完全打开(gross=1.0)后 **通过 {n_pass_open} 只**")
        order = ["✅通过", "①MA60左侧", "②无买点", "③量化弱", "④环境闸", "P9业绩禁抄", "数据不足", "其它"]
        tally_str = "　".join(f"{k} {tally[k]}" for k in order if tally.get(k))
        L.append(f"- 拒绝归因：{tally_str}")
        if n_pass == 0 and n_pass_open == 0:
            L.append(f"- **结论**：环境闸全开也 0 通过 → 卡点不在恐慌/环境闸④，"
                     f"在①MA60（{tally.get('①MA60左侧',0)}只左侧）/②买点（{tally.get('②无买点',0)}只）。")

        # 明细表（ETF + 持仓 + 池里前若干只），标注 MA60 站位
        L.append("\n| 代码 | 类别 | 收盘 | MA20 | MA60 | 站上MA60 | MA60上行 | 命中闸 |")
        L.append("|---|---|---|---|---|---|---|---|")
        shown = 0
        for c in universe_codes:
            b = bps.get(c)
            if b is None:
                continue
            cat = "ETF" if c in etf_codes else ("持仓" if c in held_codes else "池")
            # MA 站位（预载批量，与买点闸同源）
            ma20, ma60, above, up = _ma_stats(c, dt)
            # 只详列 ETF + 持仓 + 池前 15 只，避免过长
            if cat == "池" and shown >= 15:
                continue
            if cat == "池":
                shown += 1
            L.append(f"| {c}{('·'+etf_note[c][:6]) if c in etf_note and etf_note[c] else ''} | {cat} | "
                     f"{b.last:.3g} | {ma20:.3g} | {ma60:.3g} | {above} | {up} | {_classify(b.reason)} |")

    L.append("\n### 读法")
    L.append("- **通过数=0 且 gross=1.0 仍=0** ⟹ 恐慌放松（只松环境闸④）对结果毫无影响，"
             "卡点在①MA60 前置闸：左侧下降趋势一律不买（系统\"不接飞刀\"铁律）。")
    L.append("- 这正是恐慌均值回归 overlay 与主买点哲学相反之处——overlay 要专买 MA60 下方的超跌反弹，"
             "故必须独立成腿、不能走 detect_buypoints（P22 待登记）。")
    L.append("- 只读诊断、非交易建议；MA 站位用 stock_daily 前复权，与生产买点闸同源。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="恐慌放宽下无标的进入诊断")
    ap.add_argument("--db", default=None)
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db) if args.db else make_engine()
    md = run(engine, args.days)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
