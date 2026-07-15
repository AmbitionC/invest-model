"""两类买点对照回测：趋势中继(回踩MA20) vs 突破新高（用户命题 2026-07-15·一次性分析）。

命题：用户问「突破新高那时候追进去安全边际是不是很低」。本脚本用生产 buypoint.py 的
**完全一致**的两类技术买点定义，做历史 head-to-head，看两者的胜率与盈亏比，回答
「突破的低胜率是否被更高的盈亏比（不对称赔率）补偿」。

两类买点定义（与 invest_model/signals/buypoint.py 逐字对齐；只取技术层，不叠量化/环境闸，
以隔离两类买点本身的原始边际）：
  共同前置：收盘 >= MA60 且 MA60 上行（_slope_up look=5）——左侧趋势一律不看。
  趋势中继(回踩MA20)：MA20 上行 且 |收盘/MA20-1|<=3% 且 近3日最低贴到 MA20(<=MA20*1.03)
                      且 当日阳线 且 量>=1.2×20日均量。
  突破新高(P18 v2)：当日阳线 且 收盘 >= 前20日最高收盘（无量能确认，与现行生产一致）。
  分类优先级同生产：先判回踩、否则判突破（两者极少同日，另报重叠数）。

入场=信号次日开盘（计划盘后生成、次日执行的真实时序）；次日开盘涨幅>9.7% 视为买不进剔除。
前视规避：仅用截至信号日数据。前瞻窗口 5/10/20/60 交易日。

**读法（跑数前写死，避免按结果找说法）**：
  - 核心不是比胜率，而是比 **期望收益（20日均）** 与 **盈亏比（均盈利/|均亏损|）**：
    若突破胜率更低但盈亏比更高、使期望收益不输回踩 → 印证「安全边际来自赔率不对称+止损，
    不来自入场便宜」；若突破期望收益与盈亏比双输回踩 → 用户的「追高不划算」担心得到数据支持，
    应在参谋话术里明确偏向回踩。
  - 纯描述性对照，不做参数寻优、不改生产（buypoint.py 逻辑一字不动）。

局限（诚实声明）：stock_daily 为当前存续股票（幸存者偏差，对两类相对比较影响较小）；
OHLC 按 adj_factor 前复权、成交量不复权（量比为短窗相对值）；未计交易成本（对比性结论不受影响）。

用法（Actions，读生产 stock_daily）：
  python scripts/analysis/buypoint_compare_backtest.py [--db ...] [--start 20190101]
      [--limit 0] [--out results/buypoint_compare_backtest.md]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

MIN_ROWS = 300          # 至少 300 交易日（MA60 预热 + 有效样本）
HOLD = (5, 10, 20, 60)  # 前瞻窗口（交易日）；含 60 日看「让利润奔跑」
GAP_SKIP = 0.097        # 次日开盘涨幅超此值视为买不进（近似涨停开盘）
LOOKBACK = 20           # 平台/新高回看（同 BuyPointConfig.breakout_lookback）
PULLBACK = 0.03         # 回踩 MA20 贴合带（BuyPointConfig.pullback_pct）
RETRACE_VOL = 1.2       # 回踩放量倍数（BuyPointConfig.retrace_vol_mult，对 20 日均量）


def _load_all(repo: BaseRepository, start: str, limit: int) -> dict[str, pd.DataFrame]:
    codes_df = repo.read_sql(
        "SELECT code, COUNT(*) AS n FROM stock_daily WHERE trade_date>=:s "
        "GROUP BY code HAVING n>=:m ORDER BY code", {"s": start, "m": MIN_ROWS})
    codes = list(codes_df["code"].astype(str))
    if limit:
        codes = codes[:limit]
    out: dict[str, pd.DataFrame] = {}
    for i in range(0, len(codes), 400):
        batch = codes[i:i + 400]
        ph = ",".join(f":c{j}" for j in range(len(batch)))
        params = {f"c{j}": c for j, c in enumerate(batch)}
        params["s"] = start
        daily = repo.read_sql(
            f"SELECT code, trade_date, open, high, low, close, volume FROM stock_daily "
            f"WHERE trade_date>=:s AND code IN ({ph}) ORDER BY code, trade_date", params)
        adj = pd.DataFrame()
        try:
            if repo.table_exists("stock_adj"):
                adj = repo.read_sql(
                    f"SELECT code, trade_date, adj_factor FROM stock_adj "
                    f"WHERE trade_date>=:s AND code IN ({ph})", params)
        except Exception:  # noqa: BLE001 — 复权缺失 fail-open
            adj = pd.DataFrame()
        adj_by = {c: g for c, g in adj.groupby("code")} if not adj.empty else {}
        for code, g in daily.groupby("code"):
            g = g.reset_index(drop=True)
            g["trade_date"] = g["trade_date"].astype(str)
            for col in ("open", "high", "low", "close", "volume"):
                g[col] = pd.to_numeric(g[col], errors="coerce")
            a = adj_by.get(code)
            if a is not None and not a.empty:
                f = pd.to_numeric(
                    a.assign(trade_date=a["trade_date"].astype(str))
                    .set_index("trade_date")["adj_factor"], errors="coerce")
                fac = g["trade_date"].map(f).ffill()
                if fac.notna().any():
                    scale = fac / float(fac.dropna().iloc[-1])
                    for col in ("open", "high", "low", "close"):
                        g[col] = g[col] * scale
            out[str(code)] = g.dropna(subset=["close", "open", "volume"]).reset_index(drop=True)
        print(f"  loaded {min(i + 400, len(codes))}/{len(codes)}", flush=True)
    return out


def _signals_one(g: pd.DataFrame) -> pd.DataFrame | None:
    """单票：返回每个技术买点日的 kind（趋势中继/突破新高）+ 前瞻收益。"""
    n = len(g)
    if n < MIN_ROWS:
        return None
    o = g["open"].values
    low = g["low"].values
    c = g["close"].values
    v = g["volume"].values
    cl = pd.Series(c)
    ma20 = cl.rolling(20).mean()
    ma60 = cl.rolling(60).mean()
    ma20v, ma60v = ma20.values, ma60.values
    slope60 = ((ma60 - ma60.shift(5)) >= 0).fillna(False).values      # _slope_up look=5
    slope20 = ((ma20 - ma20.shift(5)) >= 0).fillna(False).values
    vma20 = pd.Series(v).rolling(20).mean().values
    plat = cl.shift(1).rolling(LOOKBACK).max().values                 # 前20日最高收盘（不含当日）
    low3 = pd.Series(low).rolling(3).min().values                     # 近3日最低（含当日）
    yang = c > o

    trend_up = np.isfinite(ma60v) & (c >= ma60v) & slope60
    # 突破新高（P18 v2 生产口径：阳线 + 收盘>=前20日最高收盘）
    brk = trend_up & np.isfinite(plat) & (c >= plat) & yang
    # 趋势中继·回踩MA20（生产口径）
    with np.errstate(invalid="ignore", divide="ignore"):
        near_ma20 = np.isfinite(ma20v) & (np.abs(c / ma20v - 1.0) <= PULLBACK)
        touch = np.isfinite(ma20v) & (low3 <= ma20v * (1 + PULLBACK))
    vol_ok = np.isfinite(vma20) & (v >= RETRACE_VOL * vma20)
    rtr = trend_up & slope20 & near_ma20 & touch & yang & vol_ok

    # 分类优先级同生产：先回踩、否则突破
    kind = np.where(rtr, "趋势中继", np.where(brk, "突破新高", ""))
    idx = [t for t in np.where((rtr | brk))[0] if 65 <= t < n - max(HOLD) - 1]
    if not idx:
        return None
    rows = []
    for t in idx:
        entry = o[t + 1]
        if not np.isfinite(entry) or entry <= 0 or entry / c[t] - 1 > GAP_SKIP:
            continue                                       # 涨停开盘买不进
        fwd = {f"r{h}": c[t + h] / entry - 1 for h in HOLD}
        path = c[t + 1: t + 1 + max(HOLD)]
        run_min = np.minimum.accumulate(path)
        fwd["mdd"] = float(np.min(run_min / entry - 1.0))
        rows.append({"date": g["trade_date"].iloc[t], "kind": kind[t],
                     "both": bool(rtr[t] and brk[t]), **fwd})
    return pd.DataFrame(rows) if rows else None


def _stat_block(sub: pd.DataFrame) -> dict:
    r20 = sub["r20"]
    win = r20 > 0
    avg_win = float(r20[win].mean()) if win.any() else 0.0
    avg_loss = float(r20[~win].mean()) if (~win).any() else 0.0
    pl = (avg_win / abs(avg_loss)) if avg_loss < 0 else float("inf")
    return dict(
        n=len(sub), win=float(win.mean()),
        r5=float(sub["r5"].mean()), r10=float(sub["r10"].mean()),
        r20=float(r20.mean()), med20=float(r20.median()), r60=float(sub["r60"].mean()),
        avg_win=avg_win, avg_loss=avg_loss, pl=pl, mdd=float(sub["mdd"].mean()))


def main() -> None:
    ap = argparse.ArgumentParser(description="两类买点对照回测：回踩MA20 vs 突破新高")
    ap.add_argument("--db", default=None)
    ap.add_argument("--start", default="20190101")
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 只（调试）")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    engine = make_engine(args.db)
    repo = BaseRepository(engine)
    data = _load_all(repo, args.start, args.limit)
    print(f"universe: {len(data)} 只（>= {MIN_ROWS} 交易日，start={args.start}）", flush=True)

    frames = []
    for i, (code, g) in enumerate(data.items()):
        s = _signals_one(g)
        if s is not None:
            s.insert(0, "code", code)
            frames.append(s)
        if (i + 1) % 500 == 0:
            print(f"  scanned {i + 1}/{len(data)}", flush=True)
    if not frames:
        print("无信号样本"); return
    allsig = pd.concat(frames, ignore_index=True)
    overlap = int(allsig["both"].sum())

    L = ["## 两类买点对照回测：趋势中继(回踩MA20) vs 突破新高",
         "",
         f"- 样本：{len(data)} 只个股 · {args.start} 起 · 信号共 **{len(allsig)}** 个"
         f"（前置：≥MA60 且 MA60 上行；入场=次日开盘，涨停开盘剔除；同日两类重叠 {overlap} 例，"
         "按生产优先级归为回踩）",
         "- 口径：与 buypoint.py 技术层逐字对齐，未叠加量化/环境闸，隔离两类买点本身的原始边际。",
         "",
         "| 买点类型 | 信号数 | 20日胜率 | 5日均 | 10日均 | 20日均 | 20日中位 | 60日均 | 均盈利 | 均亏损 | 盈亏比 | 20日内均MDD |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    st = {}
    for k in ("趋势中继", "突破新高"):
        sub = allsig[allsig["kind"] == k]
        if sub.empty:
            L.append(f"| {k} | 0 | — | — | — | — | — | — | — | — | — | — |")
            continue
        s = _stat_block(sub)
        st[k] = s
        pl = "∞" if s["pl"] == float("inf") else f"{s['pl']:.2f}"
        L.append(f"| {k} | {s['n']} | {s['win']:.1%} | {s['r5']:+.2%} | {s['r10']:+.2%} "
                 f"| {s['r20']:+.2%} | {s['med20']:+.2%} | {s['r60']:+.2%} "
                 f"| {s['avg_win']:+.2%} | {s['avg_loss']:+.2%} | {pl} | {s['mdd']:+.2%} |")

    if "趋势中继" in st and "突破新高" in st:
        a, b = st["趋势中继"], st["突破新高"]
        L += ["", "**回踩 − 突破（正=回踩更优）**：",
              f"- 20日胜率差：{(a['win'] - b['win']) * 100:+.1f}pp",
              f"- 20日期望收益差：{(a['r20'] - b['r20']) * 100:+.2f}pp",
              f"- 60日期望收益差：{(a['r60'] - b['r60']) * 100:+.2f}pp",
              f"- 盈亏比：回踩 {a['pl']:.2f} vs 突破 {b['pl']:.2f}",
              f"- 20日内均最大回撤：回踩 {a['mdd']:+.2%} vs 突破 {b['mdd']:+.2%}"
              "（越接近0越浅，代表持有期间的账面煎熬更小）"]
        # 读法裁决（预登记）
        brk_asym = b["pl"] >= a["pl"] and b["r20"] >= a["r20"] - 0.005
        if brk_asym:
            L.append("- 裁决：突破胜率虽可能更低，但**盈亏比补偿使期望收益不输回踩**——"
                     "印证「安全边际来自赔率不对称+止损、非入场便宜」。")
        elif b["r20"] < a["r20"] and b["pl"] < a["pl"]:
            L.append("- 裁决：突破**期望收益与盈亏比双输回踩**——用户「追高安全边际低」的直觉获数据支持，"
                     "参谋话术应明确偏向回踩、突破仅作动量补充。")
        else:
            L.append("- 裁决：两类各有短长（一项占优一项落后），无单边碾压，按性格取舍。")

    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
