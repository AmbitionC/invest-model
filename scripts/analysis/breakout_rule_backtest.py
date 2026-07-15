"""突破买点确认规则对照回测（用户命题 2026-07-15·一次性分析，只读不落库不改生产）。

命题："放量突破"这条规则到底合理吗？是否过于严苛？有没有更好的买点确认方式？

现行规则（buypoint.py 买点1"底部反转"）同时要求三件事：
  ① 阳线吞没昨日实体（o0<=min(po,pc) 且 c0>=max(po,pc)）
  ② 收盘 >= 20日平台高点（收盘新高）
  ③ 成交量 >= 2.0 × 5日均量
疑点：①使连续上涨中的趋势突破几乎无法触发（吞没要求今日开盘低于昨日实体下沿）；
③在持续放量趋势里 5日均量水涨船高、2倍极难满足。

对照设计（全部共用同一前置：收盘>=MA60 且 MA60 上行[_slope_up 同产产口径] 且
当日为 20 日收盘新高——隔离"确认层"的独立贡献）：
  V0_现行     阳线吞没 + 量>=2.0×vma5
  V1_仅吞没   阳线吞没（无量能）
  V2_量2.0    阳线 + 量>=2.0×vma5（去吞没）
  V3_量1.5    阳线 + 量>=1.5×vma5
  V4_量1.2m20 阳线 + 量>=1.2×vma20（对齐回踩分支的温和倍数）
  V5_裸突破   阳线（基线：无任何额外确认）
  V6_口袋支点 阳线 + 量>近10日全部下跌日量的最大值（O'Neil pocket pivot 式量能）
  V7_幅度1%   阳线 + 收盘>=平台高点×1.01（无量能，用突破幅度替代量能防假突破）

入场=信号次日开盘（计划盘后生成、次日执行的真实时序）；次日开盘涨幅>9.7% 视为
买不进剔除（近似涨停开盘，各变体同规则）。前视规避：仅用截至信号日的数据。

指标：信号数 n、20日胜率、5/10/20日平均/中位收益、20日内平均最大回撤。

**判读标准（跑数前写死，勿按结果回调）**：
  A. 若某确认条件相对 V5 基线的 20日平均收益提升 <1pp 且胜率提升 <3pp，
     却砍掉 >70% 信号 → 判"过度严苛，确认无效"；
  B. 若量能/吞没确认相对基线有 >=1pp 且胜率 >=3pp 的改善 → 判"确认有效，维持或微调"；
  C. 变体间对比取"改善幅度/信号保留率"综合，不做参数寻优（8 个变体全部预先列明）。

局限（诚实声明）：stock_daily 为当前存续股票（幸存者偏差，对"变体间相对比较"影响
较小）；OHLC 按 adj_factor 前复权、成交量不复权（量比为短窗相对值，除权影响可忽略）；
未计交易成本（对比性结论不受影响）。

用法（Actions，读生产 stock_daily）：
  python scripts/analysis/breakout_rule_backtest.py [--db ...] [--start 20190101]
      [--limit 0] [--case 002384.SZ] [--out results/breakout_rule_backtest.md]
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
HOLD = (5, 10, 20)      # 前瞻窗口（交易日）
GAP_SKIP = 0.097        # 次日开盘涨幅超此值视为买不进（近似涨停开盘）
LOOKBACK = 20           # 平台/新高回看（同 BuyPointConfig.breakout_lookback）

VARIANTS = ["V0_现行", "V1_仅吞没", "V2_量2.0", "V3_量1.5",
            "V4_量1.2m20", "V5_裸突破", "V6_口袋支点", "V7_幅度1%"]


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
    """单票：返回每个'趋势内20日新高'日的各变体触发布尔 + 前瞻收益。"""
    n = len(g)
    if n < MIN_ROWS:
        return None
    o, c, v = g["open"].values, g["close"].values, g["volume"].values
    cl = pd.Series(c)
    ma60 = cl.rolling(60).mean()
    slope_ok = (ma60 - ma60.shift(5)) >= 0          # _slope_up 同口径（look=5）
    vma5 = pd.Series(v).rolling(5).mean().values
    vma20 = pd.Series(v).rolling(20).mean().values
    plat = cl.shift(1).rolling(LOOKBACK).max().values   # 前20日最高收盘（不含当日）
    # 近10日下跌日最大量（口袋支点参照，不含当日）
    down_vol = pd.Series(np.where(np.diff(c, prepend=c[0]) < 0, v, 0.0))
    pocket_ref = down_vol.shift(1).rolling(10).max().values

    # 先向量化筛"趋势内新高突破日"候选，再逐点判定（全A股裸日循环会超时）
    ma60v = ma60.values
    mask = (np.isfinite(ma60v) & (c >= ma60v) & slope_ok.fillna(False).values
            & np.isfinite(plat) & (c >= plat))
    cand = [t for t in np.where(mask)[0] if 65 <= t < n - max(HOLD) - 1]

    rows = []
    for t in cand:
        yang = c[t] > o[t]
        engulf = yang and o[t] <= min(o[t - 1], c[t - 1]) and c[t] >= max(o[t - 1], c[t - 1])
        vol20 = np.isfinite(vma5[t]) and v[t] >= 2.0 * vma5[t]
        vol15 = np.isfinite(vma5[t]) and v[t] >= 1.5 * vma5[t]
        vol12m20 = np.isfinite(vma20[t]) and v[t] >= 1.2 * vma20[t]
        pocket = np.isfinite(pocket_ref[t]) and pocket_ref[t] > 0 and v[t] > pocket_ref[t]
        margin1 = c[t] >= plat[t] * 1.01
        entry = o[t + 1]
        if not np.isfinite(entry) or entry <= 0 or entry / c[t] - 1 > GAP_SKIP:
            continue                                   # 涨停开盘买不进
        fwd = {}
        path = c[t + 1: t + 1 + max(HOLD)]
        for h in HOLD:
            fwd[f"r{h}"] = c[t + h] / entry - 1
        run_min = np.minimum.accumulate(path[:max(HOLD)])
        fwd["mdd20"] = float(np.min(run_min / entry - 1.0))
        rows.append({
            "date": g["trade_date"].iloc[t],
            "V0_现行": engulf and vol20, "V1_仅吞没": engulf,
            "V2_量2.0": yang and vol20, "V3_量1.5": yang and vol15,
            "V4_量1.2m20": yang and vol12m20, "V5_裸突破": yang,
            "V6_口袋支点": yang and pocket, "V7_幅度1%": yang and margin1,
            **fwd})
    return pd.DataFrame(rows) if rows else None


def main() -> None:
    ap = argparse.ArgumentParser(description="突破买点确认规则对照回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--start", default="20190101")
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 只（调试）")
    ap.add_argument("--case", default="002384.SZ", help="个例：输出该票各变体触发日")
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
    base_n = len(allsig)

    L = ["## 突破买点确认规则对照回测",
         "",
         f"- 样本：{len(data)} 只个股 · {args.start} 起 · 趋势内 20 日新高日共 **{base_n}** 个"
         f"（前置：≥MA60 且 MA60 上行；入场=次日开盘，涨停开盘剔除）",
         "",
         "| 变体 | 信号数 | 保留率 | 20日胜率 | 5日均 | 10日均 | 20日均 | 20日中位 | 20日内均MDD |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    stats = {}
    for vn in VARIANTS:
        sub = allsig[allsig[vn]]
        if sub.empty:
            L.append(f"| {vn} | 0 | 0% | — | — | — | — | — | — |")
            continue
        stats[vn] = dict(n=len(sub), win=(sub["r20"] > 0).mean(),
                         r5=sub["r5"].mean(), r10=sub["r10"].mean(),
                         r20=sub["r20"].mean(), med20=sub["r20"].median(),
                         mdd=sub["mdd20"].mean())
        st = stats[vn]
        L.append(f"| {vn} | {st['n']} | {st['n'] / base_n:.1%} | {st['win']:.1%} "
                 f"| {st['r5']:+.2%} | {st['r10']:+.2%} | {st['r20']:+.2%} "
                 f"| {st['med20']:+.2%} | {st['mdd']:+.2%} |")
    if "V5_裸突破" in stats:
        b = stats["V5_裸突破"]
        L += ["", "**相对 V5 基线（确认条件的独立贡献）**：",
              "| 变体 | 信号保留率 | 20日均收益差 | 胜率差 | 判读(预登记A/B) |", "|---|---:|---:|---:|---|"]
        for vn in VARIANTS:
            if vn == "V5_裸突破" or vn not in stats:
                continue
            st = stats[vn]
            dr = (st["r20"] - b["r20"]) * 100
            dw = (st["win"] - b["win"]) * 100
            keep = st["n"] / b["n"]
            verdict = ("确认有效" if (dr >= 1.0 and dw >= 3.0) else
                       ("过度严苛·确认无效" if (dr < 1.0 and dw < 3.0 and keep < 0.30)
                        else "增益有限"))
            L.append(f"| {vn} | {keep:.1%} | {dr:+.2f}pp | {dw:+.1f}pp | {verdict} |")

    # 个例：指定票的各变体触发日（近一年）
    cs = allsig[allsig["code"] == args.case]
    if not cs.empty:
        recent = cs[cs["date"] >= sorted(cs["date"])[-1][:4] + "0101"]
        L += ["", f"**个例 {args.case}（当年触发日）**："]
        for vn in VARIANTS:
            d = list(recent.loc[recent[vn], "date"])
            L.append(f"- {vn}: {('、'.join(d) if d else '（无触发）')}")

    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
