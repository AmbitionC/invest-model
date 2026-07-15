"""回踩买点确认规则对照回测（用户命题 2026-07-15·一次性分析，只读不落库不改生产）。

命题：回踩分支的确认条件（当日阳线 + 放量 1.2×20日均量）是否有效？
注意：该 1.2× 是 2026-07-02 随初始提交转述投顾手册进入生产的原始常量，**从未验证**；
昨日突破实验中的"量1.2×20日均"变体是在突破日样本上测的，对回踩日无证明力——
回踩日量能语义不同（承接/反弹放量 vs 追涨放量），必须在回踩事件自身样本上专门检验。

事件池（严格镜像生产 buypoint.py 回踩结构，隔离"确认层"）：
  前置：收盘>=MA60 且 MA60 上行（_slope_up 同口径，含当日、look=5）
  结构：MA20 上行 且 |收盘/MA20-1|<=3% 且 近3日最低<=MA20×1.03
  ——过前置+结构的日子为事件；各变体只在确认层取子集。

变体（7 个预先列明，不做参数寻优）：
  R0_现行     阳线 + 量>=1.2×vma20
  R1_仅阳线   阳线（去量能）
  R2_仅量能   量>=1.2×vma20（去阳线）
  R3_裸回踩   无确认（基线=事件池全体）
  R4_量1.0    阳线 + 量>=1.0×vma20
  R5_缩量反弹 阳线 + 量<0.8×vma20（经典"缩量回踩"反向假设：卖压衰竭才健康）
  R6_口袋支点 阳线 + 量>近10日全部下跌日量的最大值

入场=信号次日开盘；次日开盘涨幅>9.7% 视为买不进剔除；OHLC 前复权、量不复权；
前瞻 5/10/20 交易日收益 + 20日内最大回撤。

**严谨性升级（较昨日突破实验新增三重，均预登记）**：
  ① 补集对照：每变体除对比基线外，直接对比其**补集**（同池未被选中者）——干净反事实；
  ② 按日聚类 t 统计：同日截面相关会虚增显著性——对"变体 vs 补集"构造逐日均值差序列
     （仅两组当日均非空的日子），t = mean(diff)/se(diff)，n 为天数非信号数；
  ③ 分半期稳健：样本对半切（前半/后半），要求方向一致才算数。

**判读标准（跑数前写死，勿按结果回调）**：
  有效：   变体相对补集 20日均差 >= +1.0pp 且 聚类|t| >= 2.0 且 两半期方向一致 → 保留该确认
  无效：   20日均差 < +0.5pp 或 聚类|t| < 2.0 或 半期方向翻转 → 该确认无增益（候选删除）
  增益有限：介于两者之间 → 维持现状，不动
  （R5 为反向假设：若其相对补集显著为正，说明"缩量回踩"才是对的方向，单独如实报告。）

局限：幸存者偏差（存续股票，主要影响绝对水平、对组间相对比较影响小）；无交易成本；
事件池为技术层近似（生产还有量化 rank/环境/仓位闸，对各变体等同作用，不影响对比）。

用法（Actions）：python scripts/analysis/retrace_rule_backtest.py [--start 20190101] [--out ...]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from breakout_rule_backtest import _load_all, MIN_ROWS, HOLD, GAP_SKIP  # noqa: E402

VARIANTS = ["R0_现行", "R1_仅阳线", "R2_仅量能", "R3_裸回踩",
            "R4_量1.0", "R5_缩量反弹", "R6_口袋支点"]


def _events_one(g: pd.DataFrame) -> pd.DataFrame | None:
    n = len(g)
    if n < MIN_ROWS:
        return None
    o, c, v = g["open"].values, g["close"].values, g["volume"].values
    low = g["low"].values
    cl = pd.Series(c)
    ma20s = cl.rolling(20).mean()
    ma60s = cl.rolling(60).mean()
    slope60 = (ma60s - ma60s.shift(5)) >= 0
    slope20 = (ma20s - ma20s.shift(5)) >= 0
    vma20 = pd.Series(v).rolling(20).mean().values
    low3 = pd.Series(low).rolling(3).min().values
    down_vol = pd.Series(np.where(np.diff(c, prepend=c[0]) < 0, v, 0.0))
    pocket_ref = down_vol.shift(1).rolling(10).max().values

    ma20v, ma60v = ma20s.values, ma60s.values
    with np.errstate(invalid="ignore", divide="ignore"):
        near = np.abs(c / ma20v - 1.0) <= 0.03
        touch = low3 <= ma20v * 1.03
    mask = (np.isfinite(ma60v) & (c >= ma60v) & slope60.fillna(False).values
            & slope20.fillna(False).values & np.isfinite(ma20v) & near & touch)
    cand = [t for t in np.where(mask)[0] if 65 <= t < n - max(HOLD) - 1]
    rows = []
    for t in cand:
        entry = o[t + 1]
        if not np.isfinite(entry) or entry <= 0 or entry / c[t] - 1 > GAP_SKIP:
            continue
        yang = c[t] > o[t]
        vol_ok = np.isfinite(vma20[t]) and v[t] >= 1.2 * vma20[t]
        vol10 = np.isfinite(vma20[t]) and v[t] >= 1.0 * vma20[t]
        shrink = np.isfinite(vma20[t]) and v[t] < 0.8 * vma20[t]
        pocket = np.isfinite(pocket_ref[t]) and pocket_ref[t] > 0 and v[t] > pocket_ref[t]
        fwd = {f"r{h}": c[t + h] / entry - 1 for h in HOLD}
        path = c[t + 1: t + 1 + max(HOLD)]
        fwd["mdd20"] = float(np.min(np.minimum.accumulate(path) / entry - 1.0))
        rows.append({
            "date": g["trade_date"].iloc[t],
            "R0_现行": yang and vol_ok, "R1_仅阳线": yang, "R2_仅量能": vol_ok,
            "R3_裸回踩": True, "R4_量1.0": yang and vol10,
            "R5_缩量反弹": yang and shrink, "R6_口袋支点": yang and pocket,
            **fwd})
    return pd.DataFrame(rows) if rows else None


def _clustered_t(sig: pd.DataFrame, vn: str) -> tuple[float, float, int]:
    """变体 vs 补集：按日聚类的 20 日均值差 t 统计。返回 (日均差pp, t, 有效天数)。"""
    diffs = []
    for _, day in sig.groupby("date"):
        a = day.loc[day[vn], "r20"]
        b = day.loc[~day[vn], "r20"]
        if len(a) and len(b):
            diffs.append(a.mean() - b.mean())
    if len(diffs) < 30:
        return float("nan"), float("nan"), len(diffs)
    d = np.asarray(diffs)
    t = d.mean() / (d.std(ddof=1) / np.sqrt(len(d)))
    return float(d.mean() * 100), float(t), len(d)


def main() -> None:
    ap = argparse.ArgumentParser(description="回踩买点确认规则对照回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--start", default="20190101")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    engine = make_engine(args.db)
    repo = BaseRepository(engine)
    data = _load_all(repo, args.start, args.limit)
    print(f"universe: {len(data)} 只", flush=True)
    frames = []
    for i, (code, g) in enumerate(data.items()):
        s = _events_one(g)
        if s is not None:
            s.insert(0, "code", code)
            frames.append(s)
        if (i + 1) % 1000 == 0:
            print(f"  scanned {i + 1}/{len(data)}", flush=True)
    sig = pd.concat(frames, ignore_index=True)
    base_n = len(sig)
    dates_sorted = sorted(sig["date"].unique())
    mid = dates_sorted[len(dates_sorted) // 2]

    L = ["## 回踩买点确认规则对照回测（补集对照 + 按日聚类 t + 分半期稳健）", "",
         f"- 事件池：{len(data)} 只 · {args.start} 起 · 回踩结构日共 **{base_n}** 个"
         f"（前置=趋势内；结构=MA20上行+贴合±3%+近3日触及；入场=次日开盘，涨停开盘剔除）",
         f"- 分半期切点：{mid}",
         "",
         "| 变体 | 信号数 | 占池 | 20日胜率 | 20日均 | 20日中位 | 均MDD | vs补集日均差 | 聚类t | 天数 | 前半差 | 后半差 |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    verdicts = {}
    for vn in VARIANTS:
        sub = sig[sig[vn]]
        if sub.empty:
            L.append(f"| {vn} | 0 | — | — | — | — | — | — | — | — | — | — |")
            continue
        dpp, t, nd = _clustered_t(sig, vn) if vn != "R3_裸回踩" else (float("nan"),) * 2 + (0,)
        h1 = sig[sig["date"] < mid]; h2 = sig[sig["date"] >= mid]
        def _half_diff(h):
            a = h.loc[h[vn], "r20"]; b = h.loc[~h[vn], "r20"]
            return (a.mean() - b.mean()) * 100 if len(a) and len(b) else float("nan")
        d1, d2 = _half_diff(h1), _half_diff(h2)
        if vn == "R3_裸回踩":
            verdict = "基线"
        elif np.isfinite(dpp) and np.isfinite(t):
            consistent = np.isfinite(d1) and np.isfinite(d2) and (d1 * d2 > 0)
            if dpp >= 1.0 and abs(t) >= 2.0 and consistent and dpp > 0:
                verdict = "确认有效"
            elif dpp < 0.5 or abs(t) < 2.0 or not consistent:
                verdict = "确认无增益"
            else:
                verdict = "增益有限"
        else:
            verdict = "样本不足"
        verdicts[vn] = verdict
        L.append(
            f"| {vn} | {len(sub)} | {len(sub) / base_n:.1%} | {(sub['r20'] > 0).mean():.1%} "
            f"| {sub['r20'].mean():+.2%} | {sub['r20'].median():+.2%} | {sub['mdd20'].mean():+.2%} "
            f"| {dpp:+.2f}pp | {t:+.2f} | {nd} | {d1:+.2f} | {d2:+.2f} |"
            if np.isfinite(dpp) else
            f"| {vn} | {len(sub)} | {len(sub) / base_n:.1%} | {(sub['r20'] > 0).mean():.1%} "
            f"| {sub['r20'].mean():+.2%} | {sub['r20'].median():+.2%} | {sub['mdd20'].mean():+.2%} "
            f"| — | — | — | — | — |")
    L += ["", "**判读（预登记标准）**：" + "；".join(f"{k}={v}" for k, v in verdicts.items())]
    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
