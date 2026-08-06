# -*- coding: utf-8 -*-
"""指数股息率序列（正确口径）+ 全收益序列数据质量校验

2026-08-04 红队 F1：主线此前用的
    dy = (TR_t / TR_{t-250}) / (PR_t / PR_{t-250}) - 1
**算术正确但不是估值指标**。该恒等式等于 Π(1 + d_i / P_i) - 1，
**分母是每笔分红各自除息日的价格，不是当日价格** —— 它衡量的是「过去一年已实现的分红回报」，
价格暴跌时分母不变、指标几乎不动。决定性反例：2008-10-28 历史大底，旧口径仅 42 分位。

正确口径（本模块）：
    1. 逐日反解单日分红率      dr_t = (TR_t/TR_{t-1}) / (PR_t/PR_{t-1}) - 1
    2. 还原分红金额（指数点）   amt_t = dr_t * PR_t
    3. 滚动 12 个月求和后除以**当日**价格
       dy_t = sum(amt[t-249..t]) / PR_t          ← 标准 D/P，因果、无前视

同时对全收益序列做质量校验：理论上 TR/PR 的比值（累计分红因子）应**单调不减**，
下降即数据瑕疵（红队与 D1 均发现 000922 在 2010-01-25 有 -0.149% 的倒挂）。

用法：
    python scripts/analysis/index_dividend_yield.py \
        --price results/index_dump_000922_CSI.csv \
        --total results/index_dump_H00922_CSI.csv \
        --out results/dividend_yield_000922.csv
只读 CSV，不落库、不联网。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

WIN = 250          # 滚动 12 个月（交易日）
EPS = 1e-9


def load_pair(price: Path, total: Path) -> pd.DataFrame:
    p = pd.read_csv(price, dtype={"trade_date": str})
    t = pd.read_csv(total, dtype={"trade_date": str})
    p["pr"] = pd.to_numeric(p["close"])
    t["tr"] = pd.to_numeric(t["close"])
    d = (p[["trade_date", "pr"]]
         .merge(t[["trade_date", "tr"]], on="trade_date", how="inner")
         .sort_values("trade_date").reset_index(drop=True))
    return d


def quality_check(d: pd.DataFrame) -> list[str]:
    """全收益序列质量校验：累计分红因子应单调不减。"""
    issues = []
    f = d.tr / d.pr                       # 累计分红因子
    drop = f.diff() / f.shift(1)
    bad = d[drop < -1e-5]
    if len(bad):
        issues.append(f"累计分红因子下降 {len(bad)} 处（理论应单调不减）：")
        for i in bad.index[:10]:
            issues.append(f"    {d.trade_date[i]}  相对变化 {drop[i]:+.4%}")
    n_p, n_t = len(d), None
    issues.append(f"价量对齐：合并后 {n_p} 行")
    dup = d.trade_date.duplicated().sum()
    if dup:
        issues.append(f"⚠ 重复交易日 {dup} 个")
    return issues


def build(d: pd.DataFrame) -> pd.DataFrame:
    """产出两条序列：正确口径 dy 与旧口径 dy_realized（保留作对照，明确标注不可作估值锚）。"""
    d = d.copy()
    d["dr"] = ((d.tr / d.tr.shift(1)) / (d.pr / d.pr.shift(1)) - 1).clip(lower=0)
    d["amt"] = d.dr * d.pr                                   # 分红金额（指数点）
    d["dy"] = d.amt.rolling(WIN).sum() / d.pr                # ✅ 标准 D/P
    d["dy_realized"] = (1 + d.dr).rolling(WIN).apply(np.prod, raw=True) - 1   # ❌ 旧口径
    # 因果分位（截至当日、不含当日；预热 500 个交易日后才给值）
    v = d.dy.values
    pct = np.full(len(d), np.nan)
    for i in range(WIN + 500, len(d)):
        h = v[WIN:i]
        h = h[~np.isnan(h)]
        if len(h) >= 500:
            pct[i] = float((h < v[i]).mean())
    d["dy_pct_expanding"] = pct
    return d


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--price", required=True)
    ap.add_argument("--total", required=True)
    ap.add_argument("--out", default="results/dividend_yield.csv")
    args = ap.parse_args()

    d = load_pair(Path(args.price), Path(args.total))
    print("=" * 78)
    print("数据质量校验")
    print("=" * 78)
    for line in quality_check(d):
        print("  " + line)

    d = build(d)
    s = d.dropna(subset=["dy"])
    print("\n" + "=" * 78)
    print(f"股息率序列（正确口径 D/P）  {s.trade_date.iloc[0]} ~ {s.trade_date.iloc[-1]}  {len(s)} 行")
    print("=" * 78)
    print(f"  均值 {s.dy.mean():.2%}  中位 {s.dy.median():.2%}  "
          f"区间 {s.dy.min():.2%}({s.trade_date[s.dy.idxmin()]}) ~ {s.dy.max():.2%}({s.trade_date[s.dy.idxmax()]})")
    print(f"  分位 5/25/50/75/95 = " + " / ".join(f"{s.dy.quantile(q):.2%}" for q in (.05, .25, .5, .75, .95)))
    print(f"  当前 {s.dy.iloc[-1]:.2%}")

    r20 = d.pr.pct_change(20)
    j = s.index
    c_new = np.corrcoef(s.dy, r20[j].fillna(0))[0, 1]
    c_old = np.corrcoef(s.dy_realized, r20[j].fillna(0))[0, 1]
    print(f"\n  与 20 日价格涨跌的相关：正确口径 {c_new:+.3f}   旧口径 {c_old:+.3f}")
    print("  （估值指标应与价格负相关；旧口径接近 0 即证明它不是估值指标）")

    print("\n  关键历史点位对照（截至当日因果分位）：")
    print(f"  {'日期':>10s}{'价格':>10s}{'正确口径':>10s}{'分位':>7s}{'旧口径':>10s}{'分位':>7s}")
    for dt in ("20081028", "20150708", "20160128", "20181018", "20240205", "20260730"):
        row = s[s.trade_date == dt]
        if not len(row):
            continue
        i = row.index[0]
        h = s[s.index <= i]
        pn = (h.dy < row.dy.iloc[0]).mean()
        po = (h.dy_realized < row.dy_realized.iloc[0]).mean()
        print(f"  {dt:>10s}{row.pr.iloc[0]:>10.0f}{row.dy.iloc[0]:>10.2%}{pn:>7.0%}"
              f"{row.dy_realized.iloc[0]:>10.2%}{po:>7.0%}")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    d[["trade_date", "pr", "tr", "dy", "dy_pct_expanding", "dy_realized"]].to_csv(out, index=False)
    print(f"\nsaved {out}")


if __name__ == "__main__":
    main()
