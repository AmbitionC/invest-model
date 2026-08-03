# -*- coding: utf-8 -*-
"""E37 —— 偏离率顶部风险刻度（判据写死于 docs/model_change_proposals.md P39 段，2026-08-02 跑数前）

偏离率 = 收盘价 / 60日均线 - 1，按**全历史极值排名**（因果、只用当日可得历史）度量顶部风险。
① 进入全历史前 5% 分位后，未来 20 日收益 <= 全样本均值 -2.0pp，且四腿中 >=3 腿同号
② 偏离率超过此前历史最大值的日子，其后 60 日内偏离率回落到 0 以下的比例 >= 80%
③ 独立 episode >= 15（不重叠 60 交易日）
④ 分半无符号翻转；分位阈值邻域 3%/5%/10% 结论不翻转
只读 CSV，不落库、不改生产。
"""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

SRC = {
    "沪深300": ("hs300.csv", "close", None),
    "创业板": ("spread_full.csv", "chinext", None),
    "科创50": ("star50.csv", "close", None),
    "红利": ("000922_csi.csv", "close", "000922_tr.csv"),
}


def prep(root: Path, nm: str) -> pd.DataFrame:
    f, col, trf = SRC[nm]
    d = pd.read_csv(root / f, dtype={"trade_date": str}).sort_values("trade_date").reset_index(drop=True)
    d["c"] = pd.to_numeric(d[col])
    d["ma60"] = d.c.rolling(60).mean()
    d["dev"] = d.c / d.ma60 - 1
    if trf:                                   # 红利：信号用价格、收益用全收益
        tr = pd.read_csv(root / trf, dtype={"trade_date": str})
        tr["c"] = pd.to_numeric(tr.close)
        d = d.merge(tr[["trade_date", "c"]], on="trade_date", suffixes=("", "_tr"))
        d["r"] = d.c_tr
    else:
        d["r"] = d.c
    d["f20"] = d.r.shift(-20) / d.r - 1
    d["f60"] = d.r.shift(-60) / d.r - 1
    # 因果分位：截至当日（不含当日）的历史里，当日偏离率的排位
    dv = d.dev.values
    n = len(d)
    pct = np.full(n, np.nan)
    prevmax = np.full(n, np.nan)
    for i in range(250, n):
        if dv[i] != dv[i]:
            continue
        h = dv[60:i]
        h = h[~np.isnan(h)]
        if len(h) < 200:
            continue
        pct[i] = (h < dv[i]).mean()
        prevmax[i] = np.nanmax(h)
    d["pct"] = pct
    d["prevmax"] = prevmax
    return d


def episodes(idx: np.ndarray, gap: int = 60) -> int:
    keep, last = 0, -10 ** 9
    for i in idx:
        if i - last >= gap:
            keep += 1
            last = i
    return keep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    args = ap.parse_args()
    root = Path(args.data)
    frames = {nm: prep(root, nm) for nm in SRC}

    for thr, tag in ((0.95, "主档 前5%"), (0.97, "邻域 前3%"), (0.90, "邻域 前10%")):
        print("=" * 100)
        print(f"[{tag}] 判据① 前瞻区分力（未来20日收益 vs 全样本均值，判据 <= -2.0pp 且 >=3 腿同号）")
        print(f"{'腿':8s}{'样本':>7s}{'触发日':>7s}{'episode':>9s}{'全样本20日':>11s}{'触发后20日':>11s}{'差(pp)':>9s}"
              f"{'分半H1':>9s}{'分半H2':>9s}")
        neg = 0
        for nm, d in frames.items():
            s = d.dropna(subset=["pct", "f20"])
            hit = s[s.pct >= thr]
            if len(hit) < 5:
                print(f"{nm:8s}{len(s):>7d}{len(hit):>7d}{'-':>9s}  样本不足")
                continue
            base = s.f20.mean()
            h = hit.f20.mean()
            ep = episodes(hit.index.values)
            half = len(s) // 2
            s1, s2 = s.iloc[:half], s.iloc[half:]
            d1 = (s1[s1.pct >= thr].f20.mean() - s1.f20.mean()) * 100 if (s1.pct >= thr).sum() else np.nan
            d2 = (s2[s2.pct >= thr].f20.mean() - s2.f20.mean()) * 100 if (s2.pct >= thr).sum() else np.nan
            diff = (h - base) * 100
            if diff <= -2.0:
                neg += 1
            print(f"{nm:8s}{len(s):>7d}{len(hit):>7d}{ep:>9d}{base:>11.2%}{h:>11.2%}{diff:>+9.2f}"
                  f"{d1:>+9.2f}{d2:>+9.2f}")
        print(f"  → 满足 <= -2.0pp 的腿数：{neg}/4（判据要求 >=3）")

    print("=" * 100)
    print("判据② 极值回归：偏离率超过此前历史最大值的日子，其后 60 日内回到 0 以下的比例（判据 >=80%）")
    print(f"{'腿':8s}{'破极值日数':>11s}{'独立episode':>12s}{'60日内回归比例':>15s}{'历史最大偏离':>13s}")
    for nm, d in frames.items():
        s = d.dropna(subset=["dev", "prevmax"])
        br = s[s.dev > s.prevmax]
        if len(br) == 0:
            print(f"{nm:8s}{0:>11d}  无破极值样本")
            continue
        dv = d.dev.values
        ok = 0
        for i in br.index:
            w = dv[i + 1:i + 61]
            w = w[~np.isnan(w)]
            if len(w) and (w < 0).any():
                ok += 1
        print(f"{nm:8s}{len(br):>11d}{episodes(br.index.values):>12d}"
              f"{ok / len(br):>15.0%}{np.nanmax(d.dev.values):>13.1%}")


if __name__ == "__main__":
    main()
