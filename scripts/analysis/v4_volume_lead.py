"""复核主线 §2.5 的两条事实读数（V4 独立口径）。

(1) 名义阈值 1/1.2/1.5/2 万亿是否随制度漂移；
(2) 「量底领先价底 ≤1 个月」是否成立 —— 并质疑「按自然年取最低点」这个口径本身。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v4_common import load_amount, load_legs

THRESH = [10000.0, 12000.0, 15000.0, 20000.0]  # 亿元


def zigzag_bottoms(p: np.ndarray, drop=0.20, rise=0.20):
    """标准交替式 ZigZag：峰谷交替确认。

    找谷模式：跟踪最低点，反弹 ≥rise 即确认为谷、转入找峰模式；
    找峰模式：跟踪最高点，回撤 ≥drop 即确认为峰、转入找谷模式。
    """
    bottoms = []
    mode = "peak"          # 从数据首日起先找峰
    ext, ext_i = p[0], 0
    for i in range(1, len(p)):
        if mode == "peak":
            if p[i] > ext:
                ext, ext_i = p[i], i
            elif p[i] / ext - 1 <= -drop:
                mode, ext, ext_i = "trough", p[i], i
        else:
            if p[i] < ext:
                ext, ext_i = p[i], i
            elif p[i] / ext - 1 >= rise:
                bottoms.append(ext_i)
                mode, ext, ext_i = "peak", p[i], i
    return bottoms


def main():
    amt = load_amount()
    hs300 = load_legs()["沪深300"]["px"]
    d = amt.index.to_numpy()
    a = amt.to_numpy()
    p = hs300.reindex(d).ffill().to_numpy()

    print("=" * 84)
    print("[1] 名义阈值随制度漂移 —— 按自然年统计「成交额 ≤ 阈值」的交易日占比")
    print("=" * 84)
    yrs = sorted({int(x) // 10000 for x in d})
    print(f"{'年':<6}{'交易日':>6}" + "".join(f"{f'≤{t/10000:.1f}万亿':>12}"
                                            for t in THRESH) +
          f"{'年均成交额':>12}")
    for y in yrs:
        m = (d // 10000) == y
        row = f"{y:<6}{int(m.sum()):>6}"
        for t in THRESH:
            row += f"{(a[m] <= t).mean()*100:>11.0f}%"
        row += f"{a[m].mean():>11.0f}亿"
        print(row)
    print("\n复核结论：2016~2018 三年 ≤1万亿 的天数占比 = " +
          ", ".join(f"{y}:{(a[(d//10000)==y] <= 10000).mean()*100:.0f}%"
                    for y in [2016, 2017, 2018]) +
          f"；2025:{(a[(d//10000)==2025] <= 10000).mean()*100:.1f}%"
          f"；2026:{(a[(d//10000)==2026] <= 10000).mean()*100:.1f}%")

    print("\n" + "=" * 84)
    print("[2A] 复现主线口径：每个自然年各取「成交额最低日」与「沪深300 最低日」")
    print("=" * 84)
    rows = []
    for y in yrs:
        m = np.where((d // 10000) == y)[0]
        vi = m[np.argmin(a[m])]
        pi = m[np.argmin(p[m])]
        rows.append(dict(年=y, 量底=int(d[vi]), 价底=int(d[pi]), 领先交易日=pi - vi))
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))
    lead = df["领先交易日"].to_numpy()
    print(f"\n落在 [0,20] 交易日内：{int(((lead>=0)&(lead<=20)).sum())}/{len(lead)}；"
          f"量底晚于价底（lead<0）：{int((lead<0).sum())}/{len(lead)}；"
          f"离散度 {lead.min()} ~ {lead.max()}")

    print("\n" + "=" * 84)
    print("[2B] V4 对该口径的质疑：自然年最低点是否落在年初/年末边界（＝口径产物）")
    print("=" * 84)
    for _, r in df.iterrows():
        y = int(r["年"])
        m = np.where((d // 10000) == y)[0]
        vi = int(np.argmin(a[m]))
        pi = int(np.argmin(p[m]))
        n = len(m)
        tagv = "边界" if (vi < 10 or vi > n - 11) else ""
        tagp = "边界" if (pi < 10 or pi > n - 11) else ""
        if tagv or tagp:
            print(f"  {y}: 量底在年内第 {vi+1}/{n} 日 {tagv:<4} | "
                  f"价底在年内第 {pi+1}/{n} 日 {tagp}")
    nb = sum(1 for _, r in df.iterrows()
             for y in [int(r["年"])]
             for m in [np.where((d // 10000) == y)[0]]
             if (np.argmin(a[m]) < 10 or np.argmin(a[m]) > len(m) - 11
                 or np.argmin(p[m]) < 10 or np.argmin(p[m]) > len(m) - 11))
    print(f"\n{nb}/{len(df)} 个年份至少有一侧最低点贴在年度边界 10 个交易日内"
          f" ⟹ 自然年切割本身在制造「领先/滞后」")

    print("\n" + "=" * 84)
    print("[2C] V4 的替代口径：ZigZag 主要底部（跌≥20% 后反弹≥20%）× 平滑量底")
    print("=" * 84)
    ma = pd.Series(a).rolling(20).mean().to_numpy()
    for drop, rise in [(0.20, 0.20), (0.15, 0.15), (0.25, 0.25)]:
        bots = zigzag_bottoms(p, drop, rise)
        print(f"\n>>> ZigZag(跌{drop:.0%}/反弹{rise:.0%})：识别出 {len(bots)} 个主要底部")
        out = []
        BACK, FWD = 250, 120
        for bi in bots:
            lo, hi = max(0, bi - BACK), min(len(a) - 1, bi + FWD)
            seg = ma[lo:hi + 1]
            if np.all(np.isnan(seg)):
                continue
            vi = lo + int(np.nanargmin(seg))
            cens = "截尾" if (vi == lo or vi == hi) else ""
            out.append(dict(价底=int(d[bi]), 量底MA20=int(d[vi]),
                            领先交易日=bi - vi, 备注=cens))
        od = pd.DataFrame(out)
        print(od.to_string(index=False))
        if len(od):
            L = od["领先交易日"].to_numpy()
            print(f"落在 [0,20] 内：{int(((L>=0)&(L<=20)).sum())}/{len(L)}；"
                  f"落在 [0,60] 内：{int(((L>=0)&(L<=60)).sum())}/{len(L)}；"
                  f"中位 {np.median(L):.0f} 交易日；范围 {L.min()}~{L.max()}；"
                  f"截尾 {int((od['备注']=='截尾').sum())} 例")

    print("\n" + "=" * 84)
    print("[2D] 可检验的弱化形式：主要价底当日，成交额处于什么滚动分位？")
    print("=" * 84)
    from v4_common import rolling_pct
    pctv = rolling_pct(amt).to_numpy()
    for bi in bots:
        w = ma[max(0, bi - 20):bi + 1]
        print(f"  价底 {int(d[bi])}: 当日量滚动3年分位 "
              f"{pctv[bi]*100 if np.isfinite(pctv[bi]) else float('nan'):.0f}% | "
              f"前20日MA20 量分位 "
              f"{np.nanmean(pctv[max(0,bi-20):bi+1])*100:.0f}%")
    print("\n注：这一形式检验的是「价底附近量能是否已在低位」（同期共现），"
          "不是他主张的「量底领先价底 ≤1 个月」（时序先后）。")


if __name__ == "__main__":
    main()
