# -*- coding: utf-8 -*-
"""SOP 第一步（续）：命题E 量能地量买点 的可行域标定。

先回答三个前置事实问题，再谈能不能做成信号：
  1) 他给的绝对阈值（1/1.2/1.5/2 万亿）在 2015~2026 各年触发几天？——名义阈值是否随制度漂移
  2) 「量底领先价底 ≤1 个月」这个时序主张在数据上成立吗
  3) 量能低分位 与 恐慌≥75 / 价格<中位线 的重叠度（是不是已有腿的同义反复）
"""
import sys
sys.path.insert(0, "scripts/analysis")
import numpy as np
import pandas as pd
from pathlib import Path
from long_window_backtest import prep

root = Path("results")
cw = pd.read_csv(root / "crowding_daily.csv", dtype={"trade_date": str}).sort_values("trade_date")
cw["amt"] = pd.to_numeric(cw.total_amt_yi)
fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
hs, _ = prep(root, "index_dump_000300_SH.csv", "close", None)
m = cw.merge(hs[["trade_date", "c", "exp", "r1250"]], on="trade_date", how="inner")
m["yr"] = m.trade_date.str[:4]
m["f"] = m.trade_date.map(fmap)
print(f"样本 {m.trade_date.iloc[0]}~{m.trade_date.iloc[-1]}  {len(m)} 交易日  最新成交额 {m.amt.iloc[-1]:.0f}亿")

print("\n" + "=" * 96)
print("E-1 名义阈值的年度触发天数（他的口径：1~1.2万亿开始定投，最好 1.5万亿以下）")
print("=" * 96)
TH = [10000, 12000, 15000, 20000]
print(f"{'年':>6s}{'交易日':>7s}{'中位成交额':>11s}" + "".join(f"{'≤'+str(t//10000)+'万亿':>10s}" for t in TH))
for y, g in m.groupby("yr"):
    print(f"{y:>6s}{len(g):>7d}{g.amt.median():>11.0f}" + "".join(f"{(g.amt<=t).sum():>10d}" for t in TH))

print("\n" + "=" * 96)
print("E-2 分位口径（滚动3年分位）的年度触发天数——名义阈值漂移的替代方案")
print("=" * 96)
m["pct3y"] = m.amt.rolling(750, min_periods=250).rank(pct=True)
PQ = [0.05, 0.10, 0.20, 0.30]
print(f"{'年':>6s}" + "".join(f"{'≤'+str(int(q*100))+'分位':>11s}" for q in PQ))
for y, g in m.groupby("yr"):
    print(f"{y:>6s}" + "".join(f"{(g.pct3y <= q).sum():>11d}" for q in PQ))

print("\n" + "=" * 96)
print("E-3 「量底领先价底 ≤1个月」——按自然年找量能最低日 vs 沪深300 最低日")
print("=" * 96)
print(f"{'年':>6s}{'量底日':>10s}{'价底日':>10s}{'量底-价底(交易日)':>18s}")
lead = []
for y, g in m.groupby("yr"):
    if len(g) < 100:
        continue
    iv, ip = g.amt.idxmin(), g.c.idxmin()
    d = m.index.get_loc(ip) - m.index.get_loc(iv)
    lead.append(d)
    print(f"{y:>6s}{m.trade_date[iv]:>10s}{m.trade_date[ip]:>10s}{d:>18d}")
a = np.array(lead)
print(f"  中位领先 {np.median(a):.0f} 交易日 ｜ 落在 [0,20] 天内的年份 {int(((a>=0)&(a<=20)).sum())}/{len(a)}"
      f" ｜ 量底晚于价底的年份 {int((a<0).sum())}/{len(a)}")

print("\n" + "=" * 96)
print("E-4 与既有两腿的重叠度（是不是同义反复）")
print("=" * 96)
low = m.pct3y <= 0.10
below = m.c < m.exp
panic = m.f >= 75
for nm, s in (("量能≤3年10分位", low), ("价<中位线（B1闸）", below), ("恐慌≥75（B2闸）", panic)):
    print(f"  {nm:>18s} 触发 {int(s.fillna(False).sum()):>5d} 天")
lo = low.fillna(False)
print(f"  量能低 ∩ 价<中位线 = {int((lo & below).sum())} 天（占量能低的 {(lo & below).sum()/max(1,lo.sum()):.0%}）")
print(f"  量能低 ∩ 恐慌≥75  = {int((lo & panic.fillna(False)).sum())} 天（占量能低的 {(lo & panic.fillna(False)).sum()/max(1,lo.sum()):.0%}）")

print("\n" + "=" * 96)
print("E-5 量能低分位日的前瞻收益（沪深300，未做任何策略，纯事件研究）")
print("=" * 96)
c = m.c.values
print(f"{'条件':>24s}{'样本':>7s}{'20日':>9s}{'60日':>9s}{'120日':>9s}{'250日':>9s}")


def fwd(mask, lab):
    idx = np.where(mask.fillna(False).values)[0]
    row = f"{lab:>24s}{len(idx):>7d}"
    for h in (20, 60, 120, 250):
        v = [c[i + h] / c[i] - 1 for i in idx if i + h < len(c)]
        row += f"{np.mean(v):>9.2%}" if v else f"{'—':>9s}"
    print(row)


fwd(pd.Series(True, index=m.index), "全样本（基准）")
for q in PQ:
    fwd(m.pct3y <= q, f"量能≤{int(q*100)}分位")
for t in TH:
    fwd(m.amt <= t, f"成交额≤{t//10000}万亿")
fwd((m.pct3y <= 0.10) & below, "量能≤10分位 且 价<中位线")
