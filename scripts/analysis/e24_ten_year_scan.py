# -*- coding: utf-8 -*-
"""E24 顶部合成信号·正式十年扫描 + P28 全期审计 + AND 共振 + E24 v2 终裁（只读 dumps）。

数据（全部来自 git 内 results/ 导出，不连生产库）：
  fear_daily_dump.csv          恐慌 2015-01 ~ 今（深度回填后全量）
  index_dump_000300_SH.csv     沪深300 全历史
  fund_share_dump.csv/-close   国家队 8 只宽基 ETF 份额/收盘（2018 起）
  crowding_daily.csv           拥挤度/两融/量能（2015 起）

判据全部取自 docs/model_change_proposals.md P28/P29/E24 v2 段（跑数前写死）：
  E24：①收盘>滚动5年中位×1.15 ②恐慌≤15（敏感性≤25）③五日净申购≤−200亿；
       ≥2 共振成 episode（间隔>20td）；过关=episode首日 20/60日 ≤全样本−2pp、
       负比例≥55%、episode≥8 跨≥2 顶部区制。
  P28：①<全量中位线×0.9 ②距峰≥40% ③恐慌≥85，≥2 共振=杠杆窗口。
  AND：<全量中位线 且 恐慌≥75 同日（两买入腿正交性检验）。
  E24 v2：S4 双创占比/S5 两融比/S6 量能 各滚动250日分位≥90，判据同 E24 主判据。

用法：python scripts/analysis/e24_ten_year_scan.py [--dir results]
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd


def rollpct(s: pd.Series, w: int = 250) -> pd.Series:
    out = np.full(len(s), np.nan)
    v = s.values
    for i in range(w - 1, len(s)):
        out[i] = float((v[i] >= v[i - w + 1:i + 1]).mean())
    return pd.Series(out, index=s.index)


def episodes(mask: pd.Series, dates: pd.Series, gap_td: int = 20) -> list[int]:
    """episode 首日的行号列表（按交易日行号间隔>gap_td 分段）。"""
    idx = list(mask[mask].index)
    out = []
    for i in idx:
        if not out or i - out_last > gap_td:
            out.append(i)
        out_last = i
    return out


def fwd(c: pd.Series, i: int, n: int) -> float:
    j = min(i + n, len(c) - 1)
    return float(c.iloc[j] / c.iloc[i] - 1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="results")
    a = ap.parse_args()
    d = a.dir

    hs = pd.read_csv(f"{d}/index_dump_000300_SH.csv", dtype={"trade_date": str})
    hs = hs.sort_values("trade_date").reset_index(drop=True)
    fear = pd.read_csv(f"{d}/fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))

    c = pd.to_numeric(hs.close)
    dt = hs.trade_date
    n = len(c)
    f = dt.map(fmap)
    med_exp = pd.Series([np.median(c[:i + 1]) if i >= 500 else np.nan for i in range(n)])
    med_r1250 = c.rolling(1250).median()
    dd = c / c.cummax() - 1

    # 国家队五日滚动净申购（亿元）
    sh = pd.read_csv(f"{d}/fund_share_dump.csv", dtype={"trade_date": str})
    cl = pd.read_csv(f"{d}/fund_close_dump.csv", dtype={"trade_date": str})
    m = sh.merge(cl, on=["code", "trade_date"], how="inner").sort_values(["code", "trade_date"])
    m["dsh"] = m.groupby("code")["fd_share"].diff()          # 万份
    m["flow_yi"] = m["dsh"] * pd.to_numeric(m["close"]) / 1e4  # 万元→亿元
    daily_flow = m.groupby("trade_date")["flow_yi"].sum()
    net5 = daily_flow.rolling(5).sum()
    net5map = net5.to_dict()
    g = dt.map(net5map)

    lo = dt.searchsorted("20150105")
    span = slice(lo, n)
    base20 = c.pct_change(20).shift(-20).iloc[span].mean()
    base60 = c.pct_change(60).shift(-60).iloc[span].mean()
    print(f"样本 {dt.iloc[lo]}~{dt.iloc[-1]} 基线 20日{base20:+.2%} 60日{base60:+.2%}")

    # ── E24 正式扫描 ──
    for th2, tag in [(15, "主判据 ②≤15"), (25, "敏感性 ②≤25")]:
        s1 = c > med_r1250 * 1.15
        s2 = f <= th2
        s3 = g <= -200
        cnt = s1.astype(int) + s2.astype(int) + s3.fillna(False).astype(int)
        mask = (cnt >= 2) & (pd.RangeIndex(n) >= lo)
        eps = episodes(pd.Series(mask), dt)
        r20 = [fwd(c, i, 20) for i in eps]
        r60 = [fwd(c, i, 60) for i in eps]
        neg = np.mean([r < 0 for r in r20]) if eps else float("nan")
        regimes = {dt.iloc[i][:4] for i in eps}
        print(f"\n== E24 {tag}: {int(mask.sum())} 天 {len(eps)} episode | "
              f"首日后20日均 {np.mean(r20):+.2%}({(np.mean(r20)-base20)*100:+.1f}pp) "
              f"60日均 {np.mean(r60):+.2%}({(np.mean(r60)-base60)*100:+.1f}pp) 负比例 {neg:.0%}"
              if eps else f"\n== E24 {tag}: {int(mask.sum())} 天 0 episode")
        for i in eps:
            print(f"   {dt.iloc[i]} 价{c.iloc[i]:.0f} 信号{'①' if s1.iloc[i] else ''}"
                  f"{'②' if s2.iloc[i] else ''}{'③' if bool(s3.iloc[i]) else ''} "
                  f"恐慌{f.iloc[i]} 后20日{fwd(c, i, 20):+.1%} 后60日{fwd(c, i, 60):+.1%}")
        if eps:
            ok1 = np.mean(r20) <= base20 - 0.02 and np.mean(r60) <= base60 - 0.02
            ok2 = neg >= 0.55
            ok3 = len(eps) >= 8 and len(regimes) >= 2
            print(f"   判据: 两窗≤基线−2pp {'✅' if ok1 else '❌'} | 负比例≥55% {'✅' if ok2 else '❌'} | "
                  f"episode≥8跨≥2区制 {'✅' if ok3 else '❌'}（{len(eps)}个/{len(regimes)}年份）")

    # ── AND 低价×恐慌 全期 ──
    mand = (c < med_exp) & (f >= 75) & (pd.RangeIndex(n) >= lo)
    print(f"\n== AND <全量中位线×恐慌≥75 同日: {int(mand.sum())} 天（全期）")

    # ── P28 全期审计 ──
    p1 = c < med_exp * 0.9
    p2 = dd <= -0.40
    p3 = f >= 85
    pcnt = p1.astype(int) + p2.astype(int) + p3.astype(int)
    pm = (pcnt >= 2) & (pd.RangeIndex(n) >= lo)
    peps = episodes(pd.Series(pm), dt, gap_td=30)
    print(f"== P28 ≥2共振: {int(pm.sum())} 天 {len(peps)} episode（全期）")
    for i in peps:
        print(f"   {dt.iloc[i]} 价{c.iloc[i]:.0f} 信号{'①' if p1.iloc[i] else ''}"
              f"{'②' if p2.iloc[i] else ''}{'③' if p3.iloc[i] else ''} 恐慌{f.iloc[i]} "
              f"后250日 {fwd(c, i, 250):+.1%}")

    # ── E24 v2 三候选终裁 ──
    cw = pd.read_csv(f"{d}/crowding_daily.csv", dtype={"trade_date": str})
    cw = cw.merge(pd.DataFrame({"trade_date": dt, "close": c}), on="trade_date").reset_index(drop=True)
    cc = cw.close
    cb20 = cc.pct_change(20).shift(-20).mean()
    cb60 = cc.pct_change(60).shift(-60).mean()
    print(f"\n== E24 v2（判据同主判据·两窗≤基线−2pp+负比例≥55%）基线 20日{cb20:+.2%} 60日{cb60:+.2%}")
    for col, nm in [("dual_ratio", "S4双创占比"), ("margin_ratio", "S5两融比"), ("total_amt_yi", "S6量能")]:
        p = rollpct(pd.to_numeric(cw[col]))
        hot = p >= 0.9
        f20 = cc.pct_change(20).shift(-20)[hot]
        f60 = cc.pct_change(60).shift(-60)[hot]
        ok = (f20.mean() <= cb20 - 0.02) and (f60.mean() <= cb60 - 0.02) and ((f20 < 0).mean() >= 0.55)
        print(f"   {nm}: {int(hot.sum())}天 后20日{f20.mean():+.2%}({(f20.mean()-cb20)*100:+.1f}pp) "
              f"后60日{f60.mean():+.2%}({(f60.mean()-cb60)*100:+.1f}pp) 负比例{(f20 < 0).mean():.0%} "
              f"→ {'纳入候选' if ok else '不纳入'}")


if __name__ == "__main__":
    main()
