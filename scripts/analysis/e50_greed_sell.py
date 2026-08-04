# -*- coding: utf-8 -*-
"""E50 贪婪共振「大幅卖出」（判据见 P57，跑数前已写死，本脚本不得改判据）

价格 > exp×S 且 恐慌 ≤ G → 卖持仓 F%；否则按现行卖 5%。四个 (G,F) 组合一律并报。
只读 CSV，不落库、不联网。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from long_window_backtest import CASH, FRAC, LEGS, RF, RUNG, first_tradable, prep


def run_g(df, ret, fmap, nm, d0, d1, mode, *, greed=None, big=None, init=100.0):
    d, c = df.trade_date.values, df.c.values
    rr = ret.pct_change().fillna(0).values if ret is not None else None
    i0, i1 = int(np.searchsorted(d, d0)), int(np.searchsorted(d, d1, side="right"))
    mul = 1.30 * 1.10 if nm == "创业板" else 1.30
    cash, units, nav = init, 0.0, 1.0
    last, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, pos, nb, ns, nbig = [], [], 0, 0, 0
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            cash *= (1 + CASH) ** ((pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25)
            nav = nav * (1 + rr[i]) if rr is not None else ci
        elif rr is None:
            nav = ci
        r = df.iloc[i]
        for k, fr, _t in [x for x in pend if x[2] == i]:
            if k == "B":
                a = cash * fr
                if a > 0.05:
                    units += a / nav; cash -= a; nb += 1
            else:
                sq = units * fr
                if sq > 0:
                    cash += sq * nav; units -= sq; ns += 1
        pend = [x for x in pend if x[2] > i]
        sig, f = [], fmap.get(d[i], np.nan)
        if f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and ci < r.r1250:
            sig.append(("B", 0.50))
        if f == f and f >= 75:
            last = i
        if mode == "ladder":
            dd = ci / r.peak - 1
            if dd <= -RUNG[0]:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate(RUNG) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False; sig.append(("B", FRAC[j]))
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            sig.append(("B", 0.20))
        if r.me and r.exp == r.exp and ci > r.exp * mul and units > 0:
            fr = 0.05
            if greed is not None and f == f and f <= greed:
                fr = big; nbig += 1
            sig.append(("S", fr))
        for k, fr in sig:
            pend.append((k, fr, min(i + 1, i1 - 1)))
        tv = cash + units * nav
        curve.append(tv); pos.append(units * nav / tv)
    return dict(curve=np.array(curve), dates=d[i0:i1], nb=nb, ns=ns, nbig=nbig,
                posavg=float(np.mean(pos)))


def stats(v, cal):
    v = np.asarray(v); pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(cal[-1]) - pd.Timestamp(cal[0])).days / 365.25
    ann = (v[-1] / v[0]) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    return dict(ann=ann, sharpe=(ann - RF) / vol, mdd=float(((v - pk) / pk).min()), yrs=yrs)


def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument("--data", default="."); a = ap.parse_args()
    root = Path(a.data)
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    data = {nm: prep(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}
    st = {nm: first_tradable(data[nm][0], m, fx) for nm, _, _, _, fx, m in LEGS}
    en = {nm: str(data[nm][0].trade_date.iloc[-1]) for nm in data}
    md = {nm: m for nm, _, _, _, _, m in LEGS}

    def portfolio(kw, names, d_start=None, d_end=None):
        ser = {}
        for n in names:
            df, ret = data[n]
            r = run_g(df, ret, fmap, n, st[n], en[n], md[n], **kw)
            ser[n] = pd.Series(r["curve"], index=list(r["dates"]))
        cal = sorted(set().union(*[set(s.index) for s in ser.values()]))
        if d_start: cal = [x for x in cal if x >= d_start]
        if d_end:   cal = [x for x in cal if x <= d_end]
        tot = []
        for x in cal:
            v = 0.0
            for n in names:
                s = ser[n]
                v += float(s[x]) if x in s.index else (
                    100.0 * (1 + CASH) ** ((pd.Timestamp(x) - pd.Timestamp(cal[0])).days / 365.25)
                    if x < s.index[0] else float(s.iloc[-1]))
            tot.append(v)
        return stats(tot, cal), ser

    VAR = [("现状 flat5%", dict())] + [
        (f"贪婪≤{g} 卖{int(fr*100)}%", dict(greed=g, big=fr))
        for g in (10, 7) for fr in (0.30, 0.50)]
    TWO, FOUR = ["沪深300", "红利"], list(data)
    d4 = max(st.values())
    print("=" * 100)
    print("E50 贪婪共振大幅卖出（判据见 P57，跑数前写死）")
    print("=" * 100)
    res = {}
    for lab, kw in VAR:
        two, _ = portfolio(kw, TWO)
        four, _ = portfolio(kw, FOUR, d_start=d4)
        # 分半（两腿窗口）
        ser = portfolio(kw, TWO)[1]
        cal = sorted(set().union(*[set(s.index) for s in ser.values()]))
        mid = cal[len(cal) // 2]
        h1, _ = portfolio(kw, TWO, d_end=mid)
        h2, _ = portfolio(kw, TWO, d_start=mid)
        nbig = sum(run_g(*data[n], fmap, n, st[n], en[n], md[n], **kw)["nbig"]
                   if False else 0 for n in FOUR)
        res[lab] = dict(two=two, four=four, h1=h1, h2=h2)
    b = res["现状 flat5%"]
    print(f"\n  {'方案':18s}｜{'两腿全窗 19.5y':^34s}｜{'四腿共同在场 6.6y':^30s}")
    print(f"  {'':18s}｜{'年化':>8s}{'夏普':>8s}{'回撤':>9s}{'Δ夏普':>9s}｜"
          f"{'年化':>8s}{'夏普':>8s}{'回撤':>9s}")
    for lab, _ in VAR:
        r = res[lab]
        print(f"  {lab:18s}｜{r['two']['ann']:>8.2%}{r['two']['sharpe']:>8.3f}"
              f"{r['two']['mdd']:>9.1%}{r['two']['sharpe'] - b['two']['sharpe']:>+9.3f}｜"
              f"{r['four']['ann']:>8.2%}{r['four']['sharpe']:>8.3f}{r['four']['mdd']:>9.1%}")

    print(f"\n  判据（须≥3/4 且②必过；两个组合口径都要满足）：")
    print(f"  {'方案':18s}{'①年化≥−0.30pp':>16s}{'②夏普≥+0.03':>14s}"
          f"{'③|回撤|≤基线−2pp':>18s}{'④分半不翻转':>14s}{'判定':>10s}")
    for lab, _ in VAR[1:]:
        r = res[lab]
        c1 = all((r[k]["ann"] - b[k]["ann"]) * 100 >= -0.30 for k in ("two", "four"))
        c2 = all(r[k]["sharpe"] - b[k]["sharpe"] >= 0.03 for k in ("two", "four"))
        c3 = all(abs(r[k]["mdd"]) <= abs(b[k]["mdd"]) - 0.02 for k in ("two", "four"))
        c4 = np.sign(r["h1"]["ann"] - b["h1"]["ann"]) == np.sign(r["h2"]["ann"] - b["h2"]["ann"])
        n = sum([c1, c2, c3, c4])
        ok = n >= 3 and c2
        print(f"  {lab:18s}{'✅' if c1 else '❌':>16s}{'✅' if c2 else '❌':>14s}"
              f"{'✅' if c3 else '❌':>18s}{'✅' if c4 else '❌':>14s}"
              f"{f'{n}/4 ' + ('**通过**' if ok else '未通过'):>10s}")
    print("\n  ⚠ 四个 (G,F) 组合一律并报（P57 明令），无事后择优。")


if __name__ == "__main__":
    main()
