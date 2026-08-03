# -*- coding: utf-8 -*-
"""E35 —— 恐慌抢买价格低位闸重构（滚动5年中位线 → 距全历史峰回撤）

判据写死于 docs/model_change_proposals.md P37 段（2026-08-02，跑数前）：
  ① 覆盖性：2015-08 与 2016-01 两轮股灾（恐慌≥90）各至少触发 1 次
  ② 不放行高位：全部触发点「截至当日历史分位」中位数 ≤ 60%
  ③ 组合不劣化：合计年化 ≥ 现状 −0.3pp 且日频最大回撤劣化 ≤ 2pp
  ④ 稳健：分半无翻转 + 门槛邻域 −20%/−30% 结论不翻转

口径：一笔钱 100（四腿各 25）／闲钱 2%／exec_lag=1／日频回撤／卖出 flat5%／买入检查周频。
只读 CSV，不落库、不改生产。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

RF, CASH = 0.02, 0.02
D0, D1 = "20150601", "20260729"
STAR_D0 = "20200601"
SRC = {
    "沪深300": ("hs300.csv", "close", D0),
    "创业板": ("spread_full.csv", "chinext", D0),
    "科创50": ("star50.csv", "close", STAR_D0),
    "红利": ("000922_csi.csv", "close", D0),
}
ANCHOR_MUL = {"创业板": 0.90}
SELL_MUL = {"创业板": 1.30 * 1.10}
RUNG, FRAC = [0.50, 0.55, 0.60, 0.65], [0.30, 0.35, 0.40, 0.50]


def prep(root: Path, nm: str) -> pd.DataFrame:
    p, col, _ = SRC[nm]
    df = pd.read_csv(root / p, dtype={"trade_date": str}).sort_values("trade_date").reset_index(drop=True)
    df["c"] = pd.to_numeric(df[col])
    c = df.c.values
    df["exp"] = [np.median(c[: i + 1]) if i >= 500 else np.nan for i in range(len(df))]
    df["r1250"] = df.c.rolling(1250).median()
    df["peak"] = df.c.cummax()
    ym = df.trade_date.str[:6]
    df["me"] = (ym != ym.shift(-1)).values
    wk = pd.to_datetime(df.trade_date).dt.isocalendar()
    w = wk.week.astype(str) + "-" + wk.year.astype(str)
    df["we"] = (w != w.shift(-1)).values
    return df


def gate_ok(mode: str, ci: float, r1250: float, peak: float, thr: float) -> bool:
    """价格低位闸：old=滚动5年中位线下方；dd=距全历史峰回撤 ≤ −thr。"""
    if mode == "old":
        return r1250 == r1250 and ci < r1250
    return ci / peak - 1 <= -thr


def run_leg(df: pd.DataFrame, nm: str, fmap: dict, mode: str, thr: float, init: float,
            cool_on_buy: bool = False, cool: int = 20):
    d = df.trade_date.values
    c = df.c.values
    i0 = int(np.searchsorted(d, SRC[nm][2]))
    i1 = len(df)
    cash, sh = init, 0.0
    last_f, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, trig = [], []
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            cash *= (1 + CASH) ** ((pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25)
        for k, fr, _t in [x for x in pend if x[2] == i]:
            if k == "B":
                a = cash * fr
                if a > 0.05:
                    sh += a / ci
                    cash -= a
            else:
                s = sh * fr
                if s > 0:
                    cash += s * ci
                    sh -= s
        pend = [x for x in pend if x[2] > i]
        r = df.iloc[i]
        sig = []
        f = fmap.get(d[i], np.nan)
        if f == f and f >= 75 and i - last_f > cool and gate_ok(mode, ci, r.r1250, r.peak, thr):
            sig.append(("B", 0.50))
            trig.append((d[i], ci, float(f), float((c[: i + 1] < ci).mean()), ci / r.peak - 1))
            if cool_on_buy:
                last_f = i          # P38：冷却按「上一次实际买入」计
        if f == f and f >= 75 and not cool_on_buy:
            last_f = i              # 现状：冷却按「上一个恐慌日」计（E36 待检验的缺陷）
        if nm == "科创50":
            dd = ci / r.peak - 1
            if dd <= -RUNG[0]:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate(RUNG) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False
                    sig.append(("B", FRAC[j]))
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * ANCHOR_MUL.get(nm, 1.0):
            sig.append(("B", 0.20))
        if r.me and r.exp == r.exp and ci > r.exp * SELL_MUL.get(nm, 1.30) and sh > 0:
            sig.append(("S", 0.05))
        for k, fr in sig:
            pend.append((k, fr, min(i + 1, i1 - 1)))
        curve.append((d[i], cash + sh * ci))
    return curve, trig


def portfolio(root: Path, mode: str, thr: float, cool_on_buy: bool = False, cool: int = 20):
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    dfs = {nm: prep(root, nm) for nm in SRC}
    cal = sorted({d for nm in SRC for d in dfs[nm].trade_date if D0 <= d <= D1})
    series, trigs = {}, []
    for nm in SRC:
        cv, tg = run_leg(dfs[nm], nm, fmap, mode, thr, 25.0, cool_on_buy, cool)
        series[nm] = pd.Series({d: v for d, v in cv})
        trigs += [(nm,) + t for t in tg]
    tot = []
    for d in cal:
        v = 0.0
        for nm in SRC:
            s = series[nm]
            if d in s.index:
                v += float(s[d])
            elif len(s) and d < s.index[0]:      # 科创50 未开始：现金吃货基
                v += 25.0 * (1 + CASH) ** ((pd.Timestamp(d) - pd.Timestamp(D0)).days / 365.25)
            elif len(s):
                v += float(s.iloc[-1])
        tot.append(v)
    v = np.array(tot)
    pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(cal[-1]) - pd.Timestamp(cal[0])).days / 365.25
    rets = pd.Series(v).pct_change().dropna()
    vol = float(rets.std() * np.sqrt(250))
    ann = (v[-1] / 100.0) ** (1 / yrs) - 1
    h = len(v) // 2
    return dict(
        ann=ann, vol=vol, sharpe=(ann - RF) / vol if vol else np.nan,
        mdd=float(((v - pk) / pk).min()), curve=v, cal=cal, trigs=trigs,
        h1=(v[h] / v[0]) ** (1 / (yrs / 2)) - 1, h2=(v[-1] / v[h]) ** (1 / (yrs / 2)) - 1,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    args = ap.parse_args()
    root = Path(args.data)

    base = portfolio(root, "old", 0.0)
    print("=" * 78)
    print(f"现状（滚动5年中位线闸）：年化 {base['ann']:+.2%} 波动 {base['vol']:.2%} "
          f"夏普 {base['sharpe']:.3f} 日频回撤 {base['mdd']:.1%} "
          f"分半 {base['h1']:+.2%}/{base['h2']:+.2%} 触发 {len(base['trigs'])} 笔")
    bp = [t[4] for t in base["trigs"]]
    print(f"  现状触发点历史分位中位数 {np.median(bp):.1%}")

    for thr in (0.25, 0.20, 0.30):
        r = portfolio(root, "dd", thr)
        tag = "主档" if thr == 0.25 else "邻域"
        pct = [t[4] for t in r["trigs"]]
        c1 = [t for t in r["trigs"] if t[1][:6] in ("201507", "201508", "201509")]
        c2 = [t for t in r["trigs"] if t[1][:6] in ("201601", "201602")]
        print("-" * 78)
        print(f"[{tag}] 距峰回撤 ≤ −{thr:.0%}：年化 {r['ann']:+.2%} 波动 {r['vol']:.2%} "
              f"夏普 {r['sharpe']:.3f} 日频回撤 {r['mdd']:.1%} "
              f"分半 {r['h1']:+.2%}/{r['h2']:+.2%} 触发 {len(r['trigs'])} 笔")
        print(f"  ① 2015 股灾触发 {len(c1)} 笔 / 2016 熔断触发 {len(c2)} 笔")
        print(f"  ② 触发点历史分位中位数 {np.median(pct):.1%}（判据 ≤60%）")
        print(f"  ③ Δ年化 {r['ann'] - base['ann']:+.2%}pp（判据 ≥ −0.30pp）｜"
              f"Δ回撤 {(r['mdd'] - base['mdd']) * 100:+.1f}pp（判据劣化 ≤2pp）")
        if thr == 0.25:
            for t in sorted(r["trigs"], key=lambda x: x[1])[:40]:
                print(f"     {t[0]:6s} {t[1]} 价{t[2]:8.1f} 恐慌{t[3]:5.1f} "
                      f"历史分位{t[4]:6.1%} 距峰{t[5]:+7.1%}")


if __name__ == "__main__":
    main()


def e36() -> None:
    """E36（P38 段判据·跑数前写死）：冷却按买入计 × 价格闸 两维邻域网格。"""
    import sys
    root = Path(sys.argv[sys.argv.index("--data") + 1] if "--data" in sys.argv else ".")
    hs = pd.read_csv(root / "hs300.csv", dtype={"trade_date": str})
    hs["c"] = pd.to_numeric(hs.close)
    lo15 = float(hs[(hs.trade_date >= "20150601") & (hs.trade_date <= "20150930")].c.min())
    lo16 = float(hs[(hs.trade_date >= "20160101") & (hs.trade_date <= "20160331")].c.min())
    print(f"\n2015 轮沪深300 最低收盘 {lo15:.0f}（判据线 ×1.15 = {lo15*1.15:.0f}）"
          f"｜2016 轮 {lo16:.0f}（判据线 {lo16*1.15:.0f}）")
    base = portfolio(root, "old", 0.0)
    print(f"现状基线：年化 {base['ann']:+.2%} 回撤 {base['mdd']:.1%} 恐慌买 {len(base['trigs'])} 笔"
          f" 分半 {base['h1']:+.2%}/{base['h2']:+.2%}")
    print("\n" + "=" * 96)
    print(f"{'冷却':>4} {'门槛':>6} {'年化':>8} {'Δ年化':>8} {'夏普':>6} {'回撤':>7} {'Δ回撤':>7} "
          f"{'笔数':>5} {'①15轮':>7} {'①16轮':>7} {'分半Δ同号':>9}")
    print("=" * 96)
    for cool in (10, 20, 30):
        for thr in (0.20, 0.25, 0.30):
            r = portfolio(root, "dd", thr, cool_on_buy=True, cool=cool)
            hs300 = [t for t in r["trigs"] if t[0] == "沪深300"]
            a = [t for t in hs300 if "20150601" <= t[1] <= "20150930" and t[2] <= lo15 * 1.15]
            b = [t for t in hs300 if "20160101" <= t[1] <= "20160331" and t[2] <= lo16 * 1.15]
            d1 = r["h1"] - base["h1"]; d2 = r["h2"] - base["h2"]
            print(f"{cool:>4} {thr:>6.0%} {r['ann']:>8.2%} {r['ann']-base['ann']:>+8.2%} "
                  f"{r['sharpe']:>6.3f} {r['mdd']:>7.1%} {(r['mdd']-base['mdd'])*100:>+7.1f} "
                  f"{len(r['trigs']):>5} {len(a):>7} {len(b):>7} "
                  f"{'是' if d1*d2 > 0 else '否':>9}")
    print("=" * 96)
    r = portfolio(root, "dd", 0.25, cool_on_buy=True, cool=20)
    print("\n主档（冷却20·门槛−25%）沪深300 在两轮崩盘中的成交明细：")
    for t in sorted([x for x in r["trigs"] if x[0] == "沪深300" and x[1] < "20170101"], key=lambda x: x[1]):
        print(f"   {t[1]} 价{t[2]:8.1f} 恐慌{t[3]:5.1f} 历史分位{t[4]:6.1%} 距峰{t[5]:+7.1%}")
