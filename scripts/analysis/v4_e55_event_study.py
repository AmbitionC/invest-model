"""E55 命题E（成交量地量买点）——事件级 / 统计推断路线。

不做逐日回测复算（另一条路线负责），只回答「这些数字有多少是运气」。

产出：
  1. 事件级 episode 划分与逐 episode 前瞻超额（判据 1）
  2. 循环移位置换检验（保留信号聚集结构与收益自相关）+ episode 级 bootstrap CI
  3. 有效独立周期数（自相关修正 / episode 计数 / 窗口重叠修正）
  4. 前视自查（同日 vs exec_lag=1、滚动分位是否偷看未来）
  5. 分半（判据 5）
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v4_common import (RESULTS, episodes_from_flags, eff_sample_size, load_amount,
                       load_fear, load_legs, rolling_pct, to_dt)

QS = [0.05, 0.10, 0.20, 0.30]
HORIZONS = [20, 60, 120, 250]
GAP = 60
RNG = np.random.default_rng(20260805)


def build_signal(window=750, min_periods=250):
    amt = load_amount()
    pct = rolling_pct(amt, window=window, min_periods=min_periods)
    return amt, pct


def fwd_returns(px: pd.Series, dates: np.ndarray, h: int, lag: int = 1):
    """在 dates[i] 收到信号，lag 个交易日后收盘买入，持有 h 个交易日的收益。

    px 需按日期对齐到 dates（信号日历）。返回与 dates 等长的数组（不足则 NaN）。
    """
    p = px.reindex(dates).to_numpy(float)
    n = len(p)
    out = np.full(n, np.nan)
    for i in range(n):
        a, b = i + lag, i + lag + h
        if b < n and np.isfinite(p[a]) and np.isfinite(p[b]):
            out[i] = p[b] / p[a] - 1.0
    return out


def main():
    legs = load_legs()
    hs300 = legs["沪深300"]["px"]
    fear = load_fear()

    print("=" * 78)
    print("V4 / E55 事件级统计推断")
    print("=" * 78)

    # ---------- 0. 前视自查 ----------
    amt, pct = build_signal()
    print(f"\n[0] 数据：成交额 {amt.index[0]}~{amt.index[-1]}，{len(amt)} 交易日")
    # 自查 A：滚动分位是否只用当日及之前数据 —— 截断重算比对
    cut = 2000
    pct_trunc = rolling_pct(amt.iloc[:cut])
    same = np.allclose(pct_trunc.to_numpy()[-50:], pct.to_numpy()[cut - 50:cut],
                       equal_nan=True)
    print(f"[0] 前视自查A（截断重算末50日分位一致）: {same}")
    # 自查 B：分位定义只含 <=，窗口右端为当日
    print(f"[0] 前视自查B: 窗口=[t-749,t] 含当日；min_periods=250 ⟹ "
          f"首个可用日 {pct.dropna().index[0]}")

    dates = amt.index.to_numpy()
    # 信号日历上的价格（成交额日历 ⊂ 交易日历）
    px_al = hs300.reindex(dates)
    print(f"[0] 沪深300 在成交额日历上缺失 {int(px_al.isna().sum())} 天")

    # ---------- 1. episode 划分与逐 episode 前瞻超额 ----------
    print("\n" + "-" * 78)
    print("[1] 判据1：事件级 episode（gap>60td）与 250 日前瞻超额")
    print("-" * 78)

    elig = pct.notna().to_numpy()          # 分位可用的评估样本
    base = {}
    fw = {}
    for h in HORIZONS:
        f = fwd_returns(hs300, dates, h, lag=1)
        fw[h] = f
        m = elig & np.isfinite(f)
        base[h] = float(np.nanmean(f[m]))
    print("全样本基准（分位可用区间, exec_lag=1）: " +
          "  ".join(f"{h}d={base[h]*100:+.2f}%" for h in HORIZONS))

    ep_table = {}
    for q in QS:
        flags = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, flags, gap=GAP)
        rows = []
        for (s, e, c) in eps:
            row = dict(start=int(dates[s]), end=int(dates[e]), ndays=c)
            for h in HORIZONS:
                row[f"x{h}"] = fw[h][s] - base[h] if np.isfinite(fw[h][s]) else np.nan
            rows.append(row)
        df = pd.DataFrame(rows)
        ep_table[q] = df
        n_ok = int(np.isfinite(df["x250"]).sum())
        pos = float((df["x250"] > 0).sum()) / n_ok if n_ok else np.nan
        print(f"\nq={q:.0%}: 触发日 {int(flags.sum())}，episode {len(eps)} 个"
              f"（250d 可评 {n_ok} 个，正比例 {pos:.0%}）")
        with pd.option_context("display.width", 200):
            print(df.to_string(index=False,
                               formatters={c: (lambda v: f"{v*100:+.1f}%"
                                               if np.isfinite(v) else "  n/a")
                                           for c in ["x20", "x60", "x120", "x250"]}))

    # ---------- 2. 置换检验 / bootstrap ----------
    print("\n" + "-" * 78)
    print("[2] 循环移位置换检验（保留信号聚集结构 + 收益自相关）")
    print("-" * 78)
    NPERM = 5000
    for q in QS:
        flags = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, flags, gap=GAP)
        starts = np.array([s for (s, e, c) in eps])
        for h in [60, 250]:
            f = fw[h]
            obs_v = f[starts]
            obs = float(np.nanmean(obs_v)) - base[h]
            n = len(dates)
            null = []
            for _ in range(NPERM):
                sh = RNG.integers(1, n)
                ss = (starts + sh) % n
                v = f[ss]
                if np.isfinite(v).sum() >= max(2, len(starts) // 2):
                    null.append(np.nanmean(v) - base[h])
            null = np.array(null)
            p = float((null >= obs).mean())
            lo, hi = np.percentile(null, [2.5, 97.5])
            # episode 级 bootstrap CI（对 episode 重抽样）
            vv = obs_v[np.isfinite(obs_v)]
            bs = np.array([np.mean(RNG.choice(vv, len(vv), replace=True))
                           for _ in range(5000)]) - base[h]
            print(f"q={q:.0%} h={h:3d}: 观测超额 {obs*100:+6.2f}pp | "
                  f"置换 p={p:.3f} 零分布95%[{lo*100:+.1f},{hi*100:+.1f}]pp | "
                  f"episode bootstrap 95%CI [{np.percentile(bs,2.5)*100:+.1f},"
                  f"{np.percentile(bs,97.5)*100:+.1f}]pp")

    # ---------- 3. 有效独立周期数 ----------
    print("\n" + "-" * 78)
    print("[3] 有效独立周期数")
    print("-" * 78)
    for q in QS:
        flags = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, flags, gap=GAP)
        nd = int(flags.sum())
        neff_ac = eff_sample_size(flags.astype(float))
        # 250 日重叠窗口修正：触发日数 / 250
        neff_ov = nd / 250.0
        # 跨自然年数
        yrs = sorted({int(d) // 10000 for d in dates[flags]})
        print(f"q={q:.0%}: 触发日 {nd} | episode {len(eps)} | "
              f"AR(1)修正等效样本 {neff_ac:.1f} | 250d重叠修正 {neff_ov:.1f} | "
              f"覆盖年份 {yrs}")

    # ---------- 4. 分半（判据 5） ----------
    print("\n" + "-" * 78)
    print("[4] 判据5：分半（切点=2015~2026 样本中点）")
    print("-" * 78)
    mid = len(dates) // 2
    print(f"切点：{dates[mid]}（第 {mid} 个交易日 / 共 {len(dates)}）")
    for q in QS:
        flags = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, flags, gap=GAP)
        h1 = [e for e in eps if e[0] < mid]
        h2 = [e for e in eps if e[0] >= mid]
        def _m(es, h=250):
            v = [fw[h][s] for (s, _, _) in es]
            v = [x for x in v if np.isfinite(x)]
            return (np.mean(v) - base[h]) if v else np.nan, len(v)
        m1, n1 = _m(h1)
        m2, n2 = _m(h2)
        verdict = ("样本不足" if (len(h1) < 2 or len(h2) < 2)
                   else ("不翻转" if (np.isfinite(m1) and np.isfinite(m2)
                                    and np.sign(m1) == np.sign(m2)) else "翻转"))
        print(f"q={q:.0%}: 前半 episode {len(h1)}（可评{n1}）超额 "
              f"{m1*100 if np.isfinite(m1) else float('nan'):+.1f}pp | "
              f"后半 episode {len(h2)}（可评{n2}）超额 "
              f"{m2*100 if np.isfinite(m2) else float('nan'):+.1f}pp ⟹ {verdict}")

    # ---------- 5. 敏感性：min_periods / window ----------
    print("\n" + "-" * 78)
    print("[5] 口径敏感性（滚动窗口 / 预热）")
    print("-" * 78)
    for win, mp in [(750, 250), (750, 750), (500, 250), (1000, 250)]:
        _, p2 = build_signal(win, mp)
        e2 = p2.notna().to_numpy()
        for q in [0.10]:
            fl = (p2.to_numpy() <= q) & e2
            eps = episodes_from_flags(dates, fl, gap=GAP)
            v = [fw[250][s] for (s, _, _) in eps if np.isfinite(fw[250][s])]
            print(f"window={win} min_periods={mp} q={q:.0%}: 触发日 {int(fl.sum())}, "
                  f"episode {len(eps)}, 首个可用日 {p2.dropna().index[0]}, "
                  f"250d 超额均值 {np.mean(v)*100 if v else float('nan'):+.1f}pp, "
                  f"正比例 {np.mean([x>0 for x in v])*100 if v else float('nan'):.0f}%")

    # ---------- 6. 与既有腿重叠（正交性事实核对） ----------
    print("\n" + "-" * 78)
    print("[6] 与既有买腿的重叠（事实核对）")
    print("-" * 78)
    exp_med = pd.Series(hs300).expanding(min_periods=500).median()
    below = exp_med.reindex(dates).to_numpy() > hs300.reindex(dates).to_numpy()
    fear_hi = (fear.reindex(dates).to_numpy() >= 75)
    for q in QS:
        fl = (pct.to_numpy() <= q) & elig
        n = int(fl.sum())
        print(f"q={q:.0%}: 触发日 {n} | 与「价<中位线」重叠 "
              f"{np.nansum(fl & below)/n:.0%} | 与「恐慌≥75」重叠 "
              f"{np.nansum(fl & fear_hi)/n:.0%}")

    # ---------- 7. exec_lag 与前视代价 ----------
    print("\n" + "-" * 78)
    print("[7] 判据8：exec_lag 敏感性（lag=0 属前视）")
    print("-" * 78)
    for q in [0.10]:
        fl = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, fl, gap=GAP)
        starts = [s for (s, _, _) in eps]
        for lag in [0, 1, 2]:
            f = fwd_returns(hs300, dates, 250, lag=lag)
            v = [f[s] for s in starts if np.isfinite(f[s])]
            print(f"q={q:.0%} lag={lag}: 250d 原始收益均值 {np.mean(v)*100:+.2f}%"
                  f"（n={len(v)}）")

    # 存 episode 明细供报告引用
    out = []
    for q in QS:
        d = ep_table[q].copy()
        d.insert(0, "q", q)
        out.append(d)
    pd.concat(out).to_csv(f"{RESULTS}/../scripts/analysis/v4_e55_episodes.csv",
                          index=False)


if __name__ == "__main__":
    main()
