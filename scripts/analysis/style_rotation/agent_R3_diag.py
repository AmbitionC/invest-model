#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""R3 诊断：期间稳健性 / 块自举显著性 / episode 清点 / 与 E24 v2 绝对维度对照。"""
from __future__ import annotations
import os
import numpy as np
import pandas as pd
from scipy import stats
from agent_R3_flows import (load_all, build_signals, build_targets, episodes,
                            FWD, EXTREME_Q, SIGNALS_DOC)

BASE = os.path.dirname(os.path.abspath(__file__))
RNG = np.random.default_rng(20260802)


def block_bootstrap_p(s, y, rho_obs, block=FWD, n=2000):
    """循环块自举：打乱信号的块顺序，保留 y 的时间结构 → 重叠窗自相关下的诚实 p。"""
    m = len(s)
    nb = int(np.ceil(m / block))
    cnt = 0
    for _ in range(n):
        starts = RNG.integers(0, m, nb)
        idx = np.concatenate([np.arange(st, st + block) % m for st in starts])[:m]
        r, _ = stats.spearmanr(s[idx], y)
        if abs(r) >= abs(rho_obs):
            cnt += 1
    return (cnt + 1) / (n + 1)


def seg_table(df, sc, tc, k=3):
    d = df[["trade_date", sc, tc]].dropna().reset_index(drop=True)
    out = []
    bnd = np.linspace(0, len(d), k + 1).astype(int)
    for i in range(k):
        seg = d.iloc[bnd[i]:bnd[i + 1]]
        r, p = stats.spearmanr(seg[sc], seg[tc])
        out.append({"seg": f"{i+1}/{k}", "start": int(seg.trade_date.iloc[0]),
                    "end": int(seg.trade_date.iloc[-1]), "n": len(seg),
                    "rho": round(float(r), 4), "p": round(float(p), 4)})
    return pd.DataFrame(out)


def episode_list(df, sc, tc, q=EXTREME_Q):
    d = df[["trade_date", sc, tc]].dropna().reset_index(drop=True)
    lo, hi = d[sc].quantile(q), d[sc].quantile(1 - q)
    out = []
    for name, mask in (("low", d[sc] <= lo), ("high", d[sc] >= hi)):
        idx = np.flatnonzero(mask.values)
        if idx.size == 0:
            continue
        grp = np.split(idx, np.flatnonzero(np.diff(idx) >= 20) + 1)
        for g in grp:
            out.append({"side": name, "start": int(d.trade_date.iloc[g[0]]),
                        "end": int(d.trade_date.iloc[g[-1]]), "days": len(g),
                        "mean_fwd_rel": round(float(d[tc].iloc[g].mean()), 4)})
    return pd.DataFrame(out).sort_values("start").reset_index(drop=True)


def main():
    px, crowd, fear, etf = load_all()
    sig, _ = build_signals(px, crowd, fear, etf)
    tgt = build_targets(px)
    df = sig.merge(tgt, on="trade_date", how="inner")

    # 绝对维度对照（E24 v2 口径）：同样的拥挤度信号 → 未来60日「双创绝对收益」与「沪深300绝对收益」
    p2 = px.copy()
    p2["abs_chinext"] = p2.chinext.shift(-FWD) / p2.chinext - 1
    p2["abs_star"] = p2.star.shift(-FWD) / p2.star - 1
    p2["abs_hs300"] = p2.hs300.shift(-FWD) / p2.hs300 - 1
    df = df.merge(p2[["trade_date", "abs_chinext", "abs_star", "abs_hs300"]], on="trade_date")

    print("=" * 100)
    print("【A】相对维度 vs 绝对维度（回应 E24 v2）")
    rows = []
    for sc in ["F3_dual_ratio_q250", "F4_dual_ratio_dev", "F1g_etf_flow_growth",
               "F2_margin_mom", "F5_turnover_q250"]:
        for tc in ["rel_chinext", "rel_star", "abs_chinext", "abs_star", "abs_hs300"]:
            d = df[[sc, tc]].dropna()
            r, p = stats.spearmanr(d[sc], d[tc])
            rows.append({"signal": sc, "target": tc, "n": len(d),
                         "rho": round(float(r), 4), "p": round(float(p), 5)})
    tab_a = pd.DataFrame(rows)
    print(tab_a.pivot(index="signal", columns="target", values="rho").to_string())
    tab_a.to_csv(os.path.join(BASE, "agent_R3_diag_absrel.csv"), index=False)

    print("\n" + "=" * 100)
    print("【B】期间稳健性：同一信号在两条成长腿的可比区间上")
    STAR_START = 20191231
    for sc in ["F3_dual_ratio_q250", "F4_dual_ratio_dev", "F1g_etf_flow_growth", "F2_margin_mom"]:
        for tc in ["rel_chinext", "rel_star"]:
            for lab, sub in (("全样本", df), ("科创50可比区间(2019-12起)", df[df.trade_date >= STAR_START]),
                             ("科创50之前", df[df.trade_date < STAR_START])):
                d = sub[[sc, tc]].dropna()
                if len(d) < 200:
                    print(f"{sc:22s} {tc:12s} {lab:22s} n={len(d):5d}  样本不足")
                    continue
                r, p = stats.spearmanr(d[sc], d[tc])
                print(f"{sc:22s} {tc:12s} {lab:22s} n={len(d):5d}  rho={r:+.4f} p={p:.4f}")
        print("-" * 90)

    print("\n" + "=" * 100)
    print("【C】三段稳健性（判据④分半无翻转的加严版）")
    for sc, tc in [("F3_dual_ratio_q250", "rel_star"), ("F4_dual_ratio_dev", "rel_star"),
                   ("F1g_etf_flow_growth", "rel_chinext"), ("F2_margin_mom", "rel_star"),
                   ("F5_turnover_q250", "rel_chinext")]:
        print(f"\n-- {sc} → {tc}")
        print(seg_table(df, sc, tc, 3).to_string(index=False))

    print("\n" + "=" * 100)
    print("【D】块自举 p 值（block=60，2000 次；重叠窗自相关下的诚实显著性）")
    boot = []
    for sc, tc in [("F3_dual_ratio_q250", "rel_star"), ("F4_dual_ratio_dev", "rel_star"),
                   ("F1g_etf_flow_growth", "rel_chinext"), ("F2_margin_mom", "rel_star"),
                   ("F5_turnover_q250", "rel_chinext"), ("F1_etf_flow_diff", "rel_star")]:
        d = df[[sc, tc]].dropna()
        r, p_naive = stats.spearmanr(d[sc], d[tc])
        pb = block_bootstrap_p(d[sc].values, d[tc].values, r)
        boot.append({"signal": sc, "target": tc, "n": len(d), "rho": round(float(r), 4),
                     "p_naive": round(float(p_naive), 6), "p_block_boot": round(pb, 4)})
        print(boot[-1])
    pd.DataFrame(boot).to_csv(os.path.join(BASE, "agent_R3_diag_boot.csv"), index=False)

    print("\n" + "=" * 100)
    print("【E】episode 清点（真实宏观段落 vs 机械计数）")
    for sc, tc in [("F3_dual_ratio_q250", "rel_star"), ("F4_dual_ratio_dev", "rel_star"),
                   ("F1g_etf_flow_growth", "rel_chinext")]:
        el = episode_list(df, sc, tc)
        print(f"\n-- {sc} → {tc}  机械 episode 数={len(el)}")
        print(el.to_string(index=False))
        el.to_csv(os.path.join(BASE, f"agent_R3_episodes_{sc}.csv"), index=False)


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    main()
