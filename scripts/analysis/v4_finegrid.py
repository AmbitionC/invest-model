"""补充两块：
(a) E53 细网格 —— 主线只测了 X ∈ {25,50,75,100}%，判据 1 允许「存在某个 X ∈ (0,1]」，
    小 X 未被检验过。这里补 X ∈ {5,10,15,20}%，给判据 1/2/3 一个完整答案。
(b) E55 判据 5 —— 以 episode 均值估计量做分半。
"""
from __future__ import annotations

import numpy as np

from v4_common import episodes_from_flags, load_amount, load_fear, load_legs, rolling_pct
from v4_engine import run_leg

FINE = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25]


def part_a():
    legs = load_legs()
    fear = load_fear()
    base = {}
    print("=" * 96)
    print("[a] E53 细网格：小比例底仓能否同时过判据 1(年化≥现状−0.20pp) / "
          "2(亏损年不增) / 3(|mdd|≤现状+5pp)")
    print("=" * 96)
    res = {}
    for f in FINE:
        for name, cfg in legs.items():
            on = ["B2", "B3"] if cfg["kind"] == "ladder" else ["B1", "B2"]
            res[(f, name)] = run_leg(name, cfg, fear, legs_on=on, base_frac=f)
    print(f"{'底仓':<7}" + "".join(f"{n:>26}" for n in legs) + "   判据1 判据2 判据3")
    for f in FINE:
        cells, c1, c2, c3 = [], True, True, True
        for name in legs:
            r, b = res[(f, name)], res[(0.0, name)]
            d = (r["ann"] - b["ann"]) * 100
            dm = (abs(r["mdd"]) - abs(b["mdd"])) * 100
            dl = r["nloss"] - b["nloss"]
            cells.append(f"{r['ann']*100:.2f}%({d:+.2f}) {dm:+.1f}pp {dl:+d}")
            if d < -0.20:
                c1 = False
            if dl > 0:
                c2 = False
            if dm > 5.0:
                c3 = False
        print(f"{f:<7.0%}" + "".join(f"{c:>26}" for c in cells) +
              f"   {'过' if c1 else '挂':<4} {'过' if c2 else '挂':<4} "
              f"{'过' if c3 else '挂'}")
    print("\n格式：年化(Δ年化pp) Δ|回撤|pp Δ亏损年数")


def part_b():
    print("\n" + "=" * 96)
    print("[b] E55 判据5：以 episode 均值估计量分半（切点=2015~2026 样本中点）")
    print("=" * 96)
    hs = load_legs()["沪深300"]["px"]
    amt = load_amount()
    pct = rolling_pct(amt)
    dates = amt.index.to_numpy()
    elig = pct.notna().to_numpy()
    p = hs.reindex(dates).ffill().to_numpy()
    n = len(dates)
    F = np.full(n, np.nan)
    for i in range(n):
        if i + 1 + 250 < n:
            F[i] = p[i + 251] / p[i + 1] - 1
    b = np.nanmean(F[elig])
    mid = n // 2
    print(f"切点 {dates[mid]}；基准 {b*100:+.2f}%")
    for q in [0.05, 0.10, 0.20, 0.30]:
        fl = (pct.to_numpy() <= q) & elig
        eps = episodes_from_flags(dates, fl, gap=60)
        h1, h2 = [], []
        for (s, e, c) in eps:
            m = np.zeros(n, bool)
            m[s:e + 1] = fl[s:e + 1]
            ix = np.where(m)[0]
            v = np.nanmean(F[ix])
            (h1 if s < mid else h2).append(v)
        def mm(v):
            v = [x for x in v if np.isfinite(x)]
            return (np.mean(v) - b) * 100 if v else np.nan
        m1, m2 = mm(h1), mm(h2)
        ok = len(h1) >= 2 and len(h2) >= 2
        v = ("样本不足" if not ok else
             ("不翻转" if np.sign(m1) == np.sign(m2) else "翻转"))
        print(f"q={q:.0%}: 前半 {len(h1)} ep = {m1:+.2f}pp | 后半 {len(h2)} ep = "
              f"{m2:+.2f}pp ⟹ {v}")


if __name__ == "__main__":
    part_a()
    part_b()
