"""E55 判据 2/3/4/6/7：把 B4（成交额滚动分位买腿）接进最小组合口径。

只做判据所需的最小计算：四档 q × 三档 k、增量检验、竞争/独立两套口径。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v4_common import load_amount, load_fear, load_legs, rolling_pct
from v4_engine import run_leg, show

QS = [0.05, 0.10, 0.20, 0.30]
KS = [0.10, 0.20, 0.30]


def main():
    legs = load_legs()
    fear = load_fear()
    amt = load_amount()
    pct = rolling_pct(amt)

    def run(b4q=None, k=0.20, legs_on_override=None, competing=True, S=None):
        out = {}
        for name, cfg in legs.items():
            base = ["B2", "B3"] if cfg["kind"] == "ladder" else ["B1", "B2"]
            on = list(base) if legs_on_override is None else \
                [x for x in legs_on_override if x in base + ["B4"]]
            if b4q is not None and "B4" not in on:
                on = on + ["B4"]
            out[name] = run_leg(name, cfg, fear, S_mult=S, legs_on=on,
                                b4_pct=pct if b4q is not None else None,
                                b4_q=b4q, b4_k=k, competing=competing)
        return out

    print("=" * 90)
    print("[基线] 现状（B1+B2 / 科创50 B2+B3），卖出闸 ×1.00 生产口径")
    print("=" * 90)
    base = run()
    show(list(base.values()), "基线")

    print("\n" + "=" * 90)
    print("[判据3/4] 接入 B4 后四腿指标（k=20%，竞争口径）")
    print("=" * 90)
    grid = {}
    for q in QS:
        r = run(b4q=q, k=0.20)
        grid[q] = r
        show(list(r.values()), f"q={q:.0%} k=20%")
        print("   Δ年化 vs 基线: " + "  ".join(
            f"{n}={(r[n]['ann']-base[n]['ann'])*100:+.2f}pp" for n in legs))
        print("   |mdd| 变化    : " + "  ".join(
            f"{n}={(abs(r[n]['mdd'])-abs(base[n]['mdd']))*100:+.2f}pp" for n in legs))

    print("\n" + "=" * 90)
    print("[判据2] q 四档单调且同号？（Δ年化 pp，k=20%，竞争口径）")
    print("=" * 90)
    print(f"{'腿':<8}" + "".join(f"{f'q={q:.0%}':>10}" for q in QS) + "   单调?  同号?")
    for n in legs:
        d = [(grid[q][n]["ann"] - base[n]["ann"]) * 100 for q in QS]
        mono = all(d[i] >= d[i + 1] for i in range(3)) or \
            all(d[i] <= d[i + 1] for i in range(3))
        same = all(x > 0 for x in d) or all(x < 0 for x in d)
        print(f"{n:<8}" + "".join(f"{x:>+10.2f}" for x in d) +
              f"    {'是' if mono else '否':<5} {'是' if same else '否'}")

    print("\n" + "=" * 90)
    print("[k 敏感性] q=10%，k ∈ {10%,20%,30%}（Δ年化 pp）")
    print("=" * 90)
    for k in KS:
        r = run(b4q=0.10, k=k)
        print(f"k={k:.0%}: " + "  ".join(
            f"{n}={(r[n]['ann']-base[n]['ann'])*100:+.2f}pp" for n in legs))

    print("\n" + "=" * 90)
    print("[判据6] 增量检验：B1+B2 → B1+B2+B4（q=10%,k=20%）")
    print("=" * 90)
    r12 = run(legs_on_override=["B1", "B2", "B3"])
    r124 = run(b4q=0.10, k=0.20, legs_on_override=["B1", "B2", "B3", "B4"])
    r4only = run(b4q=0.10, k=0.20, legs_on_override=["B4"])
    print(f"{'腿':<8}{'B1+B2':>10}{'B1+B2+B4':>12}{'增量pp':>10}{'仅B4':>10}"
          f"{'判据≥+0.30':>12}")
    for n in legs:
        inc = (r124[n]["ann"] - r12[n]["ann"]) * 100
        print(f"{n:<8}{r12[n]['ann']*100:>9.2f}%{r124[n]['ann']*100:>11.2f}%"
              f"{inc:>+10.2f}{r4only[n]['ann']*100:>9.2f}%"
              f"{'PASS' if inc >= 0.30 else 'FAIL':>12}")

    print("\n" + "=" * 90)
    print("[判据7 / M1] 竞争口径 vs 独立口径（q=10%,k=20%，Δ年化 vs 各自基线）")
    print("=" * 90)
    b_ind = run(competing=False)
    r_ind = run(b4q=0.10, k=0.20, competing=False)
    print(f"{'腿':<8}{'竞争Δ':>10}{'独立Δ':>10}")
    for n in legs:
        print(f"{n:<8}{(grid[0.10][n]['ann']-base[n]['ann'])*100:>+10.2f}"
              f"{(r_ind[n]['ann']-b_ind[n]['ann'])*100:>+10.2f}")

    print("\n" + "=" * 90)
    print("[窗口修正] B4 只在 2016-01 后可用；限定 2016+ 公平窗口的 Δ年化")
    print("=" * 90)
    for q in QS:
        r = grid[q]
        line = []
        for n in legs:
            d0 = base[n]["dates"]
            m = d0 >= 20160111
            if m.sum() < 250:
                line.append(f"{n}=n/a")
                continue
            yrs = m.sum() / 250.0
            a_b = (base[n]["nav"][m][-1] / base[n]["nav"][m][0]) ** (1 / yrs) - 1
            a_r = (r[n]["nav"][m][-1] / r[n]["nav"][m][0]) ** (1 / yrs) - 1
            line.append(f"{n}={(a_r-a_b)*100:+.2f}pp")
        print(f"q={q:.0%}: " + "  ".join(line))

    print("\n" + "=" * 90)
    print("[判据5 辅助] 分半：2016~2026 中点切，两半各自 Δ年化（q=10%,k=20%）")
    print("=" * 90)
    for n in legs:
        d0 = base[n]["dates"]
        m = d0 >= 20160111
        idx = np.where(m)[0]
        if len(idx) < 500:
            print(f"{n}: 可用区间不足，样本不足")
            continue
        mid = idx[len(idx) // 2]
        outs = []
        for lab, sl in [("前半", idx[:len(idx) // 2]), ("后半", idx[len(idx) // 2:])]:
            yrs = len(sl) / 250.0
            a_b = (base[n]["nav"][sl][-1] / base[n]["nav"][sl][0]) ** (1 / yrs) - 1
            a_r = (grid[0.10][n]["nav"][sl][-1] /
                   grid[0.10][n]["nav"][sl][0]) ** (1 / yrs) - 1
            outs.append((lab, (a_r - a_b) * 100))
        flip = "翻转" if outs[0][1] * outs[1][1] < 0 else "不翻转"
        print(f"{n}: 切点 {d0[mid]}  " +
              "  ".join(f"{l}={v:+.2f}pp" for l, v in outs) + f"  ⟹ {flip}")


if __name__ == "__main__":
    main()
