# -*- coding: utf-8 -*-
"""卖出闸倍数细网格扫描（owner 2026-08-05：「1.3 就是最好的吗」）。

粗网格（1.00/1.15/1.30/1.50）只能看出方向，看不出 1.30 是**平顶上的一点**还是**针尖**。
本脚本把闸位按 0.025 步长扫一遍，并做三项防过拟合检查：

  ①  形状：年化/夏普随闸位的曲线，看极值点周围是平的还是尖的
  ②  分半 argmax：前后两半各自的最优闸位——若两半的最优点差很远，
      说明「最优」是噪声，不是结构
  ③  邻域敏感性：最优点 ±0.10 内的极差，与「换一条腿」的差异做对比

**这个脚本不是用来挑参数的。** 按已写死的 SOP，事后按样本内最优挑闸位＝曲线拟合；
它的用途是回答「1.30 落在什么位置」，以及「选它要付多少代价」。

只读 results/*.csv，不落库、不联网。
"""
from __future__ import annotations

import argparse
import contextlib
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
with contextlib.redirect_stdout(io.StringIO()):
    import review_disposition_calib as K

CB = "创业板"                      # 该腿闸位在基准倍数上再乘 1.10（历史约定）
PROD = {"沪深300": 1.00, CB: 1.00, "科创50": 1.30, "红利": 1.00}   # 现行生产口径的基准倍数


def gate(nm: str, mu: float) -> float:
    return mu * (1.10 if nm == CB else 1.0)


def run(nm: str, mu: float, d0: str | None = None, d1: str | None = None) -> dict:
    df, ret = K.data[nm]
    return K.run(df, ret, nm, d0 or K.ST[nm], d1 or K.EN[nm], K.MODE[nm], sell_mul=gate(nm, mu))


def mid_date(nm: str) -> str:
    df, _ = K.data[nm]
    v = df.trade_date.values
    i0 = int(np.searchsorted(v, K.ST[nm]))
    i1 = int(np.searchsorted(v, K.EN[nm], side="right"))
    return str(v[(i0 + i1) // 2])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lo", type=float, default=1.00)
    ap.add_argument("--hi", type=float, default=1.60)
    ap.add_argument("--step", type=float, default=0.025)
    a = ap.parse_args()
    grid = [round(a.lo + i * a.step, 4) for i in range(int((a.hi - a.lo) / a.step) + 1)]
    legs = list(K.data)

    print("=" * 108)
    print(f"卖出闸细网格：{a.lo:.2f} ~ {a.hi:.2f} 步长 {a.step}　"
          f"（创业板闸位＝表列倍数 × 1.10，沿用历史约定）")
    print("=" * 108)

    res: dict[str, dict[float, dict]] = {nm: {} for nm in legs}
    for nm in legs:
        for mu in grid:
            res[nm][mu] = run(nm, mu)

    hdr = f"{'闸':>7s}" + "".join(f"{nm:>26s}" for nm in legs)
    print(hdr)
    print(f"{'':>7s}" + "".join(f"{'年化':>7s}{'夏普':>6s}{'回撤':>7s}{'卖笔':>6s}" for _ in legs))
    for mu in grid:
        row = ""
        for nm in legs:
            r = res[nm][mu]
            row += f"{r['ann']:>7.2%}{r['sharpe']:>6.2f}{r['mdd']:>7.1%}{r['ns']:>6d}"
        mark = " ←现行" if abs(mu - 1.30) < 1e-9 else ""
        print(f"{mu:>7.3f}" + row + mark)

    # ── ① 极值点与邻域 ────────────────────────────────────
    print("\n" + "=" * 108)
    print("① 最优点在哪、周围有多平（样本内 argmax，仅用于看形状，不作选参依据）")
    print("=" * 108)
    print(f"{'腿':>8s}{'年化最优闸':>11s}{'该点年化':>10s}{'1.30 年化':>11s}{'差':>8s}"
          f"{'夏普最优闸':>11s}{'该点夏普':>10s}{'1.30 夏普':>11s}{'±0.10 内年化极差':>18s}")
    for nm in legs:
        ann = {mu: res[nm][mu]["ann"] for mu in grid}
        shp = {mu: res[nm][mu]["sharpe"] for mu in grid}
        ba, bs = max(ann, key=ann.get), max(shp, key=shp.get)
        near = [ann[mu] for mu in grid if abs(mu - ba) <= 0.10 + 1e-9]
        print(f"{nm:>8s}{ba:>11.3f}{ann[ba]:>10.2%}{ann[1.30]:>11.2%}{ann[ba]-ann[1.30]:>8.2%}"
              f"{bs:>11.3f}{shp[bs]:>10.2f}{shp[1.30]:>11.2f}{max(near)-min(near):>18.2%}")

    # ── ② 分半 argmax ────────────────────────────────────
    print("\n" + "=" * 108)
    print("② 分半最优闸位——两半差得远＝「最优」是噪声，不是结构")
    print("=" * 108)
    print(f"{'腿':>8s}{'切点':>10s}{'上半最优':>10s}{'下半最优':>10s}{'全窗最优':>10s}{'判定':>26s}")
    for nm in legs:
        mid = mid_date(nm)
        h1 = {mu: run(nm, mu, K.ST[nm], mid)["ann"] for mu in grid}
        h2 = {mu: run(nm, mu, mid, K.EN[nm])["ann"] for mu in grid}
        full = {mu: res[nm][mu]["ann"] for mu in grid}
        b1, b2, bf = max(h1, key=h1.get), max(h2, key=h2.get), max(full, key=full.get)
        gap = abs(b1 - b2)
        verdict = "两半一致（差≤0.10）" if gap <= 0.10 + 1e-9 else f"两半分歧 {gap:.2f} ⟹ 不稳定"
        print(f"{nm:>8s}{mid:>10s}{b1:>10.3f}{b2:>10.3f}{bf:>10.3f}{verdict:>26s}")

    # ── ③ 与「换一条腿」的量级对比 ──────────────────────────
    print("\n" + "=" * 108)
    print("③ 尺度感：调闸位能动多少 vs 腿与腿之间本来差多少")
    print("=" * 108)
    span = {nm: max(res[nm][mu]["ann"] for mu in grid) - min(res[nm][mu]["ann"] for mu in grid)
            for nm in legs}
    at130 = {nm: res[nm][1.30]["ann"] for nm in legs}
    print("  整条网格上的年化极差（闸位能动的最大幅度）：")
    for nm in legs:
        print(f"    {nm:>8s}{span[nm]:>8.2%}")
    print(f"  四条腿在 1.30 上的年化极差（换标的的幅度）：{max(at130.values())-min(at130.values()):>8.2%}")


if __name__ == "__main__":
    main()
