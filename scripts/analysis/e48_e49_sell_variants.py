# -*- coding: utf-8 -*-
"""E48（红利腿只买不卖·股息再投）+ E49（卖出节奏优化）

判据跑数前已写死于 docs/model_change_proposals.md P55/P56 段，本脚本不得改判据。
引擎沿用 long_window_backtest 的口径（周频买 / 月频卖 / exec_lag=1 / 闲钱 2% / 日频回撤），
但把卖出参数化，以便扫描候选。只读 CSV，不落库、不联网。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from long_window_backtest import CASH, FONT, LEGS, RF, RUNG, FRAC, first_tradable, prep


def run_v(df, ret, fmap, nm, d0, d1, mode, *, sell_frac=0.05, sell_mul=None,
          sell_every_month=1, sell_cooldown=0, no_sell=False, init=100.0):
    """参数化卖出的回测。sell_mul=None 用该腿默认（创业板 1.43，其余 1.30）。"""
    d, c = df.trade_date.values, df.c.values
    rr = ret.pct_change().fillna(0).values if ret is not None else None
    i0 = int(np.searchsorted(d, d0))
    i1 = int(np.searchsorted(d, d1, side="right"))
    if i1 - i0 < 250:
        return None
    mul = sell_mul if sell_mul is not None else (1.30 * 1.10 if nm == "创业板" else 1.30)
    cash, units, nav = init, 0.0, 1.0
    last, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, pos, nb, ns = [], [], 0, 0
    last_sell = -999
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
                    cash += sq * nav; units -= sq; ns += 1; last_sell = i
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
        if not no_sell and r.me and r.exp == r.exp and ci > r.exp * mul and units > 0:
            month_idx = int(str(d[i])[:4]) * 12 + int(str(d[i])[4:6])
            ok_freq = (month_idx % sell_every_month) == 0
            ok_cool = (i - last_sell) >= sell_cooldown * 21
            if ok_freq and ok_cool:
                sig.append(("S", sell_frac))
        for k, fr in sig:
            pend.append((k, fr, min(i + 1, i1 - 1)))
        tv = cash + units * nav
        curve.append(tv); pos.append(units * nav / tv)
    v = np.array(curve); pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(d[i1 - 1]) - pd.Timestamp(d[i0])).days / 365.25
    ann = (v[-1] / init) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    mdd = float(((v - pk) / pk).min())
    base = (ret if ret is not None else df.c).values
    bh = (base[i1 - 1] / base[i0]) ** (1 / yrs) - 1
    bhv = base[i0:i1]; bhpk = np.maximum.accumulate(bhv)
    half = i0 + (i1 - i0) // 2
    return dict(ann=ann, vol=vol, sharpe=(ann - RF) / vol, mdd=mdd, yrs=yrs,
                nb=nb, ns=ns, posavg=float(np.mean(pos)), bh=bh,
                bhmdd=float(((bhv - bhpk) / bhpk).min()), half=half,
                curve=v, dates=d[i0:i1])


def halves(df, ret, fmap, nm, d0, d1, mode, **kw):
    """分半：前后两段各自的「策略 − 买持」。"""
    d = df.trade_date.values
    i0 = int(np.searchsorted(d, d0)); i1 = int(np.searchsorted(d, d1, side="right"))
    mid = str(d[i0 + (i1 - i0) // 2])
    a = run_v(df, ret, fmap, nm, d0, mid, mode, **kw)
    b = run_v(df, ret, fmap, nm, mid, d1, mode, **kw)
    return ((a["ann"] - a["bh"]) * 100 if a else np.nan,
            (b["ann"] - b["bh"]) * 100 if b else np.nan)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    a = ap.parse_args()
    root = Path(a.data)
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    data = {nm: prep(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}
    starts = {nm: first_tradable(data[nm][0], mode, fx) for nm, _, _, _, fx, mode in LEGS}
    ends = {nm: str(data[nm][0].trade_date.iloc[-1]) for nm in data}
    modes = {nm: mode for nm, _, _, _, _, mode in LEGS}

    def R(nm, **kw):
        df, ret = data[nm]
        return run_v(df, ret, fmap, nm, starts[nm], ends[nm], modes[nm], **kw)

    # ── E48：红利腿只买不卖 ──────────────────────────────────
    print("=" * 104)
    print("E48 红利腿「只买不卖·股息再投」（判据见 P55，跑数前已写死）")
    print("=" * 104)
    nm = "红利"
    cur, ns_ = R(nm), R(nm, no_sell=True)
    print(f"  {'方案':22s}{'年化':>9s}{'夏普':>8s}{'日频回撤':>10s}{'均仓':>7s}{'买':>5s}{'卖':>5s}")
    print(f"  {'现状（锚买+月卖5%）':20s}{cur['ann']:>9.2%}{cur['sharpe']:>8.2f}"
          f"{cur['mdd']:>10.1%}{cur['posavg']:>7.0%}{cur['nb']:>5d}{cur['ns']:>5d}")
    print(f"  {'只买不卖':20s}{ns_['ann']:>9.2%}{ns_['sharpe']:>8.2f}"
          f"{ns_['mdd']:>10.1%}{ns_['posavg']:>7.0%}{ns_['nb']:>5d}{ns_['ns']:>5d}")
    bhs = (cur["bh"] - RF) / (pd.Series(1.0).iloc[0] or 1)  # placeholder, 用下面显式算
    print(f"  {'买入持有（全收益）':20s}{cur['bh']:>9.2%}{'—':>8s}{cur['bhmdd']:>10.1%}"
          f"{'100%':>7s}{'1':>5s}{'0':>5s}")

    h_cur = halves(data[nm][0], data[nm][1], fmap, nm, starts[nm], ends[nm], modes[nm])
    h_ns = halves(data[nm][0], data[nm][1], fmap, nm, starts[nm], ends[nm], modes[nm], no_sell=True)
    print(f"\n  分半「策略−买持」(pp)：现状 前{h_cur[0]:+.2f} / 后{h_cur[1]:+.2f}"
          f"｜只买不卖 前{h_ns[0]:+.2f} / 后{h_ns[1]:+.2f}")

    print("\n  判据逐条：")
    c1 = (ns_["ann"] >= cur["ann"]) and (ns_["ann"] >= cur["bh"] - 0.005)
    c2 = ns_["sharpe"] >= cur["sharpe"] * 0.90
    c3 = np.sign(h_ns[0]) == np.sign(h_ns[1])
    # 【符号修正 2026-08-04】预登记文字是「日频最大回撤 ≤ 买入持有的 mdd − 3pp」，
    # 括注写明用意＝"若回撤与买持齐平＝规则没起任何作用"⟹ 要求策略回撤**比买持小**至少 3pp。
    # 首次实现写成 `mdd <= bhmdd - 0.03`，在 mdd 为负数时方向相反（等于要求策略跌得更狠）。
    # 这是实现 bug 不是改判据；按文字本意修正为比较绝对值。**修正后总分 1/4 → 2/4，裁决不变。**
    c4 = abs(ns_["mdd"]) <= abs(cur["bhmdd"]) - 0.03
    print(f"    ①收益   年化 {ns_['ann']:.2%} ≥ 现状 {cur['ann']:.2%} 且 ≥ 买持−0.5pp "
          f"({cur['bh'] - 0.005:.2%})  → {'✅过' if c1 else '❌未过'}")
    print(f"    ②风险调整 夏普 {ns_['sharpe']:.2f} ≥ 现状×0.90 ({cur['sharpe'] * 0.9:.2f})"
          f"  → {'✅过' if c2 else '❌未过'}   ← E31 的死因，不得放宽")
    print(f"    ③稳定性  分半符号 {np.sign(h_ns[0]):+.0f} / {np.sign(h_ns[1]):+.0f}"
          f"  → {'✅过' if c3 else '❌未过'}")
    print(f"    ④风控仍在 回撤 |{ns_['mdd']:.1%}| ≤ |买持mdd|−3pp ({abs(cur['bhmdd']) - 0.03:.1%})"
          f"  → {'✅过' if c4 else '❌未过'}   ← 符号修正后，见代码注释")
    n_pass = sum([c1, c2, c3, c4])
    print(f"    ⟹ {n_pass}/4 过（判据要求 ≥3）：{'**通过**' if n_pass >= 3 else '**未通过**'}")

    print(f"\n  【owner 用法的诚实对照】把「只买不卖」和「直接买入持有」摆在一起看：")
    print(f"    直接买入持有   年化 {cur['bh']:.2%}   日频回撤 {cur['bhmdd']:.1%}")
    print(f"    只买不卖      年化 {ns_['ann']:.2%}   日频回撤 {ns_['mdd']:.1%}")
    print(f"    ⟹ 买入规则用 {(cur['bh'] - ns_['ann']) * 100:.2f}pp/年 换掉 "
          f"{(abs(cur['bhmdd']) - abs(ns_['mdd'])) * 100:.1f}pp 的回撤；"
          f"均仓 {ns_['posavg']:.0%} 而非 100%")

    # 同一修法在其余三腿上的表现（防"只对红利好"）
    print("\n  同一修法用在其余三腿（对照，防单腿过拟合）：")
    print(f"  {'腿':8s}{'现状年化':>10s}{'只买不卖':>10s}{'Δ年化':>9s}"
          f"{'现状夏普':>10s}{'只买不卖':>10s}{'Δ夏普':>9s}{'Δ回撤':>9s}")
    for n2 in ("沪深300", "创业板", "科创50"):
        a_, b_ = R(n2), R(n2, no_sell=True)
        print(f"  {n2:8s}{a_['ann']:>10.2%}{b_['ann']:>10.2%}{(b_['ann'] - a_['ann']) * 100:>+9.2f}"
              f"{a_['sharpe']:>10.2f}{b_['sharpe']:>10.2f}{b_['sharpe'] - a_['sharpe']:>+9.2f}"
              f"{(b_['mdd'] - a_['mdd']) * 100:>+9.1f}")

    # ── E49：卖出节奏优化 ────────────────────────────────────
    print("\n" + "=" * 104)
    print("E49 卖出节奏优化（判据见 P56：笔数↓≥40% / 年化≥现状−0.30pp / 夏普≥现状−0.03 / 回撤≤现状+3pp）")
    print("=" * 104)
    CAND = [
        ("C1a 单笔 10%", dict(sell_frac=0.10)),
        ("C1b 单笔 15%", dict(sell_frac=0.15)),
        ("C2a 季频卖 5%", dict(sell_every_month=3)),
        ("C2b 季频卖 15%", dict(sell_every_month=3, sell_frac=0.15)),
        ("C3a 闸 1.40", dict(sell_mul=1.40)),
        ("C3b 闸 1.50", dict(sell_mul=1.50)),
        ("C4  冷却 3 月", dict(sell_cooldown=3)),
    ]
    base = {n: R(n) for n in data}
    tot_base = sum(base[n]["ns"] for n in base)
    print(f"  基线：四腿卖出共 {tot_base} 笔\n")
    print(f"  {'候选':16s}{'卖笔数':>8s}{'降幅':>8s}"
          + "".join(f"{n[:4]:>26s}" for n in data))
    print(f"  {'':16s}{'':>8s}{'':>8s}"
          + "".join(f"{'Δ年化':>9s}{'Δ夏普':>8s}{'Δ回撤':>9s}" for _ in data))
    verdicts = []
    for label, kw in CAND:
        rs = {n: R(n, **kw) for n in data}
        tot = sum(rs[n]["ns"] for n in rs)
        drop = 1 - tot / tot_base
        cells = ""
        ok_all = True
        for n in data:
            da = (rs[n]["ann"] - base[n]["ann"]) * 100
            ds = rs[n]["sharpe"] - base[n]["sharpe"]
            dd = (rs[n]["mdd"] - base[n]["mdd"]) * 100
            cells += f"{da:>+9.2f}{ds:>+8.2f}{dd:>+9.1f}"
            if not (da >= -0.30 and ds >= -0.03 and dd <= 3.0):
                ok_all = False
        ok = ok_all and drop >= 0.40
        verdicts.append((label, tot, drop, ok_all, drop >= 0.40, ok))
        print(f"  {label:16s}{tot:>8d}{drop:>7.0%}" + cells)

    print("\n  逐条判定（四条须全过，且必须四腿同时成立）：")
    for label, tot, drop, ok_all, ok_drop, ok in verdicts:
        print(f"    {label:16s} 笔数降 {drop:>4.0%} {'✅' if ok_drop else '❌'}"
              f"｜四腿收益/夏普/回撤三项 {'✅全过' if ok_all else '❌有腿未过'}"
              f"  ⟹ {'**通过**' if ok else '未通过'}")
    print("\n  ⚠ 全部候选一律并报（P56 明令）——不得事后从中挑最好的那个当结论。")


if __name__ == "__main__":
    main()
