# -*- coding: utf-8 -*-
"""E56 —— 乖离率**低尾**极值作为 B2 恐慌抢买腿的追加触发器（P65）。

判据 **2026-08-05 跑数前写死于 `docs/model_change_proposals.md` P65 段**，本脚本逐条执行、
一字不改。要点复述（不是重新定义）：

  ① 增量价值（主判据）：把「bias_w ≤ 全历史后 X 分位」作为 B2 的**追加**触发器接入四腿回测
     （其余规则一字不动），对比现状 B2（仅恐慌≥75）：**≥3 腿年化 +0.50pp 且无一腿回撤恶化 >3pp**。
     X ∈ {2%,5%,10%}，**须 ≥2 档同时满足**。
  ② 不是恐慌的影子：低尾触发日与「恐慌≥75」触发日**重合率 < 60%**（≥60% 直接 FAIL），
     且剔除所有恐慌≥75 的日子后，纯低尾触发日仍满足①的方向（年化提升为正）。
  ③ 样本充分：每腿独立 episode ≥ 8（不重叠 60 交易日），四腿合计 ≥ 40。
  ④ 稳健：分半不变号 · WARM ∈ {350,500,650,800} 不变号 · MA 窗口 {20,60,120} 中 ≥2 档同向。

判据刻意**不挂在「低尾前瞻收益为正」上**——那个点估计在归档核实时已被看过，复述它没有效力
（E51-E55 四个 AND 门集体失效的机制）。举证责任在①增量价值与②不是恐慌的影子上。

先验申明 50/50。两种结果都如实入库。

口径沿用 SOP：一笔钱 100／闲钱 2%／exec_lag=1／日频回撤／卖出 flat5%／红利按全收益指数。
分位一律**因果**（只用当日可得历史 + 500 交易日预热）；收益从**触发日次一交易日收盘**起算
（沿用引擎的 exec_lag=1）。只读 results/*.csv，不落库、不联网。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))
from invest_model.broad_gates import SELL_MUL  # noqa: E402
from long_window_backtest import CASH, LEGS, RF, RUNG, FRAC  # noqa: E402

PCT_WARM = 500          # 因果分位的预热（判据写死）
XS = [0.02, 0.05, 0.10]
WINDOWS = [20, 60, 120]
WARMS = [350, 500, 650, 800]
WARM_MAIN = 500


# ── 数据准备（把 expanding 中位数算一次，WARM 只改遮罩，避免 O(n²) 重算）──────────
def prep_all(root: Path, f: str, col: str, trf: str | None):
    d = pd.read_csv(root / f, dtype={"trade_date": str}).sort_values(
        "trade_date").reset_index(drop=True)
    d["c"] = pd.to_numeric(d[col])
    c = d.c.values
    d["_expfull"] = [np.median(c[: i + 1]) for i in range(len(c))]
    d["r1250"] = d.c.rolling(1250).median()
    d["peak"] = d.c.cummax()
    ym = d.trade_date.str[:6]
    d["me"] = (ym != ym.shift(-1)).values
    wk = pd.to_datetime(d.trade_date).dt.isocalendar()
    w = wk.week.astype(str) + "-" + wk.year.astype(str)
    d["we"] = (w != w.shift(-1)).values
    ret = None
    if trf:
        tr = pd.read_csv(root / trf, dtype={"trade_date": str})
        tr["c"] = pd.to_numeric(tr.close)
        d = d.merge(tr[["trade_date", "c"]], on="trade_date", suffixes=("", "_tr"))
        ret = d.c_tr
    return d, ret


def with_warm(d: pd.DataFrame, warm: int) -> pd.DataFrame:
    e = d.copy()
    e["exp"] = np.where(np.arange(len(e)) >= warm, e["_expfull"], np.nan)
    return e


def first_tradable(df: pd.DataFrame, mode: str) -> str:
    if mode == "ladder":
        return str(df.trade_date.iloc[0])
    idx = df.index[df["exp"].notna()]
    return str(df.trade_date.iloc[int(idx[0])]) if len(idx) else str(df.trade_date.iloc[0])


def bias_and_causal_pct(close: pd.Series, w: int) -> tuple[np.ndarray, np.ndarray]:
    """乖离率 + 其**因果**全历史分位（第 i 天只用 [0..i] 的历史，前 PCT_WARM 天不给值）。"""
    b = (close / close.rolling(w).mean() - 1.0).to_numpy(dtype=float)
    n = len(b)
    pct = np.full(n, np.nan)
    hist: list[float] = []
    for i in range(n):
        if b[i] == b[i]:
            hist.append(b[i])
            if len(hist) >= PCT_WARM:
                arr = np.asarray(hist)
                pct[i] = float((arr <= b[i]).mean())
    return b, pct


# ── 引擎（复制 long_window_backtest.run 并只加一个追加触发器）────────────────────
def run(df, ret, fmap, nm, d0, d1, mode, *, low=None, init=100.0,
        low_arms_cooldown=True, low_only_when_calm=False, no_r1250_for_low=False):
    """`low` = 与 df 等长的 bool 数组：该日乖离率进入低尾。None ⟹ 现状基线。

    low_arms_cooldown：低尾日是否也刷新 B2 的 20 日冷却锚（对照臂，见 §7.5「实现选择」）。
    low_only_when_calm：只在恐慌<75（含无恐慌数据）的日子允许低尾触发＝判据②的"纯低尾"臂。
    """
    d, c = df.trade_date.values, df.c.values
    if d1 is None:
        d1 = str(d[-1])
    rr = ret.pct_change().fillna(0).values if ret is not None else None
    i0 = int(np.searchsorted(d, d0))
    i1 = int(np.searchsorted(d, d1, side="right"))
    if i1 - i0 < 250:
        return None
    cash, units, nav = init, 0.0, 1.0
    last, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, pos, nb, ns, npan, nlow = [], [], 0, 0, 0, 0
    low_days, panic_days = [], []
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            cash *= (1 + CASH) ** ((pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25)
            nav = nav * (1 + rr[i]) if rr is not None else ci
        elif rr is None:
            nav = ci
        r = df.iloc[i]
        for k, fr, _t, _why in [x for x in pend if x[2] == i]:
            if k == "B":
                a = cash * fr
                if a > 0.05:
                    units += a / nav
                    cash -= a
                    nb += 1
            else:
                s = units * fr
                if s > 0:
                    cash += s * nav
                    units -= s
                    ns += 1
        pend = [x for x in pend if x[2] > i]
        sig, f = [], fmap.get(d[i], np.nan)
        hot = f == f and f >= 75
        lo = bool(low[i]) if low is not None else False
        if lo:
            low_days.append(i)
        if hot:
            panic_days.append(i)
        if low_only_when_calm and hot:
            lo = False
        fire = hot or lo
        gate_ok = (r.r1250 == r.r1250 and ci < r.r1250) or (lo and no_r1250_for_low)
        if fire and i - last > 20 and gate_ok:
            sig.append(("B", 0.50, "恐慌抢买" if hot else "低尾抢买"))
            npan += hot
            nlow += (not hot)
        if hot or (lo and low_arms_cooldown):
            last = i
        if mode == "ladder":
            dd = ci / r.peak - 1
            if dd <= -RUNG[0]:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate(RUNG) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False
                    sig.append(("B", FRAC[j], "深回撤阶梯"))
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            sig.append(("B", 0.20, "锚买"))
        if r.me and r.exp == r.exp and ci > r.exp * SELL_MUL[nm] and units > 0:
            sig.append(("S", 0.05, "卖出闸"))
        for k, fr, why in sig:
            pend.append((k, fr, min(i + 1, i1 - 1), why))
        tv = cash + units * nav
        curve.append(tv)
        pos.append(units * nav / tv)
    v = np.array(curve)
    pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(d[i1 - 1]) - pd.Timestamp(d[i0])).days / 365.25
    ann = (v[-1] / init) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    return dict(ann=ann, mdd=float(((v - pk) / pk).min()), yrs=yrs, nb=nb, ns=ns,
                npan=npan, nlow=nlow, sharpe=(ann - RF) / vol if vol else np.nan,
                posavg=float(np.mean(pos)), low_days=low_days, panic_days=panic_days)


def episodes(idx: list[int], gap: int = 60) -> int:
    """不重叠 60 交易日的独立 episode 数。"""
    if not idx:
        return 0
    n, prev = 1, idx[0]
    for i in idx[1:]:
        if i - prev > gap:
            n += 1
            prev = i
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    a = ap.parse_args()
    root = Path(a.data)
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    fear_start = str(fear.trade_date.min())

    raw = {nm: prep_all(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}
    mode = {nm: m for nm, _, _, _, _, m in LEGS}
    names = [nm for nm, *_ in LEGS]

    # 因果乖离率分位：(leg, w) 各算一次
    bias: dict[tuple[str, int], tuple[np.ndarray, np.ndarray]] = {}
    for nm in names:
        for w in WINDOWS:
            bias[(nm, w)] = bias_and_causal_pct(raw[nm][0].c, w)

    def legdf(nm, warm):
        return with_warm(raw[nm][0], warm), raw[nm][1]

    def base(nm, warm=WARM_MAIN, d0=None, d1=None):
        df, ret = legdf(nm, warm)
        return run(df, ret, fmap, nm, d0 or first_tradable(df, mode[nm]), d1, mode[nm])

    def treat(nm, x, w=60, warm=WARM_MAIN, d0=None, d1=None, **kw):
        df, ret = legdf(nm, warm)
        _, pct = bias[(nm, w)]
        low = (pct <= x) & ~np.isnan(pct)
        return run(df, ret, fmap, nm, d0 or first_tradable(df, mode[nm]), d1, mode[nm],
                   low=low, **kw)

    print("=" * 112)
    print("E56 —— 乖离率低尾作为 B2 追加触发器｜判据 2026-08-05 跑数前写死，本脚本逐条执行")
    print(f"恐慌数据起点 {fear_start} ⟹ 之前基线 B2 自然不触发；低尾可触发（窗口口径见判据②后的对照）")
    print("=" * 112)

    # ── 引擎自检：low=None 必须逐位复现主线引擎 ────────────────────────────
    from long_window_backtest import prep as mainprep, run as mainrun, first_tradable as mainft
    print("\n【引擎自检】low=None 时本脚本引擎 vs 主线 long_window_backtest.run")
    ok = True
    for nm, f, col, trf, _fx, m in LEGS:
        mdf, mret = mainprep(root, f, col, trf)
        m0 = mainrun(mdf, mret, fmap, nm, mainft(mdf, m, None), None, m)
        b0 = base(nm)
        d_ann, d_mdd = abs(m0["ann"] - b0["ann"]), abs(m0["mdd"] - b0["mdd"])
        good = d_ann < 1e-12 and d_mdd < 1e-12 and m0["nb"] == b0["nb"] and m0["ns"] == b0["ns"]
        ok &= good
        print(f"  {nm:>7s} 年化 {m0['ann']:.6%} vs {b0['ann']:.6%}  回撤 {m0['mdd']:.6%} vs "
              f"{b0['mdd']:.6%}  买{m0['nb']}/{b0['nb']} 卖{m0['ns']}/{b0['ns']}  "
              f"{'✅一致' if good else '❌不一致'}")
    print(f"  ⟹ {'引擎等价，增量全部来自追加触发器' if ok else '🔴 引擎不等价，结果不可用'}")

    B = {nm: base(nm) for nm in names}

    # ── 判据② 先跑（判据设计已写明：②不过直接 FAIL，不再往下评）───────────────
    # ── 机制诊断（非判据·SOP §7.5「参数完全不影响结果必须先追查」的产物）──────
    # 首跑发现 X=2%/5% 时四腿全部 Δ=+0.00 且低尾买入 0 笔，而低尾日明明有几十上百天。
    # 追查结论见下表：**低尾日与 B2 的既有价格闸几乎不相交**。
    print("\n" + "=" * 112)
    print("机制诊断（非判据）：低尾日中，有多少天同时满足 B2 的既有价格闸「收盘 < 近5年中位数」")
    print("=" * 112)
    print(f"{'腿':>7s}{'X':>7s}{'低尾日':>9s}{'其中<近5年中位数':>18s}{'占比':>9s}{'可触发次数':>12s}")
    diag = {}
    for x in XS:
        for nm in names:
            df, _ = legdf(nm, WARM_MAIN)
            _, pct = bias[(nm, 60)]
            i0 = int(np.searchsorted(df.trade_date.values, first_tradable(df, mode[nm])))
            sel = [i for i in np.where((pct <= x) & ~np.isnan(pct))[0] if i >= i0]
            r12, cl = df.r1250.values, df.c.values
            below = [i for i in sel if r12[i] == r12[i] and cl[i] < r12[i]]
            lastq, nq = -999, 0
            for i in sel:
                if i - lastq > 20 and r12[i] == r12[i] and cl[i] < r12[i]:
                    nq += 1
                lastq = i
            diag[(nm, x)] = (len(sel), len(below), nq)
            print(f"{nm:>7s}{x:>7.0%}{len(sel):>9d}{len(below):>18d}"
                  f"{(len(below)/len(sel) if sel else 0):>9.1%}{nq:>12d}")
    print("  ⟹ 乖离率低尾是**相对 60 日均线的短期偏离**（牛市中的回调也会点亮），")
    print("     而 B2 的既有价格闸是**长期估值位置**（收盘 < 近 5 年中位数）。两者时间尺度不同，")
    print("     AND 之后几乎为空集 ⟹ 追加触发器在生产口径下根本没有发挥空间。")

    print("\n" + "=" * 112)
    print("判据② 不是恐慌的影子：低尾触发日与恐慌≥75 触发日的重合率 < 60%（≥60% 直接 FAIL）")
    print("=" * 112)
    print(f"{'腿':>7s}{'X':>7s}{'低尾日':>8s}{'恐慌日':>8s}{'交集':>7s}{'重合率':>9s}{'判定':>10s}")
    overlap_ok = {}
    for x in XS:
        for nm in names:
            r = treat(nm, x)
            ld, pd_ = set(r["low_days"]), set(r["panic_days"])
            ov = len(ld & pd_) / len(ld) if ld else 1.0
            overlap_ok[(nm, x)] = ov < 0.60
            print(f"{nm:>7s}{x:>7.0%}{len(ld):>8d}{len(pd_):>8d}{len(ld & pd_):>7d}{ov:>9.1%}"
                  f"{'✅<60%' if ov < 0.60 else '❌≥60%':>10s}")
    c2a = all(overlap_ok.values())

    # ── 判据① 增量价值 ───────────────────────────────────────────────
    print("\n" + "=" * 112)
    print("判据① 增量价值：≥3 腿年化 +0.50pp 且无一腿回撤恶化 >3pp；X 三档须 ≥2 档同时满足")
    print("=" * 112)
    c1_pass, c1_detail = {}, {}
    for arm_name, kw in (("主臂·低尾也刷新冷却", dict(low_arms_cooldown=True)),
                         ("对照臂·低尾不刷新冷却", dict(low_arms_cooldown=False))):
        print(f"\n  ── {arm_name} ──")
        print(f"  {'X':>6s}" + "".join(f"{nm:>26s}" for nm in names) + f"{'达标腿':>8s}{'判定':>8s}")
        print(f"  {'':>6s}" + "".join(f"{'Δ年化':>9s}{'Δ回撤':>9s}{'低尾买':>8s}" for _ in names))
        for x in XS:
            row, nleg, worst = "", 0, 0.0
            for nm in names:
                t = treat(nm, x, **kw)
                da = (t["ann"] - B[nm]["ann"]) * 100
                dm = (t["mdd"] - B[nm]["mdd"]) * 100        # 负得更多＝恶化
                row += f"{da:>+9.2f}{dm:>+9.2f}{t['nlow']:>8d}"
                if da >= 0.50:
                    nleg += 1
                worst = min(worst, dm)
                c1_detail[(arm_name, x, nm)] = (da, dm, t["nlow"])
            ok1 = nleg >= 3 and worst >= -3.0
            c1_pass[(arm_name, x)] = ok1
            print(f"  {x:>6.0%}{row}{nleg:>8d}{'✅' if ok1 else '❌':>8s}")
        n_ok = sum(1 for x in XS if c1_pass[(arm_name, x)])
        print(f"  ⟹ {arm_name}：{n_ok}/3 档达标，判据①要求 ≥2 档 ⟹ "
              f"{'✅通过' if n_ok >= 2 else '❌不过'}")
    c1 = sum(1 for x in XS if c1_pass[("主臂·低尾也刷新冷却", x)]) >= 2

    # ── 判据② 后半：纯低尾臂（剔除恐慌日）方向仍为正 ─────────────────────
    print("\n  ② 后半：剔除所有恐慌≥75 的日子后，纯低尾触发是否仍带来年化提升")
    print(f"  {'X':>6s}" + "".join(f"{nm:>12s}" for nm in names) + f"{'为正腿数':>10s}")
    c2b = {}
    for x in XS:
        row, npos = "", 0
        for nm in names:
            t = treat(nm, x, low_only_when_calm=True)
            da = (t["ann"] - B[nm]["ann"]) * 100
            row += f"{da:>+12.2f}"
            npos += da > 0
        c2b[x] = npos >= 3
        print(f"  {x:>6.0%}{row}{npos:>10d}")
    c2 = c2a and any(c2b.values())

    # ── 判据③ 样本充分 ──────────────────────────────────────────────
    print("\n" + "=" * 112)
    print("判据③ 样本充分：每腿独立 episode ≥ 8（不重叠 60 交易日），四腿合计 ≥ 40")
    print("=" * 112)
    print(f"{'X':>6s}" + "".join(f"{nm:>12s}" for nm in names) + f"{'合计':>8s}{'判定':>8s}")
    c3 = {}
    for x in XS:
        eps, row = [], ""
        for nm in names:
            e = episodes(treat(nm, x)["low_days"])
            eps.append(e)
            row += f"{e:>12d}"
        ok3 = all(e >= 8 for e in eps) and sum(eps) >= 40
        c3[x] = ok3
        print(f"{x:>6.0%}{row}{sum(eps):>8d}{'✅' if ok3 else '❌':>8s}")

    # ── 判据④ 稳健 ────────────────────────────────────────────────
    print("\n" + "=" * 112)
    print("判据④ 稳健：分半不变号 · WARM 四档不变号 · MA 窗口三档 ≥2 档同向")
    print("=" * 112)
    XREF = 0.05
    print(f"\n  ④-1 分半（各腿按交易日中点切），X={XREF:.0%}, w=60")
    print(f"  {'腿':>7s}{'切点':>10s}{'上半Δ年化':>12s}{'下半Δ年化':>12s}{'判定':>12s}")
    c41 = 0
    for nm in names:
        df, _ = legdf(nm, WARM_MAIN)
        d0 = first_tradable(df, mode[nm])
        v = df.trade_date.values
        i0 = int(np.searchsorted(v, d0))
        mid = str(v[(i0 + len(v)) // 2])
        h1t, h1b = treat(nm, XREF, d0=d0, d1=mid), base(nm, d0=d0, d1=mid)
        h2t, h2b = treat(nm, XREF, d0=mid), base(nm, d0=mid)
        a1 = (h1t["ann"] - h1b["ann"]) * 100 if h1t and h1b else float("nan")
        a2 = (h2t["ann"] - h2b["ann"]) * 100 if h2t and h2b else float("nan")
        same = (a1 == a1 and a2 == a2 and np.sign(a1) == np.sign(a2))
        c41 += same
        print(f"  {nm:>7s}{mid:>10s}{a1:>+12.2f}{a2:>+12.2f}"
              f"{('✅同号' if same else '❌变号'):>12s}")

    print(f"\n  ④-2 WARM 敏感性（X={XREF:.0%}, w=60）—— 每档都是 treat 减该档自己的 base")
    print(f"  {'WARM':>6s}" + "".join(f"{nm:>12s}" for nm in names) + f"{'为正腿数':>10s}")
    warm_pos = []
    for wm in WARMS:
        row, npos = "", 0
        for nm in names:
            b_, t_ = base(nm, warm=wm), treat(nm, XREF, warm=wm)
            da = (t_["ann"] - b_["ann"]) * 100
            row += f"{da:>+12.2f}"
            npos += da > 0
        warm_pos.append(npos)
        print(f"  {wm:>6d}{row}{npos:>10d}")
    c42 = len(set(np.sign(np.array(warm_pos) - 2))) == 1  # 四档「多数腿为正」的方向一致

    print(f"\n  ④-3 MA 窗口对照臂（X={XREF:.0%}, WARM=500）")
    print(f"  {'w':>6s}" + "".join(f"{nm:>12s}" for nm in names) + f"{'达标腿(≥+0.5pp)':>16s}")
    win_ok = []
    for w in WINDOWS:
        row, nleg = "", 0
        for nm in names:
            t = treat(nm, XREF, w=w)
            da = (t["ann"] - B[nm]["ann"]) * 100
            row += f"{da:>+12.2f}"
            nleg += da >= 0.50
        win_ok.append(nleg >= 3)
        print(f"  {w:>6d}{row}{nleg:>16d}")
    c43 = sum(win_ok) >= 2

    # ── 附：恐慌可用窗口（2015+）的平行读数，披露窗口混淆 ────────────────────
    print("\n" + "=" * 112)
    print(f"附（非判据·披露）：限定在恐慌数据可用窗口（{fear_start} 起）重跑判据①，X=5%")
    print("  理由：低尾在全窗都能触发，而基线 B2 只在 2015 后能触发 ⟹ 全窗对比含窗口混淆。")
    print("=" * 112)
    print(f"  {'腿':>7s}{'Δ年化':>10s}{'Δ回撤':>10s}{'低尾买':>8s}")
    for nm in names:
        b_, t_ = base(nm, d0=fear_start), treat(nm, XREF, d0=fear_start)
        if b_ and t_:
            print(f"  {nm:>7s}{(t_['ann']-b_['ann'])*100:>+10.2f}"
                  f"{(t_['mdd']-b_['mdd'])*100:>+10.2f}{t_['nlow']:>8d}")
        else:
            print(f"  {nm:>7s}{'样本不足':>10s}")

    # ── 判据④ 的空洞守卫（判据设计缺陷，必须标注）──────────────────────────
    # X=5% 参考档在四腿上零触发 ⟹ 分半/WARM 两项检查全部作用在恒等的处理臂上，
    # "不变号"是**平凡成立**、不构成任何证据。这是本轮判据设计的第二处缺陷
    # （第一处是②的聚合口径未写明），记入负结果，不算作通过。
    fired_ref = sum(treat(nm, XREF)["nlow"] for nm in names)
    c4_vacuous = fired_ref == 0
    c4 = (c41 >= 3) and c42 and c43 and not c4_vacuous
    c3ok = any(c3.values())

    # ── 附二：拆掉 B2 既有价格闸的探索（**非判据·不可用于晋升**）──────────────
    # 机制诊断说明失败来自"低尾与 r1250 闸几乎不相交"。自然的下一问是：
    # 去掉 r1250 闸会怎样？**但这个问题的答案不能用来晋升任何东西**——去掉它就等于
    # 改动 B2 引擎本身，那正是「单件移植＝对冲」判定要拦的事。此处只为把机制说清楚。
    print("\n" + "=" * 112)
    print("附二（非判据·不可用于晋升）：若拆掉 B2 的既有价格闸 r1250，仅留「低尾 + 20 日冷却」")
    print("  ⚠️ 这等于改 B2 引擎本身＝单件移植，判定为对冲。此表只用来说明机制，不作证据。")
    print("=" * 112)
    print(f"  {'腿':>7s}{'X':>7s}{'Δ年化':>10s}{'Δ回撤':>10s}{'低尾买':>8s}")
    for x in XS:
        for nm in names:
            df, ret = legdf(nm, WARM_MAIN)
            _, pct = bias[(nm, 60)]
            low = (pct <= x) & ~np.isnan(pct)
            t = run(df, ret, fmap, nm, first_tradable(df, mode[nm]), None, mode[nm],
                    low=low, no_r1250_for_low=True)
            print(f"  {nm:>7s}{x:>7.0%}{(t['ann']-B[nm]['ann'])*100:>+10.2f}"
                  f"{(t['mdd']-B[nm]['mdd'])*100:>+10.2f}{t['nlow']:>8d}")

    print("\n" + "=" * 112)
    print("E56 裁决（按 2026-08-05 写死的判据，逐条）")
    print("=" * 112)
    print(f"  ① 增量价值        {'✅通过' if c1 else '❌不过'}")
    print(f"  ② 不是恐慌的影子  {'✅通过' if c2 else '❌不过'}"
          f"（重合率全<60%：{'是' if c2a else '否'}；纯低尾方向为正：{'是' if any(c2b.values()) else '否'}）")
    print(f"  ③ 样本充分        {'✅通过' if c3ok else '❌不过'}")
    print(f"  ④ 稳健            {'✅通过' if c4 else '❌不过'}"
          f"（分半同号 {c41}/4；WARM 方向一致：{'是' if c42 else '否'}；"
          f"MA 窗口 {sum(win_ok)}/3 达标）")
    if c4_vacuous:
        print("     ⚠️ 判据④ 在 X=5% 参考档**零触发**，分半/WARM 两项是平凡成立、不构成证据。")
        print("        这是本轮判据设计的第二处缺陷（第一处：②的聚合口径未写明），记入负结果。")
    if not c2:
        print("\n  ⟹ **FAIL**：判据②未过。判据设计已写明「②不过直接 FAIL、不再往下评」。")
    elif c1 and c3ok and c4:
        print("\n  ⟹ **PASS**：四条全过 → 走高置信直升评估，接入 B2 作追加触发（提示-only）。")
    elif c1:
        print("\n  ⟹ **①过而③/④未过 → 记知识库、不接入生产**（判据已写死此分支）。")
    else:
        print("\n  ⟹ **FAIL**：判据①未过 → 不接入生产，入库负结果。")


if __name__ == "__main__":
    main()
