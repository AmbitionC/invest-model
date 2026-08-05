# -*- coding: utf-8 -*-
"""V3 独立裁决路线：从零重写的宽基四腿回测引擎 + E52/E53/E54 判据裁定。

本文件**不 import 任何 scripts/analysis 下的既有模块**，全部规则按
scratchpad/verdict/SPEC.md 自行实现，用于给主线数字提供一条独立复算路线。

只读 results/ 下的 CSV，不落库、不联网、不改生产代码。

用法：
    python scripts/analysis/v3_independent_verdict.py --task all
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2] / "results"
OUT = Path("/tmp/claude-0/-home-user/68136fd0-bc98-58fc-9bbd-6f22b3f9a86b/scratchpad/verdict")

RF = 0.02          # 夏普无风险利率
CASH_RATE = 0.02   # 闲置现金年化（按自然日计息）
POT = 100.0        # 每腿一笔钱
RUNGS = (0.50, 0.55, 0.60, 0.65)

# 各腿：名称 →（信号文件, 信号列, 全收益文件或 None, 模式）
LEG_SRC = {
    "沪深300": ("index_dump_000300_SH.csv", "close", None, "anchor"),
    "创业板": ("spread_full_history.csv", "chinext", None, "anchor"),
    "科创50": ("index_dump_000688_SH.csv", "close", None, "ladder"),
    "红利": ("index_dump_000922_CSI.csv", "close", "index_dump_H00922_CSI.csv", "anchor"),
}
LEGS = list(LEG_SRC)

# 买入锚折价 M / 卖出闸 S（生产口径；创业板单独一档）
BUY_M = {"创业板": 0.90}
SELL_S_PROD = {"创业板": 1.10}
SELL_S_BT = {"创业板": 1.43}


# --------------------------------------------------------------------------- 数据

@dataclass
class LegData:
    name: str
    mode: str
    dates: np.ndarray          # YYYYMMDD 字符串
    px: np.ndarray             # 信号价（红利＝价格指数）
    navret: np.ndarray | None  # 计价日收益（红利＝全收益指数 pct_change），None 表示同 px
    expmed: np.ndarray         # expanding 中位数（未做 WARM 截断）
    r1250: np.ndarray
    peak: np.ndarray
    we: np.ndarray             # 周内最后一个交易日
    me: np.ndarray             # 月内最后一个交易日
    fear: np.ndarray           # 对齐后的恐慌值，缺失 = NaN
    dayfrac: np.ndarray        # 相对前一交易日的自然日/365.25


def _expanding_median(x: np.ndarray) -> np.ndarray:
    """expanding 中位数。用二分插入维护有序数组，O(n^2) 但 n≈5000 可接受。"""
    out = np.empty(len(x))
    buf: list[float] = []
    for i, v in enumerate(x):
        lo, hi = 0, len(buf)
        while lo < hi:
            mid = (lo + hi) // 2
            if buf[mid] < v:
                lo = mid + 1
            else:
                hi = mid
        buf.insert(lo, float(v))
        n = len(buf)
        out[i] = buf[n // 2] if n % 2 else 0.5 * (buf[n // 2 - 1] + buf[n // 2])
    return out


def _rolling_median(x: np.ndarray, w: int) -> np.ndarray:
    s = pd.Series(x).rolling(w).median()
    return s.to_numpy()


def load_legs() -> dict[str, LegData]:
    fear_df = pd.read_csv(ROOT / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear_df.trade_date, pd.to_numeric(fear_df.score)))

    legs: dict[str, LegData] = {}
    for name, (fn, col, trf, mode) in LEG_SRC.items():
        d = pd.read_csv(ROOT / fn, dtype={"trade_date": str})
        d = d.sort_values("trade_date").reset_index(drop=True)
        d["px"] = pd.to_numeric(d[col])
        navret = None
        if trf:
            tr = pd.read_csv(ROOT / trf, dtype={"trade_date": str})
            tr["tr"] = pd.to_numeric(tr["close"])
            d = d.merge(tr[["trade_date", "tr"]], on="trade_date", how="inner")
            navret = d["tr"].pct_change().fillna(0.0).to_numpy()
        dates = d.trade_date.to_numpy()
        px = d.px.to_numpy(dtype=float)
        ts = pd.to_datetime(dates)
        iso = ts.isocalendar()
        wkey = (iso.week.astype(str) + "|" + iso.year.astype(str)).to_numpy()
        mkey = np.array([s[:6] for s in dates])
        we = np.append(wkey[:-1] != wkey[1:], True)
        me = np.append(mkey[:-1] != mkey[1:], True)
        dayfrac = np.zeros(len(px))
        dd = (ts[1:] - ts[:-1]).days.to_numpy() / 365.25
        dayfrac[1:] = dd
        legs[name] = LegData(
            name=name, mode=mode, dates=dates, px=px, navret=navret,
            expmed=_expanding_median(px), r1250=_rolling_median(px, 1250),
            peak=np.maximum.accumulate(px), we=we, me=me,
            fear=np.array([fmap.get(s, np.nan) for s in dates]), dayfrac=dayfrac,
        )
    return legs


# --------------------------------------------------------------------------- 配置

@dataclass
class Cfg:
    # 卖出
    sell_mode: str = "monthly"       # monthly | grid | none
    grid_g: float = 0.05
    sell_prod: bool = True           # True=生产闸 ×1.00/1.10；False=回测闸 ×1.30/1.43
    # 买入
    size: str = "cur"                # cur | init | ramp
    cooldown: int = 20               # B2 冷却期（交易日）
    ladder_off: bool = False         # 科创50 阶梯腿去掉、改走 B1
    legs_on: tuple = ("B1", "B2", "B3")
    # 底仓
    base: float = 0.0
    base_lag: int = 1                # 底仓买入的执行滞后（1=与其它信号一致；0=主线口径）
    warm: int = 500


def sell_mul(name: str, cfg: Cfg) -> float:
    if cfg.sell_prod:
        return SELL_S_PROD.get(name, 1.00)
    return SELL_S_BT.get(name, 1.30)


def first_tradable_idx(leg: LegData, cfg: Cfg) -> int:
    """策略第一个可交易日下标。ladder 腿＝数据首日；anchor 腿＝锚预热完成日。"""
    mode = "anchor" if (cfg.ladder_off and leg.mode == "ladder") else leg.mode
    if mode == "ladder":
        return 0
    return cfg.warm if cfg.warm < len(leg.px) else len(leg.px) - 1


# --------------------------------------------------------------------------- 引擎

def simulate(leg: LegData, cfg: Cfg, i0: int | None = None, i1: int | None = None) -> dict:
    """核心回测。返回全套指标 + 明细。"""
    n = len(leg.px)
    if i0 is None:
        i0 = first_tradable_idx(leg, cfg)
    if i1 is None:
        i1 = n
    if i1 - i0 < 60:
        return {}
    mode = "anchor" if (cfg.ladder_off and leg.mode == "ladder") else leg.mode
    px, expm, r1250, peak = leg.px, leg.expmed, leg.r1250, leg.peak
    we, me, fear, dayfrac = leg.we, leg.me, leg.fear, leg.dayfrac
    warm, mul = cfg.warm, sell_mul(leg.name, cfg)
    bm = BUY_M.get(leg.name, 1.00)
    use_b1 = "B1" in cfg.legs_on and mode == "anchor"
    use_b2 = "B2" in cfg.legs_on
    use_b3 = "B3" in cfg.legs_on and mode == "ladder"

    ladder_frac = {"cur": (.30, .35, .40, .50),
                   "init": (.25, .25, .25, .25),
                   "ramp": (.15, .20, .25, .40)}[cfg.size]

    cash = POT * (1.0 - cfg.base)
    cash0 = cash
    base_cash, base_units = POT * cfg.base, 0.0
    units, nav = 0.0, (1.0 if leg.navret is not None else float(px[i0]))
    last_panic = -10**9
    armed = [True] * 4
    in_ep = False
    nxt = None
    pend: list[tuple[str, float, int]] = []
    pend_base: list[int] = []
    curve, posl, buys = [], [], []
    nb = ns = 0
    b_by_leg = {"B1": 0, "B2": 0, "B3": 0}
    amt_by_leg = {"B1": 0.0, "B2": 0.0, "B3": 0.0}

    for i in range(i0, i1):
        ci = float(px[i])
        if i > i0:
            g = (1.0 + CASH_RATE) ** dayfrac[i]
            cash *= g
            base_cash *= g
            nav = nav * (1.0 + leg.navret[i]) if leg.navret is not None else ci
        # --- 执行到期委托（次日收盘）
        if pend:
            keep = []
            for kind, amt, due in pend:
                if due != i:
                    keep.append((kind, amt, due))
                    continue
                if kind[0] == "B":
                    a = min(amt, cash)
                    if a > 0.05:
                        units += a / nav
                        cash -= a
                        nb += 1
                        buys.append((ci, a, kind[2:]))
                        b_by_leg[kind[2:]] += 1
                        amt_by_leg[kind[2:]] += a
                else:
                    s = units * amt
                    if s > 0:
                        cash += s * nav
                        units -= s
                        ns += 1
            pend = keep
        if pend_base:
            keep_b = []
            for due in pend_base:
                if due <= i and base_cash > 0.05:
                    base_units += base_cash / nav
                    base_cash = 0.0
                elif due > i:
                    keep_b.append(due)
            pend_base = keep_b

        sig: list[tuple[str, float]] = []
        fired = False
        ex = expm[i] if i >= warm else np.nan
        f = fear[i]

        # --- B2 恐慌抢买（四腿共用）
        if use_b2:
            rr = r1250[i]
            panic = (f == f) and f >= 75 and (i - last_panic) > cfg.cooldown \
                and (rr == rr) and ci < rr
            if panic:
                sig.append(("B|B2", cash * 0.50 if cfg.size == "cur" else cash0 * 0.50))
                fired = True
        if (f == f) and f >= 75:
            last_panic = i

        # --- B3 深回撤阶梯（仅 ladder 腿）
        if use_b3:
            dd = ci / peak[i] - 1.0
            if dd <= -RUNGS[0]:
                if not in_ep:
                    in_ep = True
                    armed = [True] * 4
                j = 0
                for k, th in enumerate(RUNGS):
                    if dd <= -th:
                        j = k
                if armed[j] and we[i]:
                    armed[j] = False
                    base_amt = cash if cfg.size == "cur" else cash0
                    sig.append(("B|B3", base_amt * ladder_frac[j]))
                    fired = True
            elif in_ep and dd >= -RUNGS[0] * 0.5:
                in_ep = False
                armed = [True] * 4

        # --- B1 锚买（anchor 腿，周频）
        if use_b1 and we[i] and ex == ex and ci < ex * bm:
            if cfg.size == "cur":
                amt = cash * 0.20
            elif cfg.size == "init":
                amt = cash0 * 0.20
            else:
                deep = max(0.0, 1.0 - ci / ex)
                amt = cash0 * min(0.50, 0.15 + 2.0 * deep)
            sig.append(("B|B1", amt))
            fired = True

        # --- 底仓：首个买点一次性买入、永不卖出
        if base_cash > 0.05 and fired and not pend_base:
            if cfg.base_lag <= 0:
                base_units += base_cash / nav
                base_cash = 0.0
            else:
                pend_base.append(min(i + cfg.base_lag, i1 - 1))

        # --- 卖腿
        if cfg.sell_mode != "none" and ex == ex and units > 0:
            lvl = ex * mul
            if cfg.sell_mode == "monthly":
                if me[i] and ci > lvl:
                    sig.append(("S", 0.05))
            else:
                if nxt is None and ci > lvl:
                    nxt = lvl
                if nxt is not None and ci >= nxt:
                    sig.append(("S", 0.05))
                    nxt *= (1.0 + cfg.grid_g)

        for kind, amt in sig:
            pend.append((kind, amt, min(i + 1, i1 - 1)))

        tv = cash + base_cash + (units + base_units) * nav
        curve.append(tv)
        posl.append((units + base_units) * nav / tv)

    v = np.asarray(curve)
    pk = np.maximum.accumulate(v)
    d0, d1 = leg.dates[i0], leg.dates[i1 - 1]
    yrs = (pd.Timestamp(d1) - pd.Timestamp(d0)).days / 365.25
    ann = (v[-1] / POT) ** (1.0 / yrs) - 1.0
    vol = float(pd.Series(v).pct_change().dropna().std() * math.sqrt(250))
    ser = pd.Series(v, index=pd.to_datetime(leg.dates[i0:i1]))
    yr = ser.resample("YE").last()
    yr = pd.concat([pd.Series([ser.iloc[0]], index=[ser.index[0]]), yr]).pct_change().dropna()

    win_lo = float(np.min(px[i0:i1]))
    win_mean = float(np.mean(px[i0:i1]))
    if buys:
        w = sum(a for _, a, _ in buys)
        vwap_rel = sum(p * a for p, a, _ in buys) / w / win_mean
        buy_lo = min(p for p, _, _ in buys)
        deep_buymin = sum(a for p, a, _ in buys if p <= buy_lo * 1.05) / w
        deep_winlo = sum(a for p, a, _ in buys if p <= win_lo * 1.05) / w
    else:
        vwap_rel = deep_buymin = deep_winlo = float("nan")

    return dict(
        ann=float(ann), sharpe=float((ann - RF) / vol) if vol > 0 else float("nan"),
        mdd=float(((v - pk) / pk).min()), pos=float(np.mean(posl)),
        nb=nb, ns=ns, nyr=int(len(yr)), nloss=int((yr < 0).sum()),
        worst=float(yr.min()) if len(yr) else float("nan"),
        vwap_rel=float(vwap_rel), deep_buymin=float(deep_buymin), deep_winlo=float(deep_winlo),
        d0=str(d0), d1=str(d1), yrs=float(yrs), nav_end=float(v[-1]),
        b_by_leg=dict(b_by_leg), amt_by_leg={k: float(x) for k, x in amt_by_leg.items()},
    )


def buy_hold(leg: LegData, cfg: Cfg, i0: int | None = None, i1: int | None = None) -> dict:
    n = len(leg.px)
    i0 = first_tradable_idx(leg, cfg) if i0 is None else i0
    i1 = n if i1 is None else i1
    if leg.navret is not None:
        v = np.cumprod(1.0 + leg.navret[i0:i1])
        v = v / v[0]
    else:
        v = leg.px[i0:i1] / leg.px[i0]
    pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(leg.dates[i1 - 1]) - pd.Timestamp(leg.dates[i0])).days / 365.25
    vol = float(pd.Series(v).pct_change().dropna().std() * math.sqrt(250))
    ann = v[-1] ** (1.0 / yrs) - 1.0
    ser = pd.Series(v, index=pd.to_datetime(leg.dates[i0:i1]))
    yr = ser.resample("YE").last()
    yr = pd.concat([pd.Series([ser.iloc[0]], index=[ser.index[0]]), yr]).pct_change().dropna()
    return dict(ann=float(ann), sharpe=float((ann - RF) / vol), mdd=float(((v - pk) / pk).min()),
                pos=1.0, nb=0, ns=0, nyr=int(len(yr)), nloss=int((yr < 0).sum()),
                worst=float(yr.min()))


# --------------------------------------------------------------------------- 稳健性

def halves(leg: LegData, cfg: Cfg) -> list[tuple[str, int, int]]:
    """按可交易区间中点切两半（按交易日计数取中点）。"""
    i0 = first_tradable_idx(leg, cfg)
    i1 = len(leg.px)
    mid = i0 + (i1 - i0) // 2
    return [("上半", i0, mid), ("下半", mid, i1)]


def month_starts(leg: LegData, cfg: Cfg, min_years: float = 5.0) -> list[int]:
    """每腿所有可能的月初起点（剩余样本 ≥ min_years）。"""
    i0 = first_tradable_idx(leg, cfg)
    dates = leg.dates
    end = pd.Timestamp(dates[-1])
    out = []
    prev_m = None
    for i in range(i0, len(dates)):
        m = dates[i][:6]
        if m != prev_m:
            prev_m = m
            if (end - pd.Timestamp(dates[i])).days / 365.25 >= min_years:
                out.append(i)
    return out


def sign(x: float, eps: float = 1e-12) -> int:
    return 0 if abs(x) < eps else (1 if x > 0 else -1)


# --------------------------------------------------------------------------- 任务

def fmt_pct(x, d=2):
    return "—" if x != x else f"{x*100:.{d}f}%"


def block_metrics(r: dict) -> str:
    return (f"{fmt_pct(r['ann'])} / {r['sharpe']:.2f} / {fmt_pct(r['mdd'],1)} / "
            f"{fmt_pct(r['pos'],0)} / {r['nb']}买{r['ns']}卖 / {r['nloss']}/{r['nyr']}")


def task_recon(legs, res):
    """对账主线 §2.2/2.3/2.4：复刻主线口径（卖出闸 ×1.30、底仓 lag=0、deep=买价最低档）。"""
    out = {"B": {}, "C": {}, "D": {}}
    base_kw = dict(sell_prod=False, base_lag=0)
    for size in ("cur", "init", "ramp"):
        out["B"][size] = {nm: simulate(legs[nm], Cfg(size=size, **base_kw)) for nm in LEGS}
    for b in (0.0, .25, .50, .75, 1.0):
        out["C"][f"{b:.2f}"] = {nm: simulate(legs[nm], Cfg(base=b, **base_kw)) for nm in LEGS}
    out["C"]["bh"] = {nm: buy_hold(legs[nm], Cfg(**base_kw)) for nm in LEGS}
    out["D"]["monthly"] = {nm: simulate(legs[nm], Cfg(**base_kw)) for nm in LEGS}
    out["D"]["none"] = {nm: simulate(legs[nm], Cfg(sell_mode="none", **base_kw)) for nm in LEGS}
    for g in (.02, .035, .05, .08):
        out["D"][f"grid{g}"] = {nm: simulate(legs[nm], Cfg(sell_mode="grid", grid_g=g, **base_kw))
                                for nm in LEGS}
    # 命题A（卖出闸）也顺带复算，用于确认口径一致
    out["A"] = {}
    for lab, kw in (("none", dict(sell_mode="none")), ("1.00", dict(sell_prod=True)),
                    ("1.30", dict(sell_prod=False))):
        out["A"][lab] = {nm: simulate(legs[nm], Cfg(base_lag=0, **kw)) for nm in LEGS}
    res["recon"] = out


def robustness(legs, mk_cfg, base_cfg_kw, arms, res_key, res, metric="ann"):
    """对每个 arm 跑：全样本 / 分半 / WARM 四档 / 月度滚动起点同号比例。"""
    out = {}
    for arm_lab, arm_kw in arms:
        rec = {"full": {}, "halves": {}, "warm": {}, "starts": {}}
        for nm in LEGS:
            leg = legs[nm]
            cfg = mk_cfg(**{**base_cfg_kw, **arm_kw})
            cfg0 = mk_cfg(**base_cfg_kw)
            rec["full"][nm] = simulate(leg, cfg)
            # 分半
            hh = {}
            for lab, a, b in halves(leg, cfg0):
                ra = simulate(leg, cfg, a, b)
                rb = simulate(leg, cfg0, a, b)
                hh[lab] = {"arm": ra, "base": rb,
                           "d0": leg.dates[a], "d1": leg.dates[b - 1],
                           "fear_days": int(np.sum(~np.isnan(leg.fear[a:b]))),
                           "fear75": int(np.nansum(leg.fear[a:b] >= 75))}
            rec["halves"][nm] = hh
            # WARM 四档
            ww = {}
            for w in (350, 500, 650, 800):
                ca = mk_cfg(**{**base_cfg_kw, **arm_kw, "warm": w})
                cb = mk_cfg(**{**base_cfg_kw, "warm": w})
                ww[w] = {"arm": simulate(leg, ca), "base": simulate(leg, cb)}
            rec["warm"][nm] = ww
            # 月度滚动起点
            sts = month_starts(leg, cfg0)
            diffs = []
            for si in sts:
                ra = simulate(leg, cfg, si, len(leg.px))
                rb = simulate(leg, cfg0, si, len(leg.px))
                if ra and rb:
                    diffs.append(ra[metric] - rb[metric])
            full_d = rec["full"][nm][metric] - simulate(leg, cfg0)[metric]
            s0 = sign(full_d)
            same = sum(1 for x in diffs if sign(x) == s0)
            rec["starts"][nm] = {"n": len(diffs), "same": same,
                                 "ratio": same / len(diffs) if diffs else float("nan"),
                                 "pos": sum(1 for x in diffs if x > 0),
                                 "mean": float(np.mean(diffs)) if diffs else float("nan"),
                                 "full_diff": full_d}
        out[arm_lab] = rec
    res[res_key] = out


def task_B(legs, res):
    arms = [("cur", {}), ("init", {"size": "init"}), ("ramp", {"size": "ramp"}),
            ("ladder_off", {"ladder_off": True}),
            ("cd0", {"cooldown": 0}), ("cd40", {"cooldown": 40})]
    robustness(legs, Cfg, dict(sell_prod=True), arms, "B", res)
    # 多触发器竞争口径：独立口径＝每条买腿单独一笔钱
    comp = {}
    for arm_lab, arm_kw in arms:
        comp[arm_lab] = {}
        for nm in LEGS:
            leg = legs[nm]
            solo = {}
            for lg in ("B1", "B2", "B3"):
                cfg = Cfg(sell_prod=True, legs_on=(lg,), **arm_kw)
                mode = "anchor" if (cfg.ladder_off and leg.mode == "ladder") else leg.mode
                if (lg == "B1" and mode != "anchor") or (lg == "B3" and mode != "ladder"):
                    continue
                solo[lg] = simulate(leg, cfg)
            comp[arm_lab][nm] = solo
    res["B_solo"] = comp
    # ×1.30 对照
    res["B_bt"] = {lab: {nm: simulate(legs[nm], Cfg(sell_prod=False, **kw)) for nm in LEGS}
                   for lab, kw in arms}


def task_C(legs, res):
    arms = [(f"{b:.2f}", {"base": b}) for b in (0.0, .25, .50, .75, 1.0)]
    robustness(legs, Cfg, dict(sell_prod=True), arms, "C", res, metric="ann")
    # 亏损年数也要在稳健性维度上看，单独补
    res["C_bh"] = {nm: buy_hold(legs[nm], Cfg(sell_prod=True)) for nm in LEGS}
    res["C_bt"] = {lab: {nm: simulate(legs[nm], Cfg(sell_prod=False, **kw)) for nm in LEGS}
                   for lab, kw in arms}


def task_D(legs, res):
    arms = [("monthly", {}),
            ("g2", {"sell_mode": "grid", "grid_g": .02}),
            ("g3.5", {"sell_mode": "grid", "grid_g": .035}),
            ("g5", {"sell_mode": "grid", "grid_g": .05}),
            ("g8", {"sell_mode": "grid", "grid_g": .08})]
    robustness(legs, Cfg, dict(sell_prod=True), arms, "D", res)
    res["D_bt"] = {lab: {nm: simulate(legs[nm], Cfg(sell_prod=False, **kw)) for nm in LEGS}
                   for lab, kw in arms}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", default="all")
    a = ap.parse_args()
    legs = load_legs()
    res: dict = {}
    if a.task in ("all", "recon"):
        task_recon(legs, res)
    if a.task in ("all", "B"):
        task_B(legs, res)
    if a.task in ("all", "C"):
        task_C(legs, res)
    if a.task in ("all", "D"):
        task_D(legs, res)
    OUT.mkdir(parents=True, exist_ok=True)
    p = OUT / f"v3_raw_{a.task}.json"
    p.write_text(json.dumps(res, ensure_ascii=False, default=float), encoding="utf-8")
    print(f"written {p}")
    # 起止对账
    for nm in LEGS:
        c = Cfg(sell_prod=True)
        i0 = first_tradable_idx(legs[nm], c)
        print(f"{nm}: {legs[nm].dates[i0]} ~ {legs[nm].dates[-1]}  n={len(legs[nm].px)-i0}")


if __name__ == "__main__":
    main()
