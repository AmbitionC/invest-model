"""V4 最小可用组合口径引擎（按 SPEC.md 逐字实现，仅供 E55 增量检验/E53 统计检验用）。

不是生产引擎、不落库。与另一条路线的逐日回测各自独立实现，用于交叉对账。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v4_common import (load_amount, load_fear, load_legs, max_drawdown,
                       rolling_pct, sharpe, to_dt)

WARM = 500


def week_end_flags(dates: np.ndarray) -> np.ndarray:
    dt = pd.to_datetime([str(d) for d in dates], format="%Y%m%d")
    wk = dt.isocalendar()
    key = wk.year.to_numpy() * 100 + wk.week.to_numpy()
    out = np.zeros(len(dates), bool)
    out[:-1] = key[:-1] != key[1:]
    out[-1] = True
    return out


def month_end_flags(dates: np.ndarray) -> np.ndarray:
    key = (dates // 100).astype(int)
    out = np.zeros(len(dates), bool)
    out[:-1] = key[:-1] != key[1:]
    out[-1] = True
    return out


def run_leg(name, cfg, fear, S_mult=None, base_frac=0.0,
            b4_pct=None, b4_q=None, b4_k=0.20, legs_on=("B1", "B2", "B3"),
            competing=True, warm=WARM):
    """跑一条腿。返回指标 dict。

    base_frac: 底仓 sleeve 比例（首个买点一次性买入、永不卖出）——E53 用。
    b4_pct/b4_q/b4_k: 成交额滚动分位买腿 B4（周频，现金×k）——E55 用。
    legs_on: 启用的买腿子集（增量检验用）。
    competing: True=B1/B2/B3/B4 共享同一笔现金（竞争口径）；
               False=各买腿独立各跑一套 100 再等权合并（独立口径）。
    """
    sig = cfg["sig"]
    px = cfg["px"]
    M = cfg["M"]
    S = cfg["S"] if S_mult is None else (S_mult * (1.10 if M == 0.90 else 1.00) / 1.00)
    if S_mult is not None:
        S = S_mult * 1.10 if M == 0.90 else S_mult

    d_all = sig.index.to_numpy()
    exp_ = sig.expanding(min_periods=warm).median()
    r1250 = sig.rolling(1250, min_periods=1250).median()
    peak = sig.cummax()

    if cfg["kind"] == "ladder":
        start = 0
    else:
        nz = np.where(exp_.notna().to_numpy())[0]
        if len(nz) == 0:
            raise ValueError(name)
        start = nz[0]

    dates = d_all[start:]
    c = sig.to_numpy()[start:]
    p = px.reindex(d_all).ffill().bfill().to_numpy()[start:]
    ex = exp_.to_numpy()[start:]
    r12 = r1250.to_numpy()[start:]
    pk = peak.to_numpy()[start:]
    fr = fear.reindex(dates).to_numpy()
    n = len(dates)
    wk = week_end_flags(dates)
    mo = month_end_flags(dates)
    q4 = (b4_pct.reindex(dates).to_numpy() if b4_pct is not None
          else np.full(n, np.nan))

    days = np.array([(to_dt(d) - to_dt(dates[0])).days for d in dates], float)

    # ---- 信号（在 t 生成，t+1 收盘执行）----
    b1 = np.zeros(n, bool)
    b2 = np.zeros(n, bool)
    b3 = np.zeros((n, 4), bool)
    b4 = np.zeros(n, bool)
    s1 = np.zeros(n, bool)

    last_panic = -10**9
    tiers = [-0.50, -0.55, -0.60, -0.65]
    tier_amt = [0.30, 0.35, 0.40, 0.50]
    tier_used = [False] * 4
    for i in range(n):
        if cfg["kind"] == "anchor" and wk[i] and np.isfinite(ex[i]) and c[i] < ex[i] * M:
            b1[i] = True
        if np.isfinite(fr[i]) and fr[i] >= 75 and (i - last_panic) > 20 \
                and np.isfinite(r12[i]) and c[i] < r12[i]:
            b2[i] = True
            last_panic = i
        elif np.isfinite(fr[i]) and fr[i] >= 75:
            pass
        if cfg["kind"] == "ladder":
            dd = c[i] / pk[i] - 1.0
            if dd >= -0.25:
                tier_used = [False] * 4
            if wk[i]:
                for j, th in enumerate(tiers):
                    if dd <= th and not tier_used[j]:
                        b3[i, j] = True
                        tier_used[j] = True
        if b4_q is not None and wk[i] and np.isfinite(q4[i]) and q4[i] <= b4_q:
            b4[i] = True
        if mo[i] and np.isfinite(ex[i]) and c[i] > ex[i] * S:
            s1[i] = True

    def simulate(use):
        cash, sh = 100.0, 0.0
        base_sh = 0.0
        nav = np.zeros(n)
        pos = np.zeros(n)
        buys, sells = [], []
        first_buy_done = base_frac <= 0
        for i in range(n):
            # t 的信号在 t 执行？不：exec_lag=1 ⟹ 用 i-1 的信号
            j = i - 1
            if j >= 0:
                amt = 0.0
                if base_frac > 0 and not first_buy_done and (
                        b1[j] or b2[j] or b3[j].any()):
                    inv = 100.0 * base_frac
                    base_sh += inv / p[i]
                    cash -= inv
                    first_buy_done = True
                if "B2" in use and b2[j]:
                    amt += cash * 0.50
                if "B1" in use and b1[j]:
                    amt += (cash - amt) * 0.20
                if "B3" in use and b3[j].any():
                    for k in np.where(b3[j])[0]:
                        amt += (cash - amt) * tier_amt[k]
                if "B4" in use and b4[j]:
                    amt += (cash - amt) * b4_k
                if amt > 1e-12:
                    amt = min(amt, cash)
                    sh += amt / p[i]
                    cash -= amt
                    buys.append((dates[i], p[i], amt))
                if s1[j] and sh > 0:
                    q = sh * 0.05
                    sh -= q
                    cash += q * p[i]
                    sells.append((dates[i], p[i], q * p[i]))
            if i > 0:
                cash *= 1.02 ** ((days[i] - days[i - 1]) / 365.0)
            hold = (sh + base_sh) * p[i]
            nav[i] = cash + hold
            pos[i] = hold / nav[i] if nav[i] > 0 else 0.0
        return nav, pos, buys, sells

    if competing:
        nav, pos, buys, sells = simulate(set(legs_on))
    else:
        navs, poss, buys, sells = [], [], [], []
        for L in legs_on:
            nv, ps, bb, ss = simulate({L})
            navs.append(nv)
            poss.append(ps)
            buys += bb
            sells += ss
        nav = np.mean(navs, axis=0)
        pos = np.mean(poss, axis=0)

    yrs = (to_dt(dates[-1]) - to_dt(dates[0])).days / 365.25
    ann = (nav[-1] / nav[0]) ** (1 / yrs) - 1
    mdd = max_drawdown(nav)
    shp = sharpe(nav)
    # 自然年亏损年数
    ydf = pd.DataFrame({"y": dates // 10000, "nav": nav})
    yend = ydf.groupby("y")["nav"].last()
    ystart = ydf.groupby("y")["nav"].first()
    yr_ret = yend / pd.concat([pd.Series([nav[0]], index=[yend.index[0]]),
                               yend.shift(1).dropna()]) - 1
    nloss = int((yr_ret < 0).sum())
    nyr = len(yr_ret)
    # 加权买入均价 / 全期均价, 最低价+5% 档资金占比
    if buys:
        ba = np.array([b[2] for b in buys])
        bp = np.array([b[1] for b in buys])
        wavg = float((bp * ba).sum() / ba.sum()) / float(np.mean(p))
        lo = float(np.min(p)) * 1.05
        low_frac = float(ba[bp <= lo].sum() / ba.sum())
    else:
        wavg, low_frac = np.nan, np.nan
    return dict(leg=name, start=int(dates[0]), end=int(dates[-1]), ann=ann,
                sharpe=shp, mdd=mdd, pos=float(pos.mean()), nbuy=len(buys),
                nsell=len(sells), loss=f"{nloss}/{nyr}", nloss=nloss, nyr=nyr,
                wavg=wavg, low5=low_frac, nav=nav, dates=dates,
                yr_ret=yr_ret)


def run_all(S_mult=None, **kw):
    legs = load_legs()
    fear = load_fear()
    out = []
    for name, cfg in legs.items():
        on = ["B2", "B3"] if cfg["kind"] == "ladder" else ["B1", "B2"]
        on = [x for x in on if x in kw.get("legs_on", on)] or on
        k = dict(kw)
        k.pop("legs_on", None)
        legs_on = kw.get("legs_on")
        if legs_on is not None:
            base = ["B2", "B3"] if cfg["kind"] == "ladder" else ["B1", "B2"]
            on = [x for x in legs_on if x in base + ["B4"]]
        out.append(run_leg(name, cfg, fear, S_mult=S_mult, legs_on=on, **k))
    return out


def show(rows, title=""):
    if title:
        print(f"\n--- {title}")
    print(f"{'腿':<8}{'区间':<20}{'年化':>8}{'夏普':>7}{'回撤':>8}{'均仓':>7}"
          f"{'买':>5}{'卖':>5}{'亏损年':>8}{'均价比':>8}{'最低档':>8}")
    for r in rows:
        print(f"{r['leg']:<8}{r['start']}~{r['end']}{'':<3}{r['ann']*100:>7.2f}%"
              f"{r['sharpe']:>7.2f}{r['mdd']*100:>7.1f}%{r['pos']*100:>6.0f}%"
              f"{r['nbuy']:>5}{r['nsell']:>5}{r['loss']:>8}"
              f"{r['wavg']:>8.3f}{r['low5']*100:>7.1f}%")


if __name__ == "__main__":
    print("=== 引擎自校准：与 §2.1/§2.3 已发表数字对账 ===")
    for sm in [None, 1.30]:
        show(run_all(S_mult=sm), f"卖出闸 ×{'1.00(生产)' if sm is None else sm}")
