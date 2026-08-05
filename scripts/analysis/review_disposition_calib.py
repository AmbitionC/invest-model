# -*- coding: utf-8 -*-
"""三方评审处置·SOP 第一步：命题 A/B/C/D 的三极点可行域标定。

owner 2026-08-05：「按之前约定的路径，给我一个完整的方案，仔细的数据验证，和交叉的数据校准」。
本脚本只跑边界、不做取舍、不下结论——目的是在写死判据前知道「什么是可达的」，
并堵住「把仓位推到 100% 自动达标」这类作弊通道。产出即 docs/model_change_proposals.md §2。

  A 卖出闸倍数（生产 ×1.00 vs 回测 ×1.30 的口径不一致，见 P58）
  B 加码斜率＝买入金额的计算基数（P59/E52）
  C 底仓 sleeve 比例（P60/E53）
  D 卖出触发方式：月频 vs 价格网格棘轮（P61/E54）

口径同 SOP：一笔钱 100／闲钱 2%／exec_lag=1／日频回撤／各腿自动对齐首个可交易日。
只读 results/*.csv，不落库、不联网。
"""
import sys
sys.path.insert(0, "scripts/analysis")
import numpy as np
import pandas as pd
from pathlib import Path
from long_window_backtest import CASH, LEGS, RF, first_tradable, prep

root = Path("results")
SRC = {"沪深300": ("index_dump_000300_SH.csv", "close", None),
       "创业板": ("spread_full_history.csv", "chinext", None),
       "科创50": ("index_dump_000688_SH.csv", "close", None),
       "红利": ("index_dump_000922_CSI.csv", "close", "index_dump_H00922_CSI.csv")}
fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
FMAP = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
MODE = {nm: m for nm, _, _, _, _, m in LEGS}
RUNG = [.50, .55, .60, .65]


def run(df, ret, nm, d0, d1, mode, *, sell_mul=None, no_sell=False,
        size="cur", base=0.0, grid=None):
    """size: cur=当前现金×比例（现状）｜init=起始资金×比例（绝对金额恒定）｜ramp=越深越大
       base: 底仓比例（首个买点一次性买入、永不卖出），其余资金跑现行规则
       grid: 不为 None 时用价格网格卖出（每上涨 grid 卖 5%，日频、只向上棘轮），替代月频卖
    """
    d, c = df.trade_date.values, df.c.values
    rr = ret.pct_change().fillna(0).values if ret is not None else None
    i0, i1 = int(np.searchsorted(d, d0)), int(np.searchsorted(d, d1, side="right"))
    mul = sell_mul if sell_mul is not None else (1.43 if nm == "创业板" else 1.30)
    TOT = 100.0
    cash, units, nav = TOT * (1 - base), 0.0, 1.0
    cash0 = cash
    base_cash, base_units = TOT * base, 0.0
    last, pend = -999, []
    armed, in_ep = np.ones(4, bool), False
    curve, pos, nb, ns, buys = [], [], 0, 0, []
    nxt = None
    FR = {"cur": [.30, .35, .40, .50], "init": [.25, .25, .25, .25],
          "ramp": [.15, .20, .25, .40]}[size]
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            dt = (pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25
            cash *= (1 + CASH) ** dt
            base_cash *= (1 + CASH) ** dt
            nav = nav * (1 + rr[i]) if rr is not None else ci
        elif rr is None:
            nav = ci
        r = df.iloc[i]
        for k_, amt, _t in [x for x in pend if x[2] == i]:
            if k_ == "B":
                a = min(amt, cash)
                if a > 0.05:
                    units += a / nav; cash -= a; nb += 1; buys.append((ci, a))
            else:
                s = units * amt
                if s > 0:
                    cash += s * nav; units -= s; ns += 1
        pend = [x for x in pend if x[2] > i]

        sig, f = [], FMAP.get(d[i], np.nan)
        # 买腿
        panic = f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and ci < r.r1250
        if panic:
            sig.append(("B", cash * 0.50 if size == "cur" else cash0 * 0.50))
        if f == f and f >= 75:
            last = i
        fired = panic
        if mode == "ladder":
            dd = ci / r.peak - 1
            if dd <= -RUNG[0]:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate(RUNG) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False
                    sig.append(("B", cash * FR[j] if size == "cur" else cash0 * FR[j]))
                    fired = True
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            if size == "cur":
                amt = cash * 0.20
            elif size == "init":
                amt = cash0 * 0.20
            else:  # ramp：跌得越深买得越多（相对锚的折价线性放大）
                deep = max(0.0, 1 - ci / r.exp)
                amt = cash0 * min(0.50, 0.15 + 2.0 * deep)
            sig.append(("B", amt)); fired = True
        # 底仓：首个买点一次性买入，永不卖出
        if base_cash > 0.05 and fired:
            base_units += base_cash / nav; base_cash = 0.0
        # 卖腿
        if not no_sell and r.exp == r.exp and units > 0:
            lvl = r.exp * mul
            if grid is None:
                if r.me and ci > lvl:
                    sig.append(("S", 0.05))
            else:
                if nxt is None and ci > lvl:
                    nxt = lvl
                if nxt is not None and ci >= nxt:
                    sig.append(("S", 0.05)); nxt *= (1 + grid)
        for k_, amt in sig:
            pend.append((k_, amt, min(i + 1, i1 - 1)))
        tv = cash + units * nav + base_cash + base_units * nav
        curve.append(tv); pos.append((units + base_units) * nav / tv)
    v = np.array(curve); pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(d[i1 - 1]) - pd.Timestamp(d[i0])).days / 365.25
    ann = (v[-1] / TOT) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    # 自然年收益（他的目标函数：任一年不亏）
    ser = pd.Series(v, index=pd.to_datetime(d[i0:i1]))
    yr = ser.resample("YE").last()
    yr = pd.concat([pd.Series([ser.iloc[0]], index=[ser.index[0]]), yr]).pct_change().dropna()
    if buys:
        w = sum(a for _, a in buys)
        vwap_rel = sum(p * a for p, a in buys) / w / float(np.mean(c[i0:i1]))
        lo = min(p for p, _ in buys)
        deep_share = sum(a for p, a in buys if p <= lo * 1.05) / w
    else:
        vwap_rel = deep_share = np.nan
    return dict(ann=ann, sharpe=(ann - RF) / vol, mdd=float(((v - pk) / pk).min()),
                nb=nb, ns=ns, pos=float(np.mean(pos)), vwap_rel=vwap_rel,
                deep_share=deep_share, nyr=len(yr), nloss=int((yr < 0).sum()),
                worst=float(yr.min()))


data = {nm: prep(root, f, c, t) for nm, (f, c, t) in SRC.items()}
ST = {nm: first_tradable(data[nm][0], MODE[nm], None) for nm in data}
EN = {nm: str(data[nm][0].trade_date.iloc[-1]) for nm in data}


def R(nm, **kw):
    df, ret = data[nm]
    return run(df, ret, nm, ST[nm], EN[nm], MODE[nm], **kw)


def bh(nm):
    df, ret = data[nm]
    s = ret if ret is not None else df.c
    i0 = int(np.searchsorted(df.trade_date.values, ST[nm]))
    s = s.iloc[i0:].ffill().dropna()
    yrs = (pd.Timestamp(EN[nm]) - pd.Timestamp(ST[nm])).days / 365.25
    v = s.values / s.values[0]
    pk = np.maximum.accumulate(v)
    ser = pd.Series(v, index=pd.to_datetime(df.trade_date.values[i0:i0 + len(v)]))
    yr = ser.resample("YE").last()
    yr = pd.concat([pd.Series([ser.iloc[0]], index=[ser.index[0]]), yr]).pct_change().dropna()
    return dict(ann=v[-1] ** (1 / yrs) - 1, mdd=float(((v - pk) / pk).min()),
                nloss=int((yr < 0).sum()), nyr=len(yr), worst=float(yr.min()))


BAR = "=" * 112

print(BAR); print("命题A 可行域：卖出闸倍数（生产 ×1.00 vs 回测 ×1.30 必须二选一）"); print(BAR)
print(f"{'卖出闸':>16s}" + "".join(f"{nm:>32s}" for nm in data))
print(f"{'':>16s}" + "".join(f"{'年化':>7s}{'夏普':>6s}{'回撤':>7s}{'均仓':>5s}{'买笔':>4s}{'卖笔':>5s}" for _ in data))
for lab, mu in (("不卖（极点）", None), ("×1.00（生产）", 1.00), ("×1.15", 1.15),
                ("×1.30（回测）", 1.30), ("×1.50", 1.50)):
    cells = ""
    for nm in data:
        kw = dict(no_sell=True) if mu is None else dict(sell_mul=mu * (1.10 if nm == "创业板" else 1.0))
        r = R(nm, **kw)
        cells += f"{r['ann']:>7.2%}{r['sharpe']:>6.2f}{r['mdd']:>7.1%}{r['pos']:>5.0%}{r['nb']:>4d}{r['ns']:>5d}"
    print(f"{lab:>16s}" + cells)

print(BAR); print("命题B 可行域：加码斜率（买入金额的计算基数）"); print(BAR)
print(f"{'方案':>30s}" + "".join(f"{nm:>20s}" for nm in data))
print(f"{'':>30s}" + "".join(f"{'年化':>7s}{'夏普':>6s}{'均仓':>7s}" for _ in data))
for lab, sz in (("当前现金×比例（现状）", "cur"), ("起始资金×比例（金额恒定）", "init"),
                ("越深越大（金额递增）", "ramp")):
    cells = ""
    for nm in data:
        r = R(nm, size=sz)
        cells += f"{r['ann']:>7.2%}{r['sharpe']:>6.2f}{r['pos']:>7.0%}"
    print(f"{lab:>30s}" + cells)
print(f"\n{'方案':>30s}" + "".join(f"{nm:>26s}" for nm in data))
print(f"{'':>30s}" + "".join(f"{'买笔':>5s}{'均买价/全期':>11s}{'最低价档占比':>10s}" for _ in data))
for lab, sz in (("当前现金×比例（现状）", "cur"), ("起始资金×比例（金额恒定）", "init"),
                ("越深越大（金额递增）", "ramp")):
    cells = ""
    for nm in data:
        r = R(nm, size=sz)
        cells += f"{r['nb']:>5d}{r['vwap_rel']:>11.3f}{r['deep_share']:>10.1%}"
    print(f"{lab:>30s}" + cells)

print("\n" + BAR); print("命题C 可行域：底仓比例（首个买点一次性买入·永不卖出）"); print(BAR)
print(f"{'底仓':>16s}" + "".join(f"{nm:>28s}" for nm in data))
print(f"{'':>16s}" + "".join(f"{'年化':>7s}{'夏普':>6s}{'回撤':>7s}{'均仓':>5s}{'亏损年':>7s}" for _ in data))
for lab, b in (("0%（现状）", 0.0), ("25%", .25), ("50%", .50), ("75%", .75), ("100%", 1.0)):
    cells = ""
    for nm in data:
        r = R(nm, base=b)
        cells += f"{r['ann']:>7.2%}{r['sharpe']:>6.2f}{r['mdd']:>7.1%}{r['pos']:>5.0%}{r['nloss']:>3d}/{r['nyr']:<3d}"
    print(f"{lab:>16s}" + cells)
cells = ""
for nm in data:
    r = bh(nm)
    cells += f"{r['ann']:>7.2%}{'—':>6s}{r['mdd']:>7.1%}{'100%':>5s}{r['nloss']:>3d}/{r['nyr']:<3d}"
print(f"{'买入持有（极点）':>16s}" + cells)

print("\n" + BAR); print("命题D 可行域：卖出触发方式（时间触发 vs 价格网格棘轮）"); print(BAR)
print(f"{'方案':>26s}" + "".join(f"{nm:>24s}" for nm in data))
print(f"{'':>26s}" + "".join(f"{'年化':>7s}{'夏普':>6s}{'回撤':>7s}{'卖笔':>4s}" for _ in data))
for lab, kw in (("不卖（极点）", dict(no_sell=True)), ("月末 5%（现状）", {}),
                ("网格 每+2% 卖5%", dict(grid=.02)), ("网格 每+3.5% 卖5%", dict(grid=.035)),
                ("网格 每+5% 卖5%", dict(grid=.05)), ("网格 每+8% 卖5%", dict(grid=.08))):
    cells = ""
    for nm in data:
        r = R(nm, **kw)
        cells += f"{r['ann']:>7.2%}{r['sharpe']:>6.2f}{r['mdd']:>7.1%}{r['ns']:>4d}"
    print(f"{lab:>26s}" + cells)
print("\n起止：" + " ｜ ".join(f"{nm} {ST[nm]}~{EN[nm]}" for nm in data))
