# -*- coding: utf-8 -*-
"""红队：攻击 review_disposition_calib.py 的实现与 §2 标定结论。

只读 results/*.csv，不落库、不联网、不改任何生产代码。
分节：
  R0 复现主线数字（对齐基线）
  R1 引擎对账：calib.run vs long_window_backtest.run（金额 vs 比例的挂单口径差）
  R2 卖出闸口径审计：§2.2/2.3/2.4 到底跑在哪个闸位上
  R3 底仓实现审计：何时买、是否同日成交（前视）、闲置多久
  R4 底仓忠实变体（day0 全仓长持 / 分批建仓 / 分账口径）+ 细网格 X
  R5 网格棘轮 nxt 不复位 的影响（实现选择 or bug）
  R6 ramp 斜率参数敏感性 + 「不可耗尽 ∧ 越低越重」的构造性反例
  R7 自然年口径：首尾残年是否被当成完整年计入亏损年数
用法：cd /home/user/invest-model && python3 scripts/analysis/red_calib_attack.py [节号...]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np                                  # noqa: E402
import pandas as pd                                 # noqa: E402
import long_window_backtest as LW                   # noqa: E402
from long_window_backtest import CASH, RF, first_tradable, prep  # noqa: E402

ROOT = Path("results")
SRC = {"沪深300": ("index_dump_000300_SH.csv", "close", None),
       "创业板": ("spread_full_history.csv", "chinext", None),
       "科创50": ("index_dump_000688_SH.csv", "close", None),
       "红利": ("index_dump_000922_CSI.csv", "close", "index_dump_H00922_CSI.csv")}
MODE = {"沪深300": "anchor", "创业板": "anchor", "科创50": "ladder", "红利": "anchor"}
RUNG = [.50, .55, .60, .65]
_fear = pd.read_csv(ROOT / "fear_daily_dump.csv", dtype={"trade_date": str})
FMAP = dict(zip(_fear.trade_date, pd.to_numeric(_fear.score)))
DATA = {nm: prep(ROOT, f, c, t) for nm, (f, c, t) in SRC.items()}
ST = {nm: first_tradable(DATA[nm][0], MODE[nm], None) for nm in DATA}
EN = {nm: str(DATA[nm][0].trade_date.iloc[-1]) for nm in DATA}
BAR = "=" * 118


def run2(df, ret, nm, d0, d1, mode, *, sell_mul=None, no_sell=False, size="cur",
         base=0.0, base_mode="first_signal", grid=None, grid_reset=False,
         ramp_a=2.0, ramp_cap=0.50, esc_a=2.0, esc_cap=0.60, dca_n=0,
         cool=20, split_books=False):
    """主线 run() 的可控复刻。默认参数逐字等价于 review_disposition_calib.run()。

    base_mode: first_signal=主线（首个买点当日收盘即入，无 exec_lag）
               first_signal_lag=同上但按 exec_lag=1 成交
               day0=第一个可交易日全额买入（＝语料「全仓沪深300长期持有」）
               dca=自 day0 起每月末等额买入 dca_n 次
    grid_reset: 价格跌回卖出闸下方时把棘轮 nxt 复位（主线为 False＝永不复位）
    size="esc": 现金比例随折价放大（f=min(esc_cap, 0.20+esc_a*deep)）——不可耗尽且越低越重
    split_books: 分账口径，额外返回底仓/增强仓各自的年化
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
    bcurve, ecurve = [], []
    nxt = None
    base_buy_dates, dca_left = [], dca_n
    FR = {"cur": [.30, .35, .40, .50], "init": [.25, .25, .25, .25],
          "ramp": [.15, .20, .25, .40], "esc": [.30, .35, .40, .50]}[size]
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
            elif k_ == "BASE":
                a = min(amt, base_cash)
                if a > 0.05:
                    base_units += a / nav; base_cash -= a
                    base_buy_dates.append(str(d[i]))
            else:
                s = units * amt
                if s > 0:
                    cash += s * nav; units -= s; ns += 1
        pend = [x for x in pend if x[2] > i]

        sig, f = [], FMAP.get(d[i], np.nan)
        panic = f == f and f >= 75 and i - last > cool and r.r1250 == r.r1250 and ci < r.r1250
        if panic:
            sig.append(("B", cash * 0.50 if size in ("cur", "esc") else cash0 * 0.50))
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
                    sig.append(("B", cash * FR[j] if size in ("cur", "esc") else cash0 * FR[j]))
                    fired = True
            elif in_ep and dd >= -RUNG[0] * 0.5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            if size == "cur":
                amt = cash * 0.20
            elif size == "init":
                amt = cash0 * 0.20
            elif size == "esc":
                deep = max(0.0, 1 - ci / r.exp)
                amt = cash * min(esc_cap, 0.20 + esc_a * deep)
            else:
                deep = max(0.0, 1 - ci / r.exp)
                amt = cash0 * min(ramp_cap, 0.15 + ramp_a * deep)
            sig.append(("B", amt)); fired = True
        # ── 底仓 ──
        if base_cash > 0.05:
            if base_mode == "first_signal" and fired:
                base_units += base_cash / nav; base_cash = 0.0
                base_buy_dates.append(str(d[i]))
            elif base_mode == "first_signal_lag" and fired:
                sig.append(("BASE", base_cash))
            elif base_mode == "day0" and i == i0:
                base_units += base_cash / nav; base_cash = 0.0
                base_buy_dates.append(str(d[i]))
            elif base_mode == "dca" and r.me and dca_left > 0:
                a = min(base_cash, TOT * base / dca_n)
                base_units += a / nav; base_cash -= a; dca_left -= 1
                base_buy_dates.append(str(d[i]))
        # ── 卖腿 ──
        if not no_sell and r.exp == r.exp and units > 0:
            lvl = r.exp * mul
            if grid is None:
                if r.me and ci > lvl:
                    sig.append(("S", 0.05))
            else:
                if grid_reset and nxt is not None and ci < lvl:
                    nxt = None
                if nxt is None and ci > lvl:
                    nxt = lvl
                if nxt is not None and ci >= nxt:
                    sig.append(("S", 0.05)); nxt *= (1 + grid)
        for k_, amt in sig:
            pend.append((k_, amt, min(i + 1, i1 - 1)))
        tv = cash + units * nav + base_cash + base_units * nav
        curve.append(tv); pos.append((units + base_units) * nav / tv)
        bcurve.append(base_cash + base_units * nav); ecurve.append(cash + units * nav)
    v = np.array(curve); pk = np.maximum.accumulate(v)
    yrs = (pd.Timestamp(d[i1 - 1]) - pd.Timestamp(d[i0])).days / 365.25
    ann = (v[-1] / TOT) ** (1 / yrs) - 1
    vol = float(pd.Series(v).pct_change().dropna().std() * np.sqrt(250))
    idx = pd.to_datetime(d[i0:i1])
    ser = pd.Series(v, index=idx)
    yr = ser.resample("YE").last()
    yr = pd.concat([pd.Series([ser.iloc[0]], index=[ser.index[0]]), yr]).pct_change().dropna()
    # 完整自然年口径（剔除首尾残年）
    y0, y1 = idx[0].year, idx[-1].year
    full = [y for y in range(y0, y1 + 1)
            if not (y == y0 and idx[0].month > 1) and not (y == y1 and idx[-1].month < 12)]
    yrf = yr[[t.year in full for t in yr.index]]
    if buys:
        w = sum(a for _, a in buys)
        vwap_rel = sum(p * a for p, a in buys) / w / float(np.mean(c[i0:i1]))
        lo = min(p for p, _ in buys)
        deep_share = sum(a for p, a in buys if p <= lo * 1.05) / w
    else:
        vwap_rel = deep_share = np.nan
    out = dict(ann=ann, sharpe=(ann - RF) / vol, mdd=float(((v - pk) / pk).min()),
               nb=nb, ns=ns, pos=float(np.mean(pos)), vwap_rel=vwap_rel,
               deep_share=deep_share, nyr=len(yr), nloss=int((yr < 0).sum()),
               nyr_full=len(yrf), nloss_full=int((yrf < 0).sum()),
               worst=float(yr.min()), curve=v, dates=d[i0:i1],
               base_buy=base_buy_dates[0] if base_buy_dates else None,
               nbase=len(base_buy_dates))
    if split_books and base > 0:
        b = np.array(bcurve); e = np.array(ecurve)
        out["base_ann"] = (b[-1] / (TOT * base)) ** (1 / yrs) - 1 if b[-1] > 0 else np.nan
        out["enh_ann"] = (e[-1] / (TOT * (1 - base))) ** (1 / yrs) - 1 if TOT * (1 - base) > 0 else np.nan
        pb = np.maximum.accumulate(b); pe = np.maximum.accumulate(e)
        out["base_mdd"] = float(((b - pb) / pb).min())
        out["enh_mdd"] = float(((e - pe) / pe).min())
    return out


def R(nm, **kw):
    df, ret = DATA[nm]
    return run2(df, ret, nm, ST[nm], EN[nm], MODE[nm], **kw)


def bh(nm):
    df, ret = DATA[nm]
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
                nloss=int((yr < 0).sum()), nyr=len(yr))


NMS = list(SRC)


# ────────────────────────── R0 复现 ──────────────────────────
def sec_R0():
    print(BAR); print("R0 复现主线 §2.1/§2.3（默认参数下 run2 应逐字等于 calib.run）"); print(BAR)
    print(f"{'卖出闸':>14s}" + "".join(f"{nm:>26s}" for nm in NMS))
    for lab, mu in (("×1.00", 1.00), ("×1.30", 1.30)):
        cells = ""
        for nm in NMS:
            r = R(nm, sell_mul=mu * (1.10 if nm == "创业板" else 1.0))
            cells += f"{r['ann']:>8.2%}{r['sharpe']:>6.2f}{r['mdd']:>7.1%}{r['nb']:>3d}/{r['ns']:<3d}"
        print(f"{lab:>14s}" + cells)


# ────────────────────────── R1 引擎对账 ──────────────────────────
def sec_R1():
    print("\n" + BAR)
    print("R1 引擎对账：calib.run（挂单存金额、执行时 min(amt,cash)）vs 主线 long_window_backtest.run（挂单存比例）")
    print(BAR)
    print(f"{'腿':>10s}{'calib年化':>11s}{'LW年化':>11s}{'Δpp':>8s}"
          f"{'calib买笔':>10s}{'LW买笔':>9s}{'calib卖笔':>10s}{'LW卖笔':>9s}")
    for nm in NMS:
        df, ret = DATA[nm]
        a = run2(df, ret, nm, ST[nm], EN[nm], MODE[nm])            # 默认 ×1.30/1.43
        b = LW.run(df, ret, FMAP, nm, ST[nm], EN[nm], MODE[nm])
        print(f"{nm:>10s}{a['ann']:>11.4%}{b['ann']:>11.4%}{(a['ann']-b['ann'])*100:>+8.3f}"
              f"{a['nb']:>10d}{b['nb']:>9d}{a['ns']:>10d}{b['ns']:>9d}")
    # 同日双触发的场次统计
    print("\n  同日 B1+B2 双触发（挂单口径差异只在这些日子生效）：")
    for nm in NMS:
        df, _ = DATA[nm]
        d, c = df.trade_date.values, df.c.values
        i0 = int(np.searchsorted(d, ST[nm]))
        last, cnt = -999, 0
        for i in range(i0, len(d)):
            f = FMAP.get(d[i], np.nan)
            r = df.iloc[i]
            pan = f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and c[i] < r.r1250
            if f == f and f >= 75:
                last = i
            b1 = (MODE[nm] == "anchor" and r.we and r.exp == r.exp
                  and c[i] < r.exp * (0.90 if nm == "创业板" else 1.0))
            cnt += int(pan and b1)
        print(f"    {nm}: {cnt} 天")


# ────────────────────────── R2 卖出闸审计 ──────────────────────────
def sec_R2():
    print("\n" + BAR)
    print("R2 §2.2/2.3/2.4 的隐含闸位：主线 run() 的 sell_mul 默认＝×1.30，命题B/C/D 全部跑在 ×1.30 上")
    print("   （而 P58 同一节把 ×1.30 判为「作废重出」）→ 下面用 SPEC 规定的基线 ×1.00 重跑命题C")
    print(BAR)
    for tag, mu in (("×1.30（主线 §2.3 实际口径）", None), ("×1.00（SPEC 规定基线）", 1.00)):
        print(f"\n  【{tag}】")
        print(f"  {'底仓':>10s}" + "".join(f"{nm:>30s}" for nm in NMS))
        print(f"  {'':>10s}" + "".join(f"{'年化':>8s}{'夏普':>6s}{'回撤':>8s}{'均仓':>5s}{'亏损年':>7s}" for _ in NMS))
        for lab, bb in (("0%", 0.0), ("10%", .10), ("25%", .25), ("50%", .50), ("100%", 1.0)):
            cells = ""
            for nm in NMS:
                kw = {} if mu is None else dict(sell_mul=mu * (1.10 if nm == "创业板" else 1.0))
                r = R(nm, base=bb, **kw)
                cells += (f"{r['ann']:>8.2%}{r['sharpe']:>6.2f}{r['mdd']:>8.1%}"
                          f"{r['pos']:>5.0%}{r['nloss']:>3d}/{r['nyr']:<3d}")
            print(f"  {lab:>10s}" + cells)


# ────────────────────────── R3 底仓实现审计 ──────────────────────────
def sec_R3():
    print("\n" + BAR)
    print("R3 底仓实现审计：`fired` 当日即成交（无 exec_lag）＋ 首个买点出现得多晚")
    print(BAR)
    print(f"{'腿':>10s}{'策略起点':>10s}{'底仓实际买入日':>16s}{'空置交易日':>10s}{'空置占比':>9s}"
          f"{'同日成交年化':>13s}{'T+1成交年化':>13s}{'Δpp':>8s}")
    for nm in NMS:
        df, _ = DATA[nm]
        a = R(nm, base=0.25, base_mode="first_signal")
        b = R(nm, base=0.25, base_mode="first_signal_lag")
        d = df.trade_date.values
        i0 = int(np.searchsorted(d, ST[nm]))
        ib = int(np.searchsorted(d, a["base_buy"])) if a["base_buy"] else len(d)
        n = int(np.searchsorted(d, EN[nm], side="right")) - i0
        print(f"{nm:>10s}{ST[nm]:>10s}{str(a['base_buy']):>16s}{ib-i0:>10d}{(ib-i0)/n:>9.1%}"
              f"{a['ann']:>13.4%}{b['ann']:>13.4%}{(a['ann']-b['ann'])*100:>+8.3f}")
    print("\n  说明：主线把底仓在信号日收盘直接买入，其余全部买卖走 exec_lag=1 ⟹ 底仓单方面多一天信息。")


# ────────────────────────── R4 底仓忠实变体 ──────────────────────────
def sec_R4():
    print("\n" + BAR)
    print("R4 底仓变体（语料第一层＝「全仓沪深300并长期持有」，不是「等首个买点」）")
    print(BAR)
    variants = [("主线：首个买点一次性", dict(base_mode="first_signal")),
                ("day0 全额（全仓长持）", dict(base_mode="day0")),
                ("day0 起 12 个月分批", dict(base_mode="dca", dca_n=12)),
                ("day0 起 36 个月分批", dict(base_mode="dca", dca_n=36))]
    for bb in (0.25, 0.50):
        print(f"\n  【底仓 X={bb:.0%}，卖出闸 ×1.30（与主线 §2.3 同口径）】")
        print(f"  {'变体':>22s}" + "".join(f"{nm:>28s}" for nm in NMS))
        print(f"  {'':>22s}" + "".join(f"{'年化':>8s}{'夏普':>6s}{'回撤':>8s}{'亏损年':>6s}" for _ in NMS))
        base_ref = {nm: R(nm) for nm in NMS}
        print(f"  {'0% 底仓（现状）':>22s}" + "".join(
            f"{base_ref[nm]['ann']:>8.2%}{base_ref[nm]['sharpe']:>6.2f}"
            f"{base_ref[nm]['mdd']:>8.1%}{base_ref[nm]['nloss']:>3d}/{base_ref[nm]['nyr']:<2d}" for nm in NMS))
        for lab, kw in variants:
            cells = ""
            for nm in NMS:
                r = R(nm, base=bb, **kw)
                cells += f"{r['ann']:>8.2%}{r['sharpe']:>6.2f}{r['mdd']:>8.1%}{r['nloss']:>3d}/{r['nyr']:<2d}"
            print(f"  {lab:>22s}" + cells)
    print("\n  【E53 判据1 的细网格：存在 X 使四腿年化 ≥ 现状 −0.20pp？】（主线只测了 25/50/75/100）")
    print(f"  {'X':>6s}{'变体':>22s}" + "".join(f"{nm:>11s}" for nm in NMS) + f"{'判据1':>8s}{'判据2':>8s}")
    ref = {nm: R(nm) for nm in NMS}
    for lab, kw in variants:
        for x in (0.05, 0.10, 0.15, 0.20, 0.25):
            ds, ok2 = [], True
            for nm in NMS:
                r = R(nm, base=x, **kw)
                ds.append((r["ann"] - ref[nm]["ann"]) * 100)
                ok2 &= r["nloss"] <= ref[nm]["nloss"]
            ok1 = all(v >= -0.20 for v in ds)
            print(f"  {x:>6.0%}{lab:>22s}" + "".join(f"{v:>+11.2f}" for v in ds)
                  + f"{'✅' if ok1 else '❌':>8s}{'✅' if ok2 else '❌':>8s}")
    print("\n  【分账口径：底仓与增强仓各自记账（他的「逐层迁移」＝两本账）】X=25%，day0")
    print(f"  {'腿':>10s}{'合并年化':>10s}{'底仓年化':>10s}{'增强仓年化':>11s}{'底仓回撤':>10s}{'增强仓回撤':>11s}{'买持年化':>10s}")
    for nm in NMS:
        r = R(nm, base=0.25, base_mode="day0", split_books=True)
        print(f"  {nm:>10s}{r['ann']:>10.2%}{r['base_ann']:>10.2%}{r['enh_ann']:>11.2%}"
              f"{r['base_mdd']:>10.1%}{r['enh_mdd']:>11.1%}{bh(nm)['ann']:>10.2%}")


# ────────────────────────── R5 棘轮复位 ──────────────────────────
def sec_R5():
    print("\n" + BAR)
    print("R5 价格网格棘轮：nxt 永不复位（主线）vs 跌回闸下即复位（红队）")
    print(BAR)
    print(f"{'方案':>26s}" + "".join(f"{nm:>26s}" for nm in NMS))
    print(f"{'':>26s}" + "".join(f"{'年化':>8s}{'夏普':>6s}{'回撤':>8s}{'卖笔':>4s}" for _ in NMS))
    rows = [("月末 5%（现状）", {}), ]
    for g in (.02, .035, .05, .08):
        rows.append((f"网格 +{g:.1%} 不复位", dict(grid=g)))
    for g in (.02, .035, .05, .08):
        rows.append((f"网格 +{g:.1%} 复位★", dict(grid=g, grid_reset=True)))
    for lab, kw in rows:
        cells = ""
        for nm in NMS:
            r = R(nm, **kw)
            cells += f"{r['ann']:>8.2%}{r['sharpe']:>6.2f}{r['mdd']:>8.1%}{r['ns']:>4d}"
        print(f"{lab:>26s}" + cells)
    print("\n  E54 判据1（四腿年化全部不低于现状）在两种棘轮口径下的判定：")
    ref = {nm: R(nm) for nm in NMS}
    for reset in (False, True):
        for g in (.02, .035, .05, .08):
            ds = [(R(nm, grid=g, grid_reset=reset)["ann"] - ref[nm]["ann"]) * 100 for nm in NMS]
            tag = "复位" if reset else "不复位"
            print(f"    {tag} g={g:<6.1%}" + "".join(f"{v:>+9.2f}" for v in ds)
                  + f"   → {'✅全过' if all(v >= 0 for v in ds) else '❌'}")


# ────────────────────────── R6 加码斜率 ──────────────────────────
def sec_R6():
    print("\n" + BAR)
    print("R6 ramp 的 (斜率 a, 上限 cap) 敏感性 —— 主线只取了 (2.0, 0.50) 一个点，无任何依据")
    print(BAR)
    ref = {nm: R(nm) for nm in NMS}
    print(f"{'方案':>22s}" + "".join(f"{nm:>24s}" for nm in NMS))
    print(f"{'':>22s}" + "".join(f"{'年化':>8s}{'买笔':>5s}{'最低价档':>11s}" for _ in NMS))
    print(f"{'cur（现状）':>22s}" + "".join(
        f"{ref[nm]['ann']:>8.2%}{ref[nm]['nb']:>5d}{ref[nm]['deep_share']:>11.1%}" for nm in NMS))
    r0 = {nm: R(nm, size="init") for nm in NMS}
    print(f"{'init（金额恒定）':>22s}" + "".join(
        f"{r0[nm]['ann']:>8.2%}{r0[nm]['nb']:>5d}{r0[nm]['deep_share']:>11.1%}" for nm in NMS))
    for a in (0.5, 1.0, 2.0, 4.0):
        for cap in (0.35, 0.50, 0.80):
            cells = ""
            for nm in NMS:
                r = R(nm, size="ramp", ramp_a=a, ramp_cap=cap)
                cells += f"{r['ann']:>8.2%}{r['nb']:>5d}{r['deep_share']:>11.1%}"
            print(f"{f'ramp a={a} cap={cap:.0%}':>22s}" + cells)
    print("\n  ★ 构造性反例：「不可耗尽」与「越低越重」并不互斥 ——")
    print("    f(deep)=min(cap, 0.20+a·deep) 施加在**当前现金**上：永远只花掉现金的一个真分数（不可耗尽），")
    print("    而折价越深 f 越大（越低越重）。绝对金额非减的充要条件：f_{k+1} ≥ f_k/(1−f_k)。")
    print(f"{'方案':>22s}" + "".join(f"{nm:>24s}" for nm in NMS))
    print(f"{'':>22s}" + "".join(f"{'年化':>8s}{'买笔':>5s}{'最低价档':>11s}" for _ in NMS))
    for a in (1.0, 2.0, 4.0):
        for cap in (0.40, 0.60, 0.80):
            cells = ""
            for nm in NMS:
                r = R(nm, size="esc", esc_a=a, esc_cap=cap)
                cells += f"{r['ann']:>8.2%}{r['nb']:>5d}{r['deep_share']:>11.1%}"
            print(f"{f'esc a={a} cap={cap:.0%}':>22s}" + cells)
    print("\n  E52 判据1（四腿最低价档占比全部不下降 且 ≥2 腿 +5pp）逐臂判定：")
    for lab, kw in [("init", dict(size="init")), ("ramp a=2 cap=50%", dict(size="ramp"))] + \
                   [(f"esc a={a} cap={c:.0%}", dict(size="esc", esc_a=a, esc_cap=c))
                    for a in (1.0, 2.0, 4.0) for c in (0.40, 0.60, 0.80)]:
        ds = [(R(nm, **kw)["deep_share"] - ref[nm]["deep_share"]) * 100 for nm in NMS]
        anns = [(R(nm, **kw)["ann"] - ref[nm]["ann"]) * 100 for nm in NMS]
        ok = all(v >= 0 for v in ds) and sum(v >= 5 for v in ds) >= 2
        ok2 = all(v >= -0.30 for v in anns)
        print(f"    {lab:>18s} Δ最低价档" + "".join(f"{v:>+8.1f}" for v in ds)
              + f"  判据1 {'✅' if ok else '❌'}   Δ年化" + "".join(f"{v:>+7.2f}" for v in anns)
              + f"  判据2 {'✅' if ok2 else '❌'}")


# ────────────────────────── R7 自然年口径 ──────────────────────────
def sec_R7():
    print("\n" + BAR)
    print("R7 自然年亏损年数：首尾残年是否被当作完整年（E53 判据2 的分母）")
    print(BAR)
    print(f"{'腿':>10s}{'区间':>22s}{'主线口径 亏损/总':>18s}{'剔除首尾残年':>16s}{'首年':>8s}{'末年':>8s}")
    for nm in NMS:
        r = R(nm)
        print(f"{nm:>10s}{ST[nm]+'~'+EN[nm]:>22s}{str(r['nloss'])+'/'+str(r['nyr']):>18s}"
              f"{str(r['nloss_full'])+'/'+str(r['nyr_full']):>16s}{ST[nm][:6]:>8s}{EN[nm][:6]:>8s}")
    print("\n  E53 判据2（亏损年数不增加）在两种口径下的判定（X=25%，主线底仓实现）：")
    print(f"  {'口径':>16s}" + "".join(f"{nm:>16s}" for nm in NMS))
    ref = {nm: R(nm) for nm in NMS}
    for tag, ka, kb in (("含残年（主线）", "nloss", "nyr"), ("仅完整年", "nloss_full", "nyr_full")):
        cells = ""
        okall = True
        for nm in NMS:
            r = R(nm, base=0.25)
            ok = r[ka] <= ref[nm][ka]
            okall &= ok
            cells += f"{str(ref[nm][ka])+'→'+str(r[ka]):>13s}{'✅' if ok else '❌':>3s}"
        print(f"  {tag:>16s}" + cells + f"   判据2 {'✅' if okall else '❌'}")


# ────────────────────────── R8 「互斥」的实测判决 ──────────────────────────
def _buy_stream(nm, **kw):
    """返回 (日期, 价格, 金额) 买入流水，用于检验绝对金额单调性。"""
    df, ret = DATA[nm]
    d, c = df.trade_date.values, df.c.values
    i0, i1 = int(np.searchsorted(d, ST[nm])), int(np.searchsorted(d, EN[nm], side="right"))
    rr = ret.pct_change().fillna(0).values if ret is not None else None
    size = kw.get("size", "cur"); esc_a = kw.get("esc_a", 2.0); esc_cap = kw.get("esc_cap", 0.60)
    ramp_a = kw.get("ramp_a", 2.0); ramp_cap = kw.get("ramp_cap", 0.50)
    cash, units, nav, cash0 = 100.0, 0.0, 1.0, 100.0
    last, pend, out = -999, [], []
    mul = 1.43 if nm == "创业板" else 1.30
    armed, in_ep = np.ones(4, bool), False
    FR = {"cur": [.30, .35, .40, .50], "init": [.25] * 4,
          "ramp": [.15, .20, .25, .40], "esc": [.30, .35, .40, .50]}[size]
    for i in range(i0, i1):
        ci = float(c[i])
        if i > i0:
            cash *= (1 + CASH) ** ((pd.Timestamp(d[i]) - pd.Timestamp(d[i - 1])).days / 365.25)
            nav = nav * (1 + rr[i]) if rr is not None else ci
        elif rr is None:
            nav = ci
        r = df.iloc[i]
        for k_, amt, _t in [x for x in pend if x[2] == i]:
            if k_ == "B":
                a = min(amt, cash)
                if a > 0.05:
                    units += a / nav; cash -= a; out.append((str(d[i]), ci, a))
            else:
                s = units * amt
                if s > 0:
                    cash += s * nav; units -= s
        pend = [x for x in pend if x[2] > i]
        sig, f = [], FMAP.get(d[i], np.nan)
        if f == f and f >= 75 and i - last > 20 and r.r1250 == r.r1250 and ci < r.r1250:
            sig.append(("B", cash * .50 if size in ("cur", "esc") else cash0 * .50))
        if f == f and f >= 75:
            last = i
        if MODE[nm] == "ladder":
            dd = ci / r.peak - 1
            if dd <= -RUNG[0]:
                if not in_ep:
                    in_ep, armed[:] = True, True
                j = max([k2 for k2, th in enumerate(RUNG) if dd <= -th] or [0])
                if armed[j] and r.we:
                    armed[j] = False
                    sig.append(("B", cash * FR[j] if size in ("cur", "esc") else cash0 * FR[j]))
            elif in_ep and dd >= -RUNG[0] * .5:
                in_ep, armed[:] = False, True
        elif r.we and r.exp == r.exp and ci < r.exp * (0.90 if nm == "创业板" else 1.0):
            deep = max(0.0, 1 - ci / r.exp)
            amt = {"cur": cash * .20, "init": cash0 * .20,
                   "esc": cash * min(esc_cap, .20 + esc_a * deep),
                   "ramp": cash0 * min(ramp_cap, .15 + ramp_a * deep)}[size]
            sig.append(("B", amt))
        if r.exp == r.exp and units > 0 and r.me and ci > r.exp * mul:
            sig.append(("S", 0.05))
        for k_, amt in sig:
            pend.append((k_, amt, min(i + 1, i1 - 1)))
    return out


def sec_R8():
    print("\n" + BAR)
    print("R8 P59「不可耗尽 vs 越低越重 本质互斥」的实测判决")
    print("   口径：同一下跌段内（相邻两笔买入价格更低）相邻金额是否非减；以及现金是否被打光")
    print(BAR)
    print(f"{'方案':>20s}" + "".join(f"{nm:>24s}" for nm in NMS))
    print(f"{'':>20s}" + "".join(f"{'越买越少对':>11s}{'越买越多对':>11s}" for _ in NMS))
    arms = [("cur（现状）", dict(size="cur")), ("init", dict(size="init")),
            ("ramp a=2 cap=50%", dict(size="ramp")),
            ("esc a=2 cap=60%", dict(size="esc", esc_a=2.0, esc_cap=0.60)),
            ("esc a=4 cap=80%", dict(size="esc", esc_a=4.0, esc_cap=0.80))]
    for lab, kw in arms:
        cells = ""
        for nm in NMS:
            st = _buy_stream(nm, **kw)
            dn = up = 0
            for (d1, p1, a1), (d2, p2, a2) in zip(st, st[1:]):
                if p2 < p1:                      # 更低的价位
                    if a2 < a1 * 0.999:
                        dn += 1
                    elif a2 > a1 * 1.001:
                        up += 1
            cells += f"{dn:>11d}{up:>11d}"
        print(f"{lab:>20s}" + cells)
    print("\n  读数：'越买越少对'＝在更低价位反而投入更少的相邻买入对；'越买越多对'＝更低价位投入更多。")


SECS = {"R0": sec_R0, "R1": sec_R1, "R2": sec_R2, "R3": sec_R3,
        "R4": sec_R4, "R5": sec_R5, "R6": sec_R6, "R7": sec_R7, "R8": sec_R8}
if __name__ == "__main__":
    want = [a.upper() for a in sys.argv[1:]] or list(SECS)
    print("起止：" + " ｜ ".join(f"{nm} {ST[nm]}~{EN[nm]}" for nm in NMS))
    for k in want:
        SECS[k]()
