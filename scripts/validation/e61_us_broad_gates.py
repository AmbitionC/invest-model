# -*- coding: utf-8 -*-
"""E61 —— 美股宽基（SPY/QQQ）适配 A 股价值锚引擎 + VIX 恐慌腿（P71，判据登记于
docs/model_change_proposals.md P71 段、先于本脚本提交，git 可查前后顺序）。

引擎＝A 股宽基四腿的锚闸结构逐字移植（scripts/analysis/long_window_backtest 同构）：
  信号腿：raw Close vs expanding 中位锚（WARM 预热），买入闸 ×1.00（周频、每笔投现金 20%），
  卖出闸 ×1.30（月末减持仓 5%）；恐慌腿：VIX≥TH AND 收盘<近5年(1250td)滚动中位
  AND 20td 冷却 → 投现金 50%。现金 2%/年。exec_lag=1。
  **信号用 raw Close、收益用 Adj Close**（Adj Close 是回溯调整序列，拿它算 expanding
  中位属前视——同红利腿「信号价格指数/收益全收益」口径）。

对比臂：A=引擎一次性投入 vs 买入不动；C=月定投 $1000；D=同现金流进引擎（XIRR 对比）；
③=月度起点栅格·满仓起手。判据 ①~⑧ 全部评估不短路，产物附极端样本与共现率。

跑在 us-update.yml `e61` 模式（Actions 美国出口 runner，yfinance 可用）。只读不落库。
  python scripts/validation/e61_us_broad_gates.py [--out results/e61_us_broad.md] [--post-issue]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

CASH_Y = 0.02          # 现金年化（与 A 股引擎同参）
BUY_MUL, SELL_MUL = 1.00, 1.30
BUY_FRAC, SELL_FRAC, PANIC_FRAC = 0.20, 0.05, 0.50
COOLDOWN = 20
WARM_MAIN, WARM_GRID = 500, (120, 250, 500, 650)
VIX_MAIN, VIX_GRID = 30.0, (25.0, 30.0, 35.0, 40.0)
DCA_AMT = 1000.0
LEGS = ("SPY", "QQQ")


def fetch(tk: str) -> pd.DataFrame:
    import yfinance as yf
    d = yf.download(tk, period="max", auto_adjust=False, progress=False)
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = d.columns.get_level_values(0)
    d = d[["Close", "Adj Close"]].dropna().rename(columns={"Close": "c", "Adj Close": "adj"})
    d.index = pd.to_datetime(d.index).tz_localize(None)
    return d


def prep(leg: pd.DataFrame, vix: pd.Series, warm: int) -> pd.DataFrame:
    d = leg.copy()
    d["med"] = d["c"].expanding(min_periods=warm).median()
    d["r1250"] = d["c"].rolling(1250, min_periods=1250).median()
    d["vix"] = vix.reindex(d.index).ffill(limit=5)
    iso = d.index.isocalendar()
    wk = iso["year"].astype(int) * 100 + iso["week"].astype(int)
    d["we"] = wk.diff().fillna(1) != 0                      # 每周首个交易日检查买入
    mth = d.index.year * 100 + d.index.month
    d["me"] = pd.Series(mth, index=d.index).diff().shift(-1).fillna(1) != 0   # 月末交易日
    d["rr"] = d["adj"].pct_change().fillna(0.0)
    return d


def run_engine(d: pd.DataFrame, i0: int, i1: int, vix_th: float,
               full_start: bool = False, monthly_inflow: float = 0.0) -> dict:
    """A 股引擎逐字移植。full_start=满仓起手（③栅格）；monthly_inflow>0＝D 臂（无初始本金）。
    返回 ann/寿命内买卖笔数/恐慌笔数/现金流(XIRR用)/极端买入样本/期末价值。"""
    dates = d.index
    c, adj, rr = d["c"].to_numpy(), d["adj"].to_numpy(), d["rr"].to_numpy()
    med, r1250, vix = d["med"].to_numpy(), d["r1250"].to_numpy(), d["vix"].to_numpy()
    we, me = d["we"].to_numpy(), d["me"].to_numpy()
    init = 0.0 if monthly_inflow > 0 else 100.0
    cash, units, nav = init, 0.0, adj[i0]
    last_panic = -10**9
    pend: list[tuple[str, float, int, str]] = []
    flows: list[tuple[pd.Timestamp, float]] = []
    nb = ns = npan = 0
    buys: list[tuple[str, float, float, str]] = []
    cur_month = None
    if full_start:
        units, cash = init / nav, 0.0
    for i in range(i0, i1):
        if i > i0:
            cash *= (1 + CASH_Y) ** ((dates[i] - dates[i - 1]).days / 365.25)
            nav *= (1 + rr[i])
        if monthly_inflow > 0:
            m = (dates[i].year, dates[i].month)
            if m != cur_month:
                cur_month = m
                cash += monthly_inflow
                flows.append((dates[i], -monthly_inflow))
        for k, fr, t, why in [x for x in pend if x[2] == i]:
            if k == "B":
                a = cash * fr
                if a > 0.05:
                    units += a / nav
                    cash -= a
                    nb += 1
                    buys.append((str(dates[i].date()), float(c[i]), float(vix[i]) if np.isfinite(vix[i]) else float("nan"), why))
            else:
                s = units * fr
                if s > 0:
                    cash += s * nav
                    units -= s
                    ns += 1
        pend = [x for x in pend if x[2] > i]
        sig = []
        v = vix[i]
        if (np.isfinite(v) and v >= vix_th and i - last_panic > COOLDOWN
                and np.isfinite(r1250[i]) and c[i] < r1250[i]):
            sig.append(("B", PANIC_FRAC, f"panic(VIX={v:.0f})"))
            npan += 1
            last_panic = i
        elif we[i] and np.isfinite(med[i]) and c[i] < med[i] * BUY_MUL:
            sig.append(("B", BUY_FRAC, f"anchor(c/med={c[i] / med[i]:.2f})"))
        if me[i] and np.isfinite(med[i]) and c[i] > med[i] * SELL_MUL and units > 0:
            sig.append(("S", SELL_FRAC, "sell"))
        for k, fr, why in sig:
            pend.append((k, fr, min(i + 1, i1 - 1), why))
    tv = cash + units * nav
    yrs = (dates[i1 - 1] - dates[i0]).days / 365.25
    ann = (tv / init) ** (1 / yrs) - 1 if init > 0 and yrs > 0 else float("nan")
    flows.append((dates[i1 - 1], tv))
    return dict(ann=ann, tv=tv, yrs=yrs, nb=nb, ns=ns, npan=npan, flows=flows,
                buys=buys, pos=units * nav / tv if tv > 0 else 0.0)


def dca_index(d: pd.DataFrame, i0: int, i1: int) -> dict:
    """C 臂：每月首个交易日 $1000 买入（Adj Close 口径＝含股息再投）。"""
    dates, adj = d.index, d["adj"].to_numpy()
    units, cur_month = 0.0, None
    flows = []
    for i in range(i0, i1):
        m = (dates[i].year, dates[i].month)
        if m != cur_month:
            cur_month = m
            units += DCA_AMT / adj[i]
            flows.append((dates[i], -DCA_AMT))
    tv = units * adj[i1 - 1]
    flows.append((dates[i1 - 1], tv))
    return dict(tv=tv, flows=flows)


def xirr(flows: list[tuple[pd.Timestamp, float]]) -> float:
    t0 = flows[0][0]
    ts = np.array([(t - t0).days / 365.25 for t, _ in flows])
    cf = np.array([v for _, v in flows])

    def npv(r: float) -> float:
        return float(np.sum(cf / (1 + r) ** ts))
    lo, hi = -0.95, 5.0
    if npv(lo) * npv(hi) > 0:
        return float("nan")
    for _ in range(200):
        mid = (lo + hi) / 2
        if npv(lo) * npv(mid) <= 0:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2


def bh_ann(d: pd.DataFrame, i0: int, i1: int) -> float:
    yrs = (d.index[i1 - 1] - d.index[i0]).days / 365.25
    return (d["adj"].iloc[i1 - 1] / d["adj"].iloc[i0]) ** (1 / yrs) - 1


def episodes(idx: np.ndarray, gap: int = 60) -> list[int]:
    out, last = [], -10**9
    for i in idx:
        if i - last > gap:
            out.append(int(i))
        last = i
    return out


def vix_event_study(d: pd.DataFrame, vix_th: float) -> dict:
    """④：VIX≥TH AND c<r1250 触发日 → episode 归并 → 前瞻 20/60/120/250 日超额（Adj 口径）。"""
    c, adj = d["c"].to_numpy(), d["adj"].to_numpy()
    vix, r1250 = d["vix"].to_numpy(), d["r1250"].to_numpy()
    n = len(d)
    trig = np.where((vix >= vix_th) & np.isfinite(r1250) & (c < r1250) & np.isfinite(vix))[0]
    eps = episodes(trig)
    out = dict(n_trig=int(len(trig)), n_ep=len(eps), fwd={}, ep_dates=[str(d.index[i].date()) for i in eps])
    for h in (20, 60, 120, 250):
        ok = [i for i in eps if i + h < n]
        if not ok:
            out["fwd"][h] = None
            continue
        ev = np.mean([adj[i + h] / adj[i] - 1 for i in ok])
        base_i = np.arange(0, n - h)
        base_i = base_i[np.isfinite(vix[base_i])]           # 与触发同一可比窗（有 VIX 的日子）
        base = np.mean(adj[base_i + h] / adj[base_i] - 1)
        out["fwd"][h] = dict(ev=float(ev), base=float(base), ex=float(ev - base), n=len(ok))
    # 共现率：VIX≥TH 的日子里价格闸（c<r1250）同时成立的占比（E56 教训：换触发器先查共现）
    vd = np.where((vix >= vix_th) & np.isfinite(vix))[0]
    vd_ok = vd[np.isfinite(r1250[vd])]
    out["cooccur"] = float(np.mean(c[vd_ok] < r1250[vd_ok])) if len(vd_ok) else float("nan")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results/e61_us_broad.md")
    ap.add_argument("--post-issue", action="store_true")
    args = ap.parse_args()

    vix_raw = fetch("^VIX")["c"]
    legs = {t: fetch(t) for t in LEGS}
    L: list[str] = ["# E61 —— 美股宽基适配 A 股价值锚引擎 + VIX 恐慌腿（P71）", "",
                    f"跑数时间 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}；"
                    f"判据登记先于脚本提交（P71 段）。引擎参数：买闸×{BUY_MUL:.2f}/卖闸×{SELL_MUL:.2f}/"
                    f"周检月卖/恐慌 VIX≥{VIX_MAIN:.0f}∧c<r1250∧{COOLDOWN}td 冷却/现金 {CASH_Y:.0%}。", ""]
    # VIX 常识对撞：全历史 top5
    v5 = vix_raw.sort_values(ascending=False).head(5)
    L.append("**VIX 全历史 top5（应命中 2008-10~11 / 2020-03）**：" + "；".join(
        f"{d.date()}={v:.1f}" for d, v in v5.items()))
    L.append("")

    verdicts: dict[str, str] = {}
    for tk in LEGS:
        d = prep(legs[tk], vix_raw, WARM_MAIN)
        n = len(d)
        i0 = int(np.argmax(d["med"].notna().to_numpy()))    # 锚预热完成日＝首个可交易日
        if not d["med"].notna().any():
            L.append(f"⚠️ {tk} 锚预热不足，跳过")
            continue
        L += [f"## {tk}（{d.index[0].date()} ~ {d.index[-1].date()}，锚就绪 {d.index[i0].date()}）", ""]

        # ① 引擎一次性投入 vs 买入不动（信息判据，无 PASS 线）
        eng = run_engine(d, i0, n, VIX_MAIN)
        bh = bh_ann(d, i0, n)
        L.append(f"**① 策略 vs 买入不动**（同窗 {eng['yrs']:.1f} 年）：策略年化 {eng['ann']:+.2%} vs "
                 f"买持 {bh:+.2%} ＝ 超额 **{(eng['ann'] - bh) * 100:+.2f}pp**；"
                 f"买 {eng['nb']} 笔（恐慌 {eng['npan']}）/卖 {eng['ns']} 笔，期末仓位 {eng['pos']:.0%}")
        verdicts[f"{tk}-①"] = f"{(eng['ann'] - bh) * 100:+.2f}pp"

        # ② 定投对比（XIRR，同现金流）
        dca = dca_index(d, i0, n)
        engD = run_engine(d, i0, n, VIX_MAIN, monthly_inflow=DCA_AMT)
        x_c, x_d = xirr(dca["flows"]), xirr(engD["flows"])
        d2 = (x_d - x_c) * 100
        verdicts[f"{tk}-②"] = f"{d2:+.2f}pp（{'支持' if d2 >= 0.5 else '不支持'}）"
        L.append(f"**② 同现金流定投对比**：定投 XIRR {x_c:+.2%} vs 引擎 XIRR {x_d:+.2%} ＝ "
                 f"**{d2:+.2f}pp**（判据 ≥+0.5pp；引擎买 {engD['nb']} 笔/恐慌 {engD['npan']}/卖 {engD['ns']}，"
                 f"期末仓位 {engD['pos']:.0%}）")

        # ③ 月度起点栅格·满仓起手（≥5 年剩余窗）
        wins, tot = 0, 0
        month_seen = set()
        for i in range(i0, n):
            m = (d.index[i].year, d.index[i].month)
            if m in month_seen:
                continue
            month_seen.add(m)
            if (d.index[-1] - d.index[i]).days < 5 * 365:
                break
            g = run_engine(d, i, n, VIX_MAIN, full_start=True)
            b = bh_ann(d, i, n)
            tot += 1
            wins += int(g["ann"] >= b)
        L.append(f"**③ 月度起点栅格（满仓起手·剩余≥5年·{tot} 个起点）**：策略 ≥ 买持 的占比 "
                 f"**{wins / tot:.0%}**" if tot else "**③** 窗口不足不可评")
        verdicts[f"{tk}-③"] = f"{wins}/{tot}"

        # ④ VIX 恐慌腿事件级
        ev = vix_event_study(d, VIX_MAIN)
        seg = "；".join(f"{h}d {f['ex'] * 100:+.1f}pp(n={f['n']})" if f else f"{h}d 无窗"
                        for h, f in ev["fwd"].items())
        ep_ok = ev["n_ep"] >= 8
        L.append(f"**④ VIX 恐慌腿事件级**（VIX≥{VIX_MAIN:.0f}∧c<r1250，{ev['n_trig']} 触发日→"
                 f"{ev['n_ep']} episode{'，≥8 可读' if ep_ok else '，<8 样本不足'}）：超额 {seg}；"
                 f"episode 日期 {', '.join(ev['ep_dates'][:10])}{'…' if len(ev['ep_dates']) > 10 else ''}；"
                 f"**共现率**（VIX≥{VIX_MAIN:.0f} 日中价格闸同立）={ev['cooccur']:.0%}")
        verdicts[f"{tk}-④"] = ("样本不足" if not ep_ok else
                               "；".join(f"{h}d{f['ex'] * 100:+.1f}" for h, f in ev["fwd"].items() if f))

        # ⑤ WARM 敏感性（②的符号）
        w_out = []
        for w in WARM_GRID:
            dw = prep(legs[tk], vix_raw, w)
            j0 = int(np.argmax(dw["med"].notna().to_numpy())) if dw["med"].notna().any() else -1
            if j0 < 0:
                w_out.append(f"W{w}=不可评")
                continue
            dcw = dca_index(dw, j0, len(dw))
            egw = run_engine(dw, j0, len(dw), VIX_MAIN, monthly_inflow=DCA_AMT)
            w_out.append(f"W{w}={(xirr(egw['flows']) - xirr(dcw['flows'])) * 100:+.2f}pp")
        L.append(f"**⑤ WARM 敏感性（②口径）**：{'；'.join(w_out)}")
        verdicts[f"{tk}-⑤"] = "；".join(w_out)

        # ⑥ VIX 阈值敏感性（④的 60/120d 方向）
        t_out = []
        for th in VIX_GRID:
            e2 = vix_event_study(d, th)
            f60 = e2["fwd"].get(60)
            t_out.append(f"TH{th:.0f}: ep={e2['n_ep']}"
                         + (f", 60d{f60['ex'] * 100:+.1f}pp" if f60 else ""))
        L.append(f"**⑥ VIX 阈值敏感性**：{'；'.join(t_out)}")

        # ⑦ 分半方向（②）
        mid = (i0 + n) // 2
        halves = []
        for a, b2 in ((i0, mid), (mid, n)):
            if b2 - a < 500:
                halves.append("窗口不足")
                continue
            dc_h = dca_index(d, a, b2)
            eg_h = run_engine(d, a, b2, VIX_MAIN, monthly_inflow=DCA_AMT)
            halves.append(f"{(xirr(eg_h['flows']) - xirr(dc_h['flows'])) * 100:+.2f}pp")
        L.append(f"**⑦ 分半（②口径）**：H1={halves[0]}｜H2={halves[1]}")

        # ⑧ 空洞守卫 + 极端样本
        if eng["nb"] == 0:
            L.append("**⑧ 空洞守卫**：⚠️ 引擎全窗零买入——①③相应判据记「不可评」，不得读成风控/超额")
        pb = sorted(engD["buys"], key=lambda x: -(x[2] if np.isfinite(x[2]) else -1))[:5]
        if pb:
            L.append("**极端买入样本（D 臂按 VIX 降序 top5）**：" + "；".join(
                f"{dt} @{px:.0f} VIX={vv:.0f}({why.split('(')[0]})" for dt, px, vv, why in pb))
        L.append("")

    L += ["## 判据速览", ""] + [f"- {k}: {v}" for k, v in verdicts.items()]
    L += ["", "> 裁决与先验对照、结论撰写由主线完成（本产物只报数）。**零生产接线**：",
          "> 无论结果如何不改任何 A 股链路、不开美股自动交易（P71 裁决用途段）。"]
    md = "\n".join(L)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(md)
    if args.post_issue:
        from faas import gh_notify
        gh_notify.post_issue_comment(
            "🧪 验证报告",
            seed_body="本 issue 汇总预登记验证（E 系列）结论。",
            comment_body=md[:60000],
            dedupe_prefix="# E61 ——")
    print(f"\n[e61] 写出 {out}")


if __name__ == "__main__":
    main()
