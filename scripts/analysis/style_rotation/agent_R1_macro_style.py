"""
E34 / R1（宏观·利率驱动路线）研究脚本

内容：
  A. 被解释变量构造（真实 CSV）：未来 60 个交易日「成长 − 价值」相对收益
  B. 宏观信号取数层（tushare）：含**发布滞后对齐**实现；数据不可得时抛 DataUnavailable
  C. E34 判据评估器（Spearman 全样本/分半、方向准确率、独立 episode、组合口径）
  D. 自由度/功效分析：判据②的 episode 上限、以及零假设下判据①②的误过率标定

一切口径在跑数前写死于本文件顶部 CONFIG 段，不得事后调整。
只读本目录 CSV 与 tushare，不造数。
"""
from __future__ import annotations

import os
import sys
import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

HERE = os.path.dirname(os.path.abspath(__file__))
END_DATE = "20260729"          # BRIEF 写死的统一终点
HORIZON = 60                   # 未来 60 个交易日
TAIL_Q = 0.20                  # 20/80 极端分位分档
RHO_MIN = 0.15                 # 判据①
ACC_MIN = 0.58                 # 判据②
EPISODE_MIN = 30               # 判据②
RF = 0.02                      # 夏普无风险利率
IDLE_CASH_RATE = 0.02          # 闲置现金年化
EXEC_LAG = 1
REBAL_BAND = 0.05              # 「卖出 flat 5%」→ 权重偏离带宽 5pp 才动手
TILT_CAP = 0.10                # 风格倾斜 ±10pp 上限
SEED = 20260802


class DataUnavailable(RuntimeError):
    pass


# ─────────────────────────────────────────────────────────────
# A. 被解释变量
# ─────────────────────────────────────────────────────────────
def _read_close(fname: str, col: str = "close") -> pd.Series:
    df = pd.read_csv(os.path.join(HERE, fname), dtype={"trade_date": str})
    s = df.set_index("trade_date")[col].astype(float).sort_index()
    return s[s.index <= END_DATE]


def load_legs() -> dict[str, pd.Series]:
    spread = pd.read_csv(os.path.join(HERE, "spread_full.csv"), dtype={"trade_date": str})
    spread = spread.set_index("trade_date").sort_index()
    spread = spread[spread.index <= END_DATE]
    return {
        "dividend": _read_close("000922_csi.csv"),        # 中证红利（价值腿）
        "chinext": spread["chinext"].astype(float),        # 创业板（成长腿1）
        "star50": _read_close("star50.csv"),               # 科创50（成长腿2）
        "hs300": _read_close("hs300.csv"),                 # 市场基准
    }


def forward_relative_return(growth: pd.Series, value: pd.Series, h: int = HORIZON) -> pd.DataFrame:
    """未来 h 个交易日「成长 − 价值」相对收益。仅保留两腿共同交易日。"""
    idx = growth.index.intersection(value.index)
    g, v = growth.loc[idx].sort_index(), value.loc[idx].sort_index()
    fwd_g = g.shift(-h) / g - 1.0
    fwd_v = v.shift(-h) / v - 1.0
    out = pd.DataFrame({"g": g, "v": v, "y": fwd_g - fwd_v})
    return out.dropna(subset=["y"])


# ─────────────────────────────────────────────────────────────
# B. 宏观信号取数（含发布滞后对齐）
# ─────────────────────────────────────────────────────────────
# 发布滞后表（写死）：统计月 → 实际可得日。取「保守可得日」= 次月的第 N 个自然日，
# 只要该日 ≤ 交易日，才允许该统计月的数值进入当日信号。
# 依据国家统计局/人民银行常规发布节奏，取比实际再晚 1-3 天的保守值，宁可迟不可早。
PUBLICATION_LAG = {
    "cpi": ("next_month_day", 12),      # CPI 次月 9-10 日左右 → 保守 12 日
    "ppi": ("next_month_day", 12),      # PPI 与 CPI 同批
    "m1": ("next_month_day", 18),       # 金融统计数据 次月 10-15 日不定 → 保守 18 日
    "m2": ("next_month_day", 18),
    "sf": ("next_month_day", 18),       # 社融与金融统计同批
    "pmi": ("next_month_day", 2),       # 当月最后一日发布 → 保守次月 2 日
}


def available_date(period_yyyymm: str, rule: tuple[str, int]) -> str:
    """统计月 → 保守实际可得日（YYYYMMDD）。"""
    kind, day = rule
    assert kind == "next_month_day"
    y, m = int(period_yyyymm[:4]), int(period_yyyymm[4:6])
    y2, m2 = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y2:04d}{m2:02d}{day:02d}"


def align_monthly_to_daily(monthly: pd.Series, key: str, trade_days: pd.Index) -> pd.Series:
    """月频宏观序列 → 日频信号。

    1) 每个统计月按 PUBLICATION_LAG 映射到「实际可得日」；
    2) 在交易日轴上做**前值填充**（ffill）——只用已公布的最新一期，
       绝不把统计月直接对齐到当月交易日（那是最典型的前视偏差）；
    3) 可得日之前的交易日为 NaN，直接丢弃，不做任何回填/插值。
    """
    if key not in PUBLICATION_LAG:
        raise KeyError(f"{key} 未登记发布滞后，拒绝对齐")
    rule = PUBLICATION_LAG[key]
    rows = [(available_date(str(p), rule), float(v)) for p, v in monthly.items() if pd.notna(v)]
    rows.sort()
    avail = pd.Series({d: v for d, v in rows})
    s = avail.reindex(sorted(set(avail.index) | set(trade_days))).ffill()
    return s.reindex(trade_days)


def align_daily_to_daily(daily: pd.Series, trade_days: pd.Index) -> pd.Series:
    """日频宏观/利率序列（10Y国债、DR007、shibor）→ 交易日轴：T 日数值 T+1 才可用（EOD 发布）。"""
    s = daily.sort_index().shift(1)
    return s.reindex(sorted(set(s.index) | set(trade_days))).ffill().reindex(trade_days)


def fetch_macro() -> dict[str, pd.Series]:
    """从 tushare 拉宏观/利率原始序列。不可得则抛 DataUnavailable（禁止代理硬凑）。"""
    sys.path.insert(0, "/home/user/invest-model")
    try:
        from invest_model.sources.tushare_client import TushareClient
        c = TushareClient()
    except Exception as e:  # noqa: BLE001
        raise DataUnavailable(f"tushare 客户端不可用: {e}") from e

    out: dict[str, pd.Series] = {}
    probes = {
        "m_supply": lambda: c.pro.cn_m(start_m="200501", end_m="202607"),
        "cpi": lambda: c.pro.cn_cpi(start_m="200501", end_m="202607"),
        "ppi": lambda: c.pro.cn_ppi(start_m="200501", end_m="202607"),
        "sf": lambda: c.pro.cn_sf(start_m="200501", end_m="202607"),
        "yc_cb": lambda: c.pro.yc_cb(ts_code="1001.CB", curve_type="0",
                                     start_date="20050101", end_date=END_DATE),
        "shibor": lambda: c.pro.shibor(start_date="20050101", end_date=END_DATE),
    }
    for k, fn in probes.items():
        try:
            df = fn()
            if df is not None and len(df):
                out[k] = df
        except Exception as e:  # noqa: BLE001
            print(f"  [接口失败] {k}: {e}")
    if not out:
        raise DataUnavailable("全部宏观接口均不可得")
    return out


# ─────────────────────────────────────────────────────────────
# C. E34 判据评估器
# ─────────────────────────────────────────────────────────────
def count_episodes(flag: pd.Series, h: int = HORIZON) -> list[str]:
    """独立 episode 口径（写死）：按时间顺序扫描信号点亮日，取第一个点亮日为一个 episode，
    随后 h 个交易日内的点亮日全部并入该 episode；h 日之后的下一个点亮日开启新 episode。
    ⇒ 每个 episode 的未来 60 日窗口互不重叠 = 独立观测。"""
    days = list(flag.index[flag.fillna(False).astype(bool)])
    all_days = list(flag.index)
    pos = {d: i for i, d in enumerate(all_days)}
    eps, last = [], -10**9
    for d in days:
        if pos[d] - last >= h:
            eps.append(d)
            last = pos[d]
    return eps


@dataclass
class Verdict:
    n: int
    rho: float
    p: float
    rho_h1: float
    rho_h2: float
    acc: float          # 全部极端交易日口径（重叠、非独立）
    acc_ep: float       # episode 口径（互不重叠 = 独立观测）
    n_ep: int
    c1: bool
    c2: bool            # 按 acc（BRIEF 字面口径）
    c2_ep: bool         # 按 acc_ep（本脚本建议的独立口径）


def evaluate(sig: pd.Series, y: pd.Series, label: str = "") -> Verdict:
    df = pd.concat([sig.rename("x"), y.rename("y")], axis=1).dropna()
    if len(df) < 50:
        return Verdict(len(df), np.nan, np.nan, np.nan, np.nan,
                       np.nan, np.nan, 0, False, False, False)
    rho, p = stats.spearmanr(df["x"], df["y"])
    half = len(df) // 2
    r1 = stats.spearmanr(df["x"][:half], df["y"][:half])[0]
    r2 = stats.spearmanr(df["x"][half:], df["y"][half:])[0]

    lo, hi = df["x"].quantile(TAIL_Q), df["x"].quantile(1 - TAIL_Q)
    hi_f, lo_f = df["x"] >= hi, df["x"] <= lo
    sign = 1.0 if (rho >= 0) else -1.0
    # 方向准确率：高分位预期 y 与 rho 同号，低分位反号
    hit_all = pd.concat([(df["y"][hi_f] * sign > 0), (df["y"][lo_f] * sign < 0)])
    acc = float(hit_all.mean()) if len(hit_all) else np.nan

    extreme = (hi_f | lo_f).reindex(df.index).fillna(False)
    ep_days = count_episodes(extreme)
    n_ep = len(ep_days)
    if n_ep:
        ep_hit = [(df.loc[d, "y"] * sign > 0) if hi_f.loc[d] else (df.loc[d, "y"] * sign < 0)
                  for d in ep_days]
        acc_ep = float(np.mean(ep_hit))
    else:
        acc_ep = np.nan

    c1 = (abs(rho) >= RHO_MIN) and (np.sign(r1) == np.sign(r2)) and np.isfinite(r1 * r2)
    c2 = (acc >= ACC_MIN) and (n_ep >= EPISODE_MIN)
    c2_ep = (acc_ep >= ACC_MIN) and (n_ep >= EPISODE_MIN)
    return Verdict(len(df), rho, p, r1, r2, acc, acc_ep, n_ep, bool(c1), bool(c2), bool(c2_ep))


# ─────────────────────────────────────────────────────────────
# C2. 组合口径（判据③）：四腿等权 25% 基线 + 风格倾斜
# ─────────────────────────────────────────────────────────────
def backtest(legs: dict[str, pd.Series], tilt: pd.Series | None = None) -> dict:
    """一笔钱 100；四腿等权 25%（红利/创业板/科创50/沪深300）；闲置现金 2%；
    exec_lag=1；权重偏离 ≥5pp 才调仓（买入检查周频）；风格倾斜年度再平衡、±10pp 上限。
    tilt: 日频 [-1,1] 信号，正=偏成长，实际倾斜 = tilt * TILT_CAP，从红利腿挪到两成长腿。"""
    names = ["dividend", "chinext", "star50", "hs300"]
    px = pd.concat([legs[n].rename(n) for n in names], axis=1).dropna()
    if px.empty:
        raise ValueError("四腿无共同交易日")
    ret = px.pct_change().fillna(0.0)
    days = list(px.index)

    base = np.array([0.25, 0.25, 0.25, 0.25])
    # 年度再平衡日（每年首个交易日）+ 周频买入检查
    year_first = {d[:4]: None for d in days}
    rebal_days = set()
    for d in days:
        if year_first[d[:4]] is None:
            year_first[d[:4]] = d
            rebal_days.add(d)
    weekly = set(days[::5])

    tgt = base.copy()
    w = base.copy()
    cash = 0.0
    nav = [100.0]
    pend = None
    for i in range(1, len(days)):
        d = days[i]
        r = ret.loc[d, names].values
        w = w * (1 + r)
        cash *= (1 + IDLE_CASH_RATE / 243)
        tot = w.sum() + cash
        w, cash = w / tot, cash / tot
        nav.append(nav[-1] * tot)

        if pend is not None and i >= pend[0]:            # exec_lag=1
            tgt_exec = pend[1]
            if np.abs(w - tgt_exec).max() >= REBAL_BAND:
                w = tgt_exec.copy()
            pend = None

        if d in rebal_days and tilt is not None:         # 风格倾斜：年度再平衡
            t = float(np.clip(tilt.get(d, 0.0) if pd.notna(tilt.get(d, np.nan)) else 0.0, -1, 1))
            shift = t * TILT_CAP
            tgt = base + np.array([-shift, shift / 2, shift / 2, 0.0])
            pend = (i + EXEC_LAG, tgt)
        elif d in weekly or d in rebal_days:
            pend = (i + EXEC_LAG, tgt)

    nav = pd.Series(nav, index=days)
    yrs = len(days) / 243.0
    ann = (nav.iloc[-1] / nav.iloc[0]) ** (1 / yrs) - 1
    dr = nav.pct_change().dropna()
    vol = dr.std() * np.sqrt(243)
    sharpe = (ann - RF) / vol if vol > 0 else np.nan
    mdd = float((nav / nav.cummax() - 1).min())
    return {"ann": float(ann), "vol": float(vol), "sharpe": float(sharpe),
            "mdd": mdd, "days": len(days), "years": round(yrs, 2)}


# ─────────────────────────────────────────────────────────────
# D. 自由度 / 零假设标定
# ─────────────────────────────────────────────────────────────
def ar1_null(n: int, phi: float, rng: np.random.Generator) -> np.ndarray:
    x = np.zeros(n)
    for i in range(1, n):
        x[i] = phi * x[i - 1] + rng.normal(0, np.sqrt(1 - phi ** 2))
    return x


def null_calibration(y: pd.Series, freq: str, phi: float, n_sim: int = 400) -> dict:
    """零假设标定：造与真实宏观序列同持续性的随机信号（与 y 完全无关），
    统计判据①②的误过率。freq='monthly'：月频 AR(1) 后 ffill 到日频（M1/CPI/社融口径）；
    freq='daily'：日频 AR(1)（10Y 国债收益率 / DR007 / shibor 口径）。"""
    rng = np.random.default_rng(SEED)
    days = list(y.index)
    months = sorted({d[:6] for d in days})
    p1 = p2 = p2e = p12 = p12e = 0
    rhos, eps = [], []
    for _ in range(n_sim):
        if freq == "monthly":
            m = dict(zip(months, ar1_null(len(months), phi, rng)))
            x = pd.Series([m[d[:6]] for d in days], index=days)
        else:
            x = pd.Series(ar1_null(len(days), phi, rng), index=days)
        v = evaluate(x, y)
        rhos.append(abs(v.rho)); eps.append(v.n_ep)
        p1 += v.c1; p2 += v.c2; p2e += v.c2_ep
        p12 += (v.c1 and v.c2); p12e += (v.c1 and v.c2_ep)
    return {"freq": freq, "phi": phi, "n_sim": n_sim,
            "误过①": p1 / n_sim, "误过②": p2 / n_sim, "误过②_ep": p2e / n_sim,
            "误过①②": p12 / n_sim, "误过①②_ep": p12e / n_sim,
            "|rho|中位数": float(np.median(rhos)), "|rho|95分位": float(np.percentile(rhos, 95)),
            "episode中位数": float(np.median(eps)), "episode最大": int(np.max(eps))}


# ─────────────────────────────────────────────────────────────
def main() -> None:
    print("=" * 78)
    print("E34 / R1 宏观·利率路线")
    print("=" * 78)

    legs = load_legs()
    for k, v in legs.items():
        print(f"  {k:9s} {len(v):5d} td  {v.index.min()} → {v.index.max()}")

    pairs = {
        "创业板−红利": forward_relative_return(legs["chinext"], legs["dividend"]),
        "科创50−红利": forward_relative_return(legs["star50"], legs["dividend"]),
    }

    print("\n【A】被解释变量（未来 60td 成长−价值 相对收益）")
    for k, df in pairs.items():
        print(f"  {k}: n={len(df)}  {df.index.min()}→{df.index.max()}  "
              f"均值 {df['y'].mean():+.4f}  中位 {df['y'].median():+.4f}  std {df['y'].std():.4f}")

    print("\n【B】宏观信号取数")
    macro_ok, macro_err = False, ""
    try:
        raw = fetch_macro()
        macro_ok = True
        print(f"  取到: {list(raw.keys())}")
    except DataUnavailable as e:
        macro_err = str(e)
        print(f"  ❌ 数据不可得: {e}")
        print("  → 判据①②③对宏观信号无法评估（禁止用价格类代理硬凑）")

    print("\n【D-1】判据② episode 上限（纯日历上界，与信号无关）")
    cap = {}
    for k, df in pairs.items():
        n_td = len(df)
        c = n_td // HORIZON
        cap[k] = c
        print(f"  {k}: 有效交易日 {n_td} → 不重叠 60td 窗口上限 {c} 个"
              f"  {'✅ 可能达到 30' if c >= EPISODE_MIN else '❌ 无论什么信号都达不到 30'}")
        # 月频宏观的现实上限
        n_mo = len({d[:6] for d in df.index})
        tail_mo = int(round(n_mo * TAIL_Q * 2))
        print(f"      月频宏观：{n_mo} 个统计月 → 双尾极端月 ≈ {tail_mo} 个，"
              f"每 episode 占用 ≈3 个月 → 现实 episode ≈ {tail_mo // 3}")

    print("\n【D-2】零假设误过率标定（信号与 y 无关，仅持续性相同；φ 邻域扫描）")
    null_res = {}
    grid = [("monthly", 0.90), ("monthly", 0.95), ("monthly", 0.98),
            ("daily", 0.95), ("daily", 0.98), ("daily", 0.995)]
    for k, df in pairs.items():
        for freq, phi in grid:
            r = null_calibration(df["y"], freq, phi, n_sim=300)
            null_res[f"{k}|{freq}|{phi}"] = r
            print(f"  {k} / {freq:8s} φ={phi:<5}: 误过① {r['误过①']:5.1%}  "
                  f"②(全日) {r['误过②']:5.1%}  ②(episode) {r['误过②_ep']:5.1%}  "
                  f"①②同过 {r['误过①②']:5.1%} → ep口径 {r['误过①②_ep']:5.1%}  "
                  f"|ρ|95分位 {r['|rho|95分位']:.3f}  ep中位 {r['episode中位数']:.0f}")

    print("\n【D-3】多重比较：族系误过率 FWER = 1-(1-p)^K（p 取创业板/monthly φ=0.95 的 ①②_ep）")
    p_one = null_res["创业板−红利|monthly|0.95"]["误过①②_ep"]
    fwer = {}
    for K in (1, 3, 6, 10, 15):
        fwer[K] = 1 - (1 - p_one) ** K
        print(f"  候选信号数 K={K:2d}: 至少一条纯噪声信号过①② 的概率 = {fwer[K]:.1%}")

    print("\n【C】判据③基线（四腿等权 25%，无信号）")
    bl = backtest(legs, tilt=None)
    print(f"  {bl}")
    print("  ※ 四腿共同交易日受科创50（2019-12 起）限制")
    rng = np.random.default_rng(SEED)
    days = list(pd.concat([legs[n] for n in ["dividend", "chinext", "star50", "hs300"]],
                          axis=1).dropna().index)
    rnd_tilt = pd.Series(ar1_null(len(days), 0.98, rng), index=days).clip(-1, 1)
    bt = backtest(legs, tilt=rnd_tilt)
    print(f"  随机倾斜（管路自检，非结论）: {bt}")

    out = {
        "macro_available": macro_ok, "macro_error": macro_err,
        "episode_cap": cap, "null": null_res, "fwer": fwer,
        "baseline": bl, "random_tilt": bt,
        "y_stats": {k: {"n": len(v), "start": v.index.min(), "end": v.index.max(),
                        "mean": float(v["y"].mean()), "std": float(v["y"].std())}
                    for k, v in pairs.items()},
    }
    with open(os.path.join(HERE, "agent_R1_results.json"), "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n结果写入 agent_R1_results.json")


if __name__ == "__main__":
    main()
