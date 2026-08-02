#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
E34 / R2（盈利与景气驱动）—— 被解释变量刻画 + 统计功效审计 + 评估 harness 自检

本脚本**不构造任何候选信号**。它只做三件事：
  A. 按 BRIEF 写死口径构造被解释变量：未来 60 交易日「成长 − 价值」相对收益
     （成长腿 = 创业板指 / 科创50，价值腿 = 中证红利）
  B. 统计功效审计：季频基本面信号在本样本上**先验**能拿到多少独立 episode，
     以及在该 n 下 E34 判据①（|ρ|≥0.15）②（方向准确率≥58%）离随机噪声有多远
  C. 评估 harness 自检：用固定种子的安慰剂信号跑通全部指标计算，确认 harness 无偏
     （安慰剂**不是**候选信号，只用于验证代码；其结果应落在零假设附近）

只读本目录 CSV，不联网、不造数。
运行：python3 agent_R2_power_audit.py
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------- 写死参数
HORIZON = 60          # BRIEF 写死：未来 60 个交易日
END_DATE = 20260729   # BRIEF 写死：统一终点
EXTREME_LO, EXTREME_HI = 0.20, 0.80   # BRIEF 写死：信号 20/80 极端分位分档
RHO_MIN = 0.15        # E34 判据①
ACC_MIN = 0.58        # E34 判据②
EPISODE_MIN = 30      # E34 判据②
RF = 0.02             # 夏普 rf=2%
CASH_YIELD = 0.02     # 闲置现金 2%
SEED = 20260802       # 安慰剂随机种子（写死，保证可复现）
N_PLACEBO = 500       # 假阳性率重抽次数（写死）


def _load(fname: str, col: str, rename: str) -> pd.Series:
    df = pd.read_csv(os.path.join(HERE, fname))
    df = df[df["trade_date"] <= END_DATE]
    s = df.set_index("trade_date")[col].astype(float)
    s.name = rename
    return s[~s.index.duplicated(keep="last")].sort_index()


def load_legs() -> pd.DataFrame:
    value = _load("000922_csi.csv", "close", "dividend")        # 中证红利（价值腿）
    chinext = _load("spread_full.csv", "chinext", "chinext")     # 创业板指（成长腿1）
    star50 = _load("star50.csv", "close", "star50")              # 科创50（成长腿2）
    hs300 = _load("hs300.csv", "close", "hs300")                 # 市场基准 / 第四腿
    return pd.concat([value, chinext, star50, hs300], axis=1)


def forward_rel(px: pd.DataFrame, growth: str, value: str = "dividend") -> pd.DataFrame:
    """未来 60 交易日「成长 − 价值」对数相对收益（因果：t 日信号 → t..t+60 收益）。"""
    sub = px[[growth, value]].dropna()
    lg = np.log(sub[growth])
    lv = np.log(sub[value])
    fwd = (lg.shift(-HORIZON) - lg) - (lv.shift(-HORIZON) - lv)
    out = pd.DataFrame({"fwd_rel": fwd}, index=sub.index)
    out["date"] = pd.to_datetime(out.index.astype(str), format="%Y%m%d")
    return out


# ---------------------------------------------------------------- A. 被解释变量刻画
def describe_target(name: str, tgt: pd.DataFrame) -> dict:
    v = tgt["fwd_rel"].dropna()
    d = tgt.loc[v.index, "date"]
    ac = v.autocorr(lag=HORIZON)          # 相隔一个视界的自相关（重叠已消除处的持续性）
    return {
        "腿": name,
        "起": int(v.index.min()), "止": int(v.index.max()),
        "交易日": len(v),
        "年数": round((d.max() - d.min()).days / 365.25, 1),
        "均值%": round(v.mean() * 100, 2),
        "标准差%": round(v.std() * 100, 2),
        "正比例%": round((v > 0).mean() * 100, 1),
        f"自相关(lag{HORIZON})": round(ac, 3),
        "不重叠窗口数": len(v) // HORIZON,
    }


# ---------------------------------------------------------------- B. 功效审计
def quarters_available(tgt: pd.DataFrame, yoy_warmup_q: int = 4) -> dict:
    """季频基本面信号能取到多少个**独立**观测。

    季频财务信号在两个报告期之间是常数（阶跃函数），信息只在公告日更新，
    因此独立观测数 = 覆盖的报告期数，不是交易日数。
    再叠加 E34 的 20/80 极端分位分档（只保留两端 40%）与 YoY 预热（4 个季度）。
    """
    d = tgt.dropna(subset=["fwd_rel"])["date"]
    q = pd.PeriodIndex(d, freq="Q").unique()
    n_q = len(q)
    n_eff = max(n_q - yoy_warmup_q, 0)                  # YoY 需 4 季预热
    n_extreme = int(round(n_eff * (EXTREME_HI - EXTREME_LO + 0.0) * 0 + n_eff * 0.4))
    return {"覆盖季度数": n_q, "YoY预热后": n_eff, "20/80极端档后": n_extreme}


def power_table(ns: list[int]) -> pd.DataFrame:
    """在给定独立样本数 n 下，E34 判据①②离随机噪声有多远。"""
    rows = []
    for n in ns:
        # Spearman rho=0.15 在 n 下的双尾 p（用 Fisher-z 近似，n>=10 可用）
        if n > 3:
            z = np.arctanh(RHO_MIN) * np.sqrt(n - 3)
            p_rho = 2 * (1 - stats.norm.cdf(abs(z)))
        else:
            p_rho = np.nan
        # 方向准确率 58% 在 n 下的单尾二项 p（H0: p=0.5）
        k = int(np.ceil(ACC_MIN * n))
        p_acc = stats.binom.sf(k - 1, n, 0.5)
        # 达到 5% 显著所需的最小准确率
        k95 = stats.binom.isf(0.05, n, 0.5) + 1
        rows.append({
            "独立样本 n": n,
            f"ρ={RHO_MIN} 的 p 值": round(p_rho, 3),
            f"准确率≥{ACC_MIN:.0%} 需 k/n": f"{k}/{n}",
            "该准确率的 p 值": round(p_acc, 3),
            "5%显著所需准确率": f"{k95 / n:.1%}",
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- C. harness 自检
def eval_signal(sig: pd.Series, tgt: pd.DataFrame, label: str) -> dict:
    """E34 判据①②的完整计算：全样本/分半 Spearman + 极端分位方向准确率 + episode 数。

    episode 定义：信号进入极端档后的**连续段**算 1 个 episode（去重叠），
    段与段之间至少间隔 1 个非极端观测。
    """
    df = tgt.join(sig.rename("sig")).dropna(subset=["fwd_rel", "sig"])
    if df.empty:
        return {"信号": label, "备注": "无重叠样本"}
    rho, p = stats.spearmanr(df["sig"], df["fwd_rel"])
    mid = len(df) // 2
    r1, _ = stats.spearmanr(df["sig"].iloc[:mid], df["fwd_rel"].iloc[:mid])
    r2, _ = stats.spearmanr(df["sig"].iloc[mid:], df["fwd_rel"].iloc[mid:])

    lo, hi = df["sig"].quantile([EXTREME_LO, EXTREME_HI])
    hi_m, lo_m = df["sig"] >= hi, df["sig"] <= lo
    ext = df[hi_m | lo_m].copy()
    ext["dir_ok"] = np.where(ext["sig"] >= hi, ext["fwd_rel"] > 0, ext["fwd_rel"] < 0)
    # episode = 极端档内的连续段
    flag = (hi_m.astype(int) - lo_m.astype(int))
    epi = int((flag.ne(flag.shift()) & flag.ne(0)).sum())
    return {
        "信号": label,
        "n": len(df),
        "ρ全样本": round(rho, 3), "p": round(p, 4),
        "ρ前半": round(r1, 3), "ρ后半": round(r2, 3),
        "分半同号": bool(np.sign(r1) == np.sign(r2) and r1 != 0),
        "方向准确率": f"{ext['dir_ok'].mean():.1%}" if len(ext) else "—",
        "极端档观测": len(ext), "独立episode": epi,
        "判据①": "过" if abs(rho) >= RHO_MIN and np.sign(r1) == np.sign(r2) else "未过",
        "判据②": "过" if len(ext) and ext["dir_ok"].mean() >= ACC_MIN and epi >= EPISODE_MIN else "未过",
    }


def median_episodes(v: pd.DataFrame, freq: str, n_rep: int) -> int:
    """给定信号更新频率，安慰剂信号能产生的独立 episode 数中位数（判据②的结构上限）。"""
    rng = np.random.default_rng(SEED)
    if freq == "D":
        keys = pd.Index(range(len(v)))
    else:
        keys = pd.PeriodIndex(v["date"], freq=freq)
    uk = pd.Index(keys).unique()
    epis = []
    for _ in range(n_rep):
        m = dict(zip(uk, rng.normal(size=len(uk))))
        sig = pd.Series([m[k] for k in keys], index=v.index)
        lo, hi = sig.quantile([EXTREME_LO, EXTREME_HI])
        flag = (sig >= hi).astype(int) - (sig <= lo).astype(int)
        epis.append(int((flag.ne(flag.shift()) & flag.ne(0)).sum()))
    return int(np.median(epis))


def placebo_fpr(targets: dict[str, pd.DataFrame], n_rep: int) -> pd.DataFrame:
    """假阳性率：反复抽取**无信息**的季频阶跃白噪声，看它多大比例通过 E34 判据①②。

    这是对 E34 判据本身（在日频重叠样本上评估季频信号）的校准检验，
    与候选信号内容无关。
    """
    rows = []
    for name, tgt in targets.items():
        v = tgt.dropna(subset=["fwd_rel"])
        q = pd.PeriodIndex(v["date"], freq="Q")
        uq = q.unique()
        rng = np.random.default_rng(SEED)
        c1_rho = c1_full = c2_acc = c2_full = both = 0
        epis = []
        for _ in range(n_rep):
            qmap = dict(zip(uq, rng.normal(size=len(uq))))
            sig = pd.Series([qmap[p] for p in q], index=v.index)
            r = eval_signal(sig, tgt, "placebo")
            if abs(r["ρ全样本"]) >= RHO_MIN:
                c1_rho += 1
            p1 = r["判据①"] == "过"
            if p1:
                c1_full += 1
            acc = float(r["方向准确率"].rstrip("%")) / 100
            if acc >= ACC_MIN:
                c2_acc += 1
            p2 = r["判据②"] == "过"
            if p2:
                c2_full += 1
            if p1 and p2:
                both += 1
            epis.append(r["独立episode"])
        rows.append({
            "腿": name,
            "|ρ|≥0.15 比例": f"{c1_rho / n_rep:.1%}",
            "判据①(含分半同号)通过率": f"{c1_full / n_rep:.1%}",
            "准确率≥58% 比例": f"{c2_acc / n_rep:.1%}",
            "判据②(含episode≥30)通过率": f"{c2_full / n_rep:.1%}",
            "①②同时通过率": f"{both / n_rep:.1%}",
            "episode 中位数": int(np.median(epis)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- 组合基线（判据③用的等权基线）
@dataclass
class BTResult:
    ann: float
    vol: float
    sharpe: float
    mdd: float


def equal_weight_baseline(px: pd.DataFrame, legs: list[str], exec_lag: int = 1) -> BTResult:
    """四腿等权 25% + 年度再平衡 + exec_lag=1 + 闲置现金 2% + 日频回撤。

    仅作基线参考：无信号时四腿始终满仓等权，闲置现金份额为 0。
    """
    sub = px[legs].dropna()
    rets = sub.pct_change().fillna(0.0)
    dates = pd.to_datetime(sub.index.astype(str), format="%Y%m%d")
    w = np.full(len(legs), 1.0 / len(legs))
    nav, navs = 100.0, []
    year = dates[0].year
    pending = None
    for i in range(len(sub)):
        if pending is not None and i >= pending:      # exec_lag=1：次日生效
            w = np.full(len(legs), 1.0 / len(legs))
            pending = None
        r = rets.iloc[i].values
        w = w * (1 + r)
        tot = w.sum()
        nav *= tot
        w = w / tot
        navs.append(nav)
        if dates[i].year != year:                     # 年度再平衡（下一日执行）
            year = dates[i].year
            pending = i + exec_lag
    nav_s = pd.Series(navs, index=dates)
    yrs = (dates[-1] - dates[0]).days / 365.25
    ann = (nav_s.iloc[-1] / 100.0) ** (1 / yrs) - 1
    dr = nav_s.pct_change().dropna()
    vol = dr.std() * np.sqrt(252)
    mdd = (nav_s / nav_s.cummax() - 1).min()          # 日频回撤
    return BTResult(ann, vol, (ann - RF) / vol if vol else np.nan, mdd)


def main() -> None:
    px = load_legs()
    print("=" * 78)
    print("E34 / R2 —— 被解释变量刻画 · 统计功效审计 · harness 自检")
    print("=" * 78)

    targets = {"创业板−红利": forward_rel(px, "chinext"),
               "科创50−红利": forward_rel(px, "star50")}

    print("\n【A】被解释变量：未来 60 交易日「成长 − 价值」相对收益")
    print(pd.DataFrame([describe_target(k, v) for k, v in targets.items()]).to_string(index=False))

    print("\n【B1】季频基本面信号的独立观测上限（先验，与信号内容无关）")
    rows = []
    for k, v in targets.items():
        r = {"腿": k}
        r.update(quarters_available(v))
        r["达判据②(≥30)"] = "可能" if r["20/80极端档后"] >= EPISODE_MIN else "结构性不可达"
        rows.append(r)
    qtab = pd.DataFrame(rows)
    print(qtab.to_string(index=False))

    print("\n【B2】在该独立样本量下，E34 判据①②离随机噪声多远")
    ns = sorted({int(r) for r in qtab["20/80极端档后"]} | {10, 20, 24, 30, 40, 60})
    print(power_table(ns).to_string(index=False))

    print("\n【C1】harness 自检：安慰剂信号（seed=%d，非候选信号）" % SEED)
    rng = np.random.default_rng(SEED)
    out = []
    for k, v in targets.items():
        idx = v.dropna(subset=["fwd_rel"]).index
        # 安慰剂 1：日频白噪声；安慰剂 2：季频阶跃白噪声（模拟真实季频信号的粒度）
        out.append(eval_signal(pd.Series(rng.normal(size=len(idx)), index=idx), v, f"{k} / 日频白噪声"))
        q = pd.PeriodIndex(v.loc[idx, "date"], freq="Q")
        qmap = {p: rng.normal() for p in q.unique()}
        out.append(eval_signal(pd.Series([qmap[p] for p in q], index=idx), v, f"{k} / 季频阶跃白噪声"))
    print(pd.DataFrame(out).to_string(index=False))
    print("  ↑ 安慰剂应落在 ρ≈0、准确率≈50% 附近；注意日频白噪声的『极端档观测』数被重叠严重虚增，")
    print("    正是 E34 判据②用『独立 episode≥30』而非『观测数』把关的原因。")

    print("\n【C1b】判据①②的假阳性率：%d 次季频阶跃白噪声重抽（信号无任何信息）" % N_PLACEBO)
    print(placebo_fpr(targets, N_PLACEBO).to_string(index=False))
    print("  ↑ 若纯噪声也能高比例通过判据①，说明『日频重叠样本上算 Spearman』把有效自由度")
    print("    从 ~%d 个季度虚增到数千个交易日，p 值与分半同号都不再有把关意义。" % 61)

    print("\n【B3】判据②的 episode 门槛需要多高的信号更新频率（安慰剂 %d 次重抽的 episode 中位数）" % N_PLACEBO)
    frows = []
    for k, v in targets.items():
        vv = v.dropna(subset=["fwd_rel"])
        row = {"腿": k}
        for fname, freq in [("季频(R2 基本面)", "Q"), ("月频(宏观/景气)", "M"),
                            ("周频", "W"), ("日频(资金/情绪)", "D")]:
            row[fname] = median_episodes(vv, freq, N_PLACEBO)
        frows.append(row)
    ftab = pd.DataFrame(frows)
    print(ftab.to_string(index=False))
    print("  ↑ 判据②要求独立 episode ≥ %d。季频信号在本样本上先验达不到——与信号好坏无关。" % EPISODE_MIN)

    print("\n【C2】组合基线（判据③的对照基准，四腿等权 25%）")
    combos = {
        "四腿=红利/创业板/科创50/沪深300（科创50 起点约束）":
            ["dividend", "chinext", "star50", "hs300"],
        "三腿=红利/创业板/沪深300（长样本参考）":
            ["dividend", "chinext", "hs300"],
    }
    brows = []
    for name, legs in combos.items():
        sub = px[legs].dropna()
        b = equal_weight_baseline(px, legs)
        brows.append({"组合": name, "起": int(sub.index.min()), "止": int(sub.index.max()),
                      "年化%": round(b.ann * 100, 2), "波动%": round(b.vol * 100, 2),
                      "夏普": round(b.sharpe, 3), "最大回撤%": round(b.mdd * 100, 2)})
    print(pd.DataFrame(brows).to_string(index=False))
    print("\n注：判据③需要候选信号才能评估；本表仅提供等权基线数值，供 R1/R3 与后续复跑对照。")
    print("=" * 78)


if __name__ == "__main__":
    main()
