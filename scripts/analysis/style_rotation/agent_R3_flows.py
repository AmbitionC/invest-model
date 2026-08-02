#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
R3 资金流与拥挤度驱动 —— 风格轮动信号检验（P36 / E34）

【前置约束遵守说明】
- 本脚本所有信号均来自「资金面 / 拥挤度」：ETF 份额净申购、两融余额、成交额占比、换手、恐慌。
  不含任何价格或价格比值（RS / 点差 / 相对强弱分位）的变换 —— E33 / E20 已证伪那一族。
- 被解释变量固定为 BRIEF 写死口径：未来 60 个交易日「成长 − 价值」相对收益，
  成长腿 = 创业板 chinext / 科创50 star50，价值腿 = 中证红利 000922。

【所有定义在跑数前写死，见 SPEC 常量区，跑完不回头改】
"""
from __future__ import annotations
import itertools
import json
import os
import numpy as np
import pandas as pd
from scipy import stats

BASE = os.path.dirname(os.path.abspath(__file__))
END_DATE = 20260729          # BRIEF 统一终点
FWD = 60                     # 未来 60 个交易日
EXTREME_Q = 0.20             # 判据②的 20/80 极端分位
EPISODE_GAP = 20             # 同侧触发日间隔 < 20 交易日视为同一 episode

# ────────────────────────────────────────────────────────────────────
# SPEC-1  ETF 归类（跑数前写死，跑完不得调整）
# ────────────────────────────────────────────────────────────────────
# 本目录 fund_share/fund_close 篮子里的 8 只 ETF，全部归类如下：
GROWTH_ETF = ["159915.SZ",   # 易方达创业板 ETF        —— 双创
              "159949.SZ",   # 华安创业板 50 ETF        —— 双创
              "588000.SH"]   # 华夏科创 50 ETF          —— 双创（2020-09 起）
VALUE_ETF  = ["510050.SH"]   # 华夏上证 50 ETF —— 大盘蓝筹「代用价值腿」
                             #   ⚠ 篮子里没有任何红利 ETF（510880/512890/515180 均不在），
                             #     tushare 拉取通道 token 已过期不可用 → 红利 ETF 份额流向不可得。
                             #     此处以上证50 作「代用腿」，结论明确标注为代用，不冒充红利。
NEUTRAL_ETF = ["510300.SH", "159919.SZ",   # 沪深300（两只）—— 宽基，不计入任何一侧
               "510500.SH",                # 中证500       —— 中盘，不计入
               "512100.SH"]                # 中证1000      —— 小盘，不计入

# ────────────────────────────────────────────────────────────────────
# SPEC-2  数据可得日滞后（写死）
# ────────────────────────────────────────────────────────────────────
LAG_SHARE  = 2   # ETF 份额 T+1~T+2 披露 → 统一按 T+2 才可用
LAG_MARGIN = 1   # 两融余额当日盘后公布 → T+1 才可用
LAG_AMT    = 0   # 成交额/换手/恐慌为当日收盘可得

# ────────────────────────────────────────────────────────────────────
# SPEC-3  份额数据清洗（写死）
# ────────────────────────────────────────────────────────────────────
# 份额折算（拆分）识别：单日 |Δshare/share| > 20% 且 |Δclose/close| > 20% 且两者反号
# → 判为份额折算而非申赎，当日净申购记 0。除此之外不做任何截尾/缩尾。
SPLIT_SHARE_TH = 0.20
SPLIT_PRICE_TH = 0.20

# ────────────────────────────────────────────────────────────────────
# SPEC-4  候选信号（跑数前写死定义与滚动窗）
# ────────────────────────────────────────────────────────────────────
MAIN_WIN = 20        # 主窗口；判据④邻域扫描 {10, 40}
NEIGH_WINS = [10, 40]
QUANT_WIN = 250      # 滚动分位窗（用于 F3/F5，复刻 E24 v2 构造以做对照）

SIGNALS_DOC = {
    "F1_etf_flow_diff": "双创ETF 20日净申购率 − 上证50ETF 20日净申购率（净申购金额/期初AUM），lag2",
    "F1g_etf_flow_growth": "双创ETF 20日净申购率（单腿，无代用价值腿），lag2",
    "F2_margin_mom": "全市场两融余额 20日对数变化，lag1",
    "F2b_margin_ratio_chg": "两融余额/流通市值 20日差分，lag1",
    "F3_dual_ratio_q250": "双创成交额占比 250日滚动分位（= E24 v2 被证伪的构造，相对维度重测）",
    "F4_dual_ratio_dev": "双创成交额占比 − 其60日均值（拥挤度变化，新构造）",
    "F5_turnover_q250": "全市场换手率20日均 的 250日滚动分位",
    "F6_fear": "恐慌指数（水平，0-100）",
    "F6d_fear_chg": "恐慌指数 20日差分",
}

# ────────────────────────────────────────────────────────────────────
# SPEC-5  方向判定协议（防 p-hacking，写死）
# ────────────────────────────────────────────────────────────────────
# 判据②的「预测方向」不许跑完看结果再定。两种口径都报：
#   (a) 全样本符号（in-sample，乐观上界，对应 BRIEF 字面口径）
#   (b) 前半段定符号 → 只在后半段计准确率（样本外，诚实口径）
# 判定以 (a) 为准（BRIEF 口径），但 (b) 与 (a) 背离时在报告中明示。


# ═══════════════════════════ 数据加载 ═══════════════════════════
def _rd(name, **kw):
    df = pd.read_csv(os.path.join(BASE, name), **kw)
    return df


def load_all():
    div = _rd("000922_csi.csv").rename(columns={"close": "div"})
    spr = _rd("spread_full.csv")[["trade_date", "chinext"]]
    star = _rd("star50.csv").rename(columns={"close": "star"})
    hs = _rd("hs300.csv").rename(columns={"close": "hs300"})
    crowd = _rd("crowding_daily.csv")
    fear = _rd("fear_daily_dump.csv").rename(columns={"score": "fear"})

    px = div.merge(spr, on="trade_date", how="outer") \
            .merge(star, on="trade_date", how="outer") \
            .merge(hs, on="trade_date", how="outer")
    px = px[px.trade_date <= END_DATE].sort_values("trade_date").reset_index(drop=True)

    sh = _rd("fund_share_dump.csv")
    cl = _rd("fund_close_dump.csv")
    etf = sh.merge(cl, on=["code", "trade_date"], how="inner") \
            .sort_values(["code", "trade_date"]).reset_index(drop=True)
    etf = etf[etf.trade_date <= END_DATE]
    return px, crowd, fear, etf


def etf_basket_flow(etf: pd.DataFrame, codes: list[str]) -> pd.DataFrame:
    """篮子日频净申购金额（亿元）与篮子 AUM（亿元）。fd_share 单位万份，close 元/份。"""
    d = etf[etf.code.isin(codes)].copy()
    d["dsh"] = d.groupby("code")["fd_share"].diff()
    d["gsh"] = d.groupby("code")["fd_share"].pct_change()
    d["gpx"] = d.groupby("code")["close"].pct_change()
    split = (d.gsh.abs() > SPLIT_SHARE_TH) & (d.gpx.abs() > SPLIT_PRICE_TH) & (d.gsh * d.gpx < 0)
    d.loc[split, "dsh"] = 0.0
    d["_split"] = split
    # 净申购金额：Δ份额(万份) × 收盘价(元) = 万元 → /1e4 = 亿元
    d["flow_yi"] = d["dsh"] * d["close"] / 1e4
    d["aum_yi"] = d["fd_share"] * d["close"] / 1e4
    g = d.groupby("trade_date").agg(flow_yi=("flow_yi", "sum"),
                                    aum_yi=("aum_yi", "sum"),
                                    n_split=("_split", "sum")).reset_index()
    return g


# ═══════════════════════════ 信号构造 ═══════════════════════════
def build_signals(px, crowd, fear, etf, win=MAIN_WIN):
    cal = px[["trade_date"]].copy()

    gf = etf_basket_flow(etf, GROWTH_ETF).rename(
        columns={"flow_yi": "g_flow", "aum_yi": "g_aum", "n_split": "g_split"})
    vf = etf_basket_flow(etf, VALUE_ETF).rename(
        columns={"flow_yi": "v_flow", "aum_yi": "v_aum", "n_split": "v_split"})
    ef = gf.merge(vf, on="trade_date", how="outer").sort_values("trade_date")
    # 20 日累计净申购 / 期初 AUM
    for side in ("g", "v"):
        ef[f"{side}_f{win}"] = ef[f"{side}_flow"].rolling(win).sum()
        ef[f"{side}_rate"] = ef[f"{side}_f{win}"] / ef[f"{side}_aum"].shift(win)
    ef["F1_etf_flow_diff"] = ef["g_rate"] - ef["v_rate"]
    ef["F1g_etf_flow_growth"] = ef["g_rate"]
    ef_use = ef[["trade_date", "F1_etf_flow_diff", "F1g_etf_flow_growth"]].copy()
    # 可得日滞后
    for c in ("F1_etf_flow_diff", "F1g_etf_flow_growth"):
        ef_use[c] = ef_use[c].shift(LAG_SHARE)

    cw = crowd.sort_values("trade_date").copy()
    cw["F2_margin_mom"] = np.log(cw.rzye_yi).diff(win)
    cw["F2b_margin_ratio_chg"] = cw.margin_ratio.diff(win)
    for c in ("F2_margin_mom", "F2b_margin_ratio_chg"):
        cw[c] = cw[c].shift(LAG_MARGIN)
    cw["F3_dual_ratio_q250"] = cw.dual_ratio.rolling(QUANT_WIN).rank(pct=True)
    cw["F4_dual_ratio_dev"] = cw.dual_ratio - cw.dual_ratio.rolling(60).mean()
    cw["F5_turnover_q250"] = cw.turnover.rolling(win).mean().rolling(QUANT_WIN).rank(pct=True)
    cw_use = cw[["trade_date", "F2_margin_mom", "F2b_margin_ratio_chg",
                 "F3_dual_ratio_q250", "F4_dual_ratio_dev", "F5_turnover_q250"]]

    fr = fear.sort_values("trade_date").copy()
    fr["F6_fear"] = fr.fear
    fr["F6d_fear_chg"] = fr.fear.diff(win)
    fr_use = fr[["trade_date", "F6_fear", "F6d_fear_chg"]]

    sig = cal.merge(ef_use, on="trade_date", how="left") \
             .merge(cw_use, on="trade_date", how="left") \
             .merge(fr_use, on="trade_date", how="left")
    return sig, ef


def build_targets(px):
    t = px.copy()
    for leg, col in (("chinext", "chinext"), ("star", "star")):
        t[f"fwd_{leg}"] = t[col].shift(-FWD) / t[col] - 1
    t["fwd_div"] = t["div"].shift(-FWD) / t["div"] - 1
    t["rel_chinext"] = t["fwd_chinext"] - t["fwd_div"]
    t["rel_star"] = t["fwd_star"] - t["fwd_div"]
    return t[["trade_date", "rel_chinext", "rel_star"]]


# ═══════════════════════════ 评估 ═══════════════════════════
def episodes(mask: pd.Series, gap=EPISODE_GAP) -> int:
    """连续/近邻触发日归并为一个 episode（间隔 < gap 个观测日视为同一段）。"""
    idx = np.flatnonzero(mask.values)
    if idx.size == 0:
        return 0
    return int(1 + (np.diff(idx) >= gap).sum())


def eval_signal(df: pd.DataFrame, sc: str, tc: str, q=EXTREME_Q) -> dict:
    d = df[["trade_date", sc, tc]].dropna()
    if len(d) < 300:
        return {"signal": sc, "target": tc, "n": len(d), "note": "样本不足"}
    s, y = d[sc].values, d[tc].values
    rho, p = stats.spearmanr(s, y)
    half = len(d) // 2
    r1, p1 = stats.spearmanr(s[:half], y[:half])
    r2, p2 = stats.spearmanr(s[half:], y[half:])
    # 非重叠子样本（每 60 个观测取一个）以缓解重叠窗自相关
    rho_nl, p_nl = stats.spearmanr(s[::FWD], y[::FWD])

    lo, hi = np.quantile(s, q), np.quantile(s, 1 - q)
    m_lo, m_hi = d[sc] <= lo, d[sc] >= hi
    # (a) 全样本符号口径
    sign_full = 1 if rho >= 0 else -1
    pred = np.where(m_hi, sign_full, np.where(m_lo, -sign_full, 0))
    ext = pred != 0
    acc_full = float((np.sign(y[ext]) == pred[ext]).mean()) if ext.sum() else np.nan
    # (b) 前半定符号 → 后半算准确率
    sign_h1 = 1 if r1 >= 0 else -1
    ext2 = ext.copy(); ext2[:half] = False
    pred2 = np.where(m_hi, sign_h1, np.where(m_lo, -sign_h1, 0))
    acc_oos = float((np.sign(y[ext2]) == pred2[ext2]).mean()) if ext2.sum() else np.nan

    n_ep = episodes(m_lo) + episodes(m_hi)
    return {"signal": sc, "target": tc, "n": len(d),
            "start": int(d.trade_date.iloc[0]), "end": int(d.trade_date.iloc[-1]),
            "rho": round(float(rho), 4), "p": round(float(p), 5),
            "rho_h1": round(float(r1), 4), "rho_h2": round(float(r2), 4),
            "same_sign": bool(np.sign(r1) == np.sign(r2)),
            "rho_nonoverlap": round(float(rho_nl), 4), "p_nonoverlap": round(float(p_nl), 5),
            "n_extreme": int(ext.sum()), "episodes": n_ep,
            "acc_full": None if np.isnan(acc_full) else round(acc_full, 4),
            "acc_oos_h2": None if np.isnan(acc_oos) else round(acc_oos, 4),
            "c1_rho": bool(abs(rho) >= 0.15 and np.sign(r1) == np.sign(r2)),
            "c2_acc": bool((acc_full or 0) >= 0.58 and n_ep >= 30)}


def run(win=MAIN_WIN, q=EXTREME_Q, tag="main"):
    px, crowd, fear, etf = load_all()
    sig, ef = build_signals(px, crowd, fear, etf, win=win)
    tgt = build_targets(px)
    df = sig.merge(tgt, on="trade_date", how="inner")
    rows = []
    for sc in SIGNALS_DOC:
        for tc in ("rel_chinext", "rel_star"):
            rows.append(eval_signal(df, sc, tc, q=q))
    res = pd.DataFrame(rows)
    res["win"] = win; res["q"] = q; res["tag"] = tag
    return res, df, ef


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    all_res = []
    res, df, ef = run()
    all_res.append(res)
    cols = ["signal", "target", "n", "start", "end", "rho", "p", "rho_h1", "rho_h2",
            "same_sign", "rho_nonoverlap", "episodes", "acc_full", "acc_oos_h2",
            "c1_rho", "c2_acc"]
    print("=" * 120)
    print("主口径 win=20, q=0.20")
    print(res[cols].to_string(index=False))

    # 判据④：参数邻域
    for w in NEIGH_WINS:
        r, _, _ = run(win=w, tag=f"win{w}")
        all_res.append(r)
        print(f"\n--- 邻域 win={w} ---")
        print(r[["signal", "target", "rho", "rho_h1", "rho_h2", "same_sign",
                 "episodes", "acc_full"]].to_string(index=False))
    for qq in (0.10, 0.30):
        r, _, _ = run(q=qq, tag=f"q{qq}")
        all_res.append(r)
        print(f"\n--- 邻域 q={qq} ---")
        print(r[["signal", "target", "episodes", "acc_full", "acc_oos_h2"]].to_string(index=False))

    out = pd.concat(all_res, ignore_index=True)
    out.to_csv(os.path.join(BASE, "agent_R3_results.csv"), index=False)
    df.to_csv(os.path.join(BASE, "agent_R3_panel.csv"), index=False)
    ef.to_csv(os.path.join(BASE, "agent_R3_etf_flow.csv"), index=False)
    print("\n已写出 agent_R3_results.csv / agent_R3_panel.csv / agent_R3_etf_flow.csv")
