"""复盘取数审计（owner 命题 2026-07-25·只读不落库）：量化未复权价对复盘结论的污染。

背景：`stock_daily` 存未复权原始价（P11 注释明确），风控/计划链路已改用 stock_adj
前复权（invest_model/data/adjust.py），但 scripts/review.py 与 build_signal_scorecard.py
的收益计算（投顾分级战绩·模型分档前瞻·买点时效）仍用原始价——正值 6-7 月分红除权季，
分红 1-3% 系统性压低收益、送转直接假腰斩。本脚本对三段各算 raw vs qfq 两套数字：

  1) 投顾分级表（review 第一段口径逐字复刻）raw vs qfq 对照 + 受影响标的清单
  2) 模型分档前瞻（第二段口径）逐区间 spread raw vs qfq + 区间天数分布（检口径混杂）
  3) 买点时效（第四段）ref_price 入场（现状=前视）vs 次日收盘入场（与第一段一致）
  附) stock_adj 覆盖范围 · 同票跨级重复推荐计数

产物（供多 Agent 交叉验证，回提 master）：
  results/review_audit.md               审计报告
  results/review_audit_signals.csv      逐信号明细（含 raw/qfq 两套收益与复权比）
  results/review_audit_model.csv        逐调仓区间明细
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

VERSION = "ic_v1"


def _adj_on_dates(repo: BaseRepository, dates: list[str],
                  codes: list[str] | None = None) -> pd.DataFrame:
    """取给定交易日的复权因子 [code, trade_date, adj_factor]；无表返回空。"""
    if not repo.table_exists("stock_adj") or not dates:
        return pd.DataFrame(columns=["code", "trade_date", "adj_factor"])
    dph = ",".join(f":d{i}" for i in range(len(dates)))
    params = {f"d{i}": d for i, d in enumerate(dates)}
    sql = f"SELECT code, trade_date, adj_factor FROM stock_adj WHERE trade_date IN ({dph})"
    if codes:
        cph = ",".join(f":c{i}" for i in range(len(codes)))
        params.update({f"c{i}": c for i, c in enumerate(codes)})
        sql += f" AND code IN ({cph})"
    df = repo.read_sql(sql, params)
    if not df.empty:
        df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")
    return df


def _fac_lookup(adj: pd.DataFrame, dates: list[str]):
    """按 code 在给定日期序列上 ffill/bfill 的因子查询表：fac(code,date)->float|nan。"""
    if adj.empty:
        return lambda c, d: float("nan")
    piv = adj.pivot_table(index="code", columns="trade_date", values="adj_factor",
                          aggfunc="last")
    piv = piv.reindex(columns=sorted(set(dates))).ffill(axis=1).bfill(axis=1)

    def fac(c, d):
        try:
            return float(piv.at[c, d])
        except (KeyError, ValueError):
            return float("nan")
    return fac


def main() -> None:  # noqa: PLR0915
    repo = BaseRepository(make_engine())
    asof = str(repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0])
    L = [f"# 复盘取数审计 — 截至 {asof}", ""]

    # 附) stock_adj 覆盖
    cov = repo.read_sql("SELECT MIN(trade_date) a, MAX(trade_date) b, "
                        "COUNT(DISTINCT trade_date) nd, COUNT(DISTINCT code) nc FROM stock_adj") \
        if repo.table_exists("stock_adj") else pd.DataFrame()
    if not cov.empty:
        r = cov.iloc[0]
        L.append(f"- stock_adj 覆盖：{r['a']} ~ {r['b']}（{r['nd']} 个交易日 × {r['nc']} 只）")
    else:
        L.append("- ⚠️ 无 stock_adj 表——全部只能 raw 口径")

    # 1) 投顾分级 raw vs qfq（复刻 review_advisor 口径）
    reco = repo.read_sql("SELECT code, grade, rec_date FROM advisor_reco WHERE direction='long'")
    reco = reco.sort_values("rec_date").drop_duplicates("code", keep="first")
    dup = repo.read_sql("SELECT code, COUNT(DISTINCT grade) ng FROM advisor_reco "
                        "WHERE direction='long' GROUP BY code HAVING ng>1")
    codes = sorted(set(reco["code"]))
    cph = ",".join(f":c{i}" for i in range(len(codes)))
    win = repo.read_sql(
        f"SELECT code, trade_date, close FROM stock_daily WHERE code IN ({cph}) "
        f"AND trade_date>=:s AND trade_date<=:e",
        {**{f"c{i}": c for i, c in enumerate(codes)},
         "s": str(reco["rec_date"].min()), "e": asof})
    win["close"] = pd.to_numeric(win["close"], errors="coerce")
    win = win.dropna(subset=["close"])
    all_dates = sorted(win["trade_date"].astype(str).unique())
    fac = _fac_lookup(_adj_on_dates(repo, all_dates, codes), all_dates)

    sig_rows = []
    for _, r in reco.iterrows():
        c = r["code"]
        g = win[(win["code"] == c) & (win["trade_date"] > r["rec_date"])].sort_values("trade_date")
        if g.empty:
            continue
        e_d, e_px = str(g["trade_date"].iloc[0]), float(g["close"].iloc[0])
        l_d, l_px = str(g["trade_date"].iloc[-1]), float(g["close"].iloc[-1])
        if e_px <= 0:
            continue
        f0, f1 = fac(c, e_d), fac(c, l_d)
        ret_raw = l_px / e_px - 1
        adj_ratio = (f1 / f0) if np.isfinite(f0) and np.isfinite(f1) and f0 > 0 else 1.0
        ret_qfq = (l_px * (f1 if np.isfinite(f1) else 1.0)) / \
                  (e_px * (f0 if np.isfinite(f0) else 1.0)) - 1 \
            if np.isfinite(f0) and np.isfinite(f1) and f0 > 0 else ret_raw
        sig_rows.append({"code": c, "grade": r["grade"] or "?", "first": str(r["rec_date"]),
                         "entry_date": e_d, "last_date": l_d,
                         "entry_raw": e_px, "last_raw": l_px,
                         "ret_raw": ret_raw, "ret_qfq": ret_qfq,
                         "adj_ratio": adj_ratio,
                         "affected": abs(ret_qfq - ret_raw) > 0.001})
    sig = pd.DataFrame(sig_rows)
    L += ["", "## 一、投顾分级战绩 raw vs 前复权", "",
          f"- 样本 {len(sig)}；同票跨级重复推荐 {len(dup)} 只（首评桶口径下后续升级不体现）",
          f"- 受复权影响（|Δret|>0.1pp）：{int(sig['affected'].sum())} 只"
          f"；其中疑似送转/大额除权（复权比>1.05）：{int((sig['adj_ratio'] > 1.05).sum())} 只", "",
          "| 分级 | n | 均值raw | 均值qfq | Δ | 胜率raw | 胜率qfq |", "|---|---|---|---|---|---|---|"]
    for gr in ["A", "B", "C", "?", "ALL"]:
        sub = sig if gr == "ALL" else sig[sig["grade"] == gr]
        if sub.empty:
            continue
        L.append(f"| {gr} | {len(sub)} | {sub['ret_raw'].mean():+.1%} | {sub['ret_qfq'].mean():+.1%} "
                 f"| {(sub['ret_qfq'].mean()-sub['ret_raw'].mean())*100:+.2f}pp "
                 f"| {(sub['ret_raw']>0).mean():.0%} | {(sub['ret_qfq']>0).mean():.0%} |")
    worst = sig[sig["affected"]].copy()
    if not worst.empty:
        worst["d"] = (worst["ret_qfq"] - worst["ret_raw"]).abs()
        L += ["", "- 影响最大标的：" + "；".join(
            f"{r['code']}({r['grade']}) raw{r['ret_raw']:+.0%}→qfq{r['ret_qfq']:+.0%}"
            for _, r in worst.sort_values("d", ascending=False).head(8).iterrows())]

    # 2) 模型分档 raw vs qfq
    preds = repo.read_sql("SELECT trade_date, code, rank_pct FROM model_prediction "
                          "WHERE version=:v", {"v": VERSION})
    preds["rank_pct"] = pd.to_numeric(preds["rank_pct"], errors="coerce")
    pdates = sorted(preds["trade_date"].astype(str).unique())
    dph = ",".join(f":d{i}" for i in range(len(pdates)))
    closes = repo.read_sql(
        f"SELECT code, trade_date, close FROM stock_daily WHERE trade_date IN ({dph})",
        {f"d{i}": d for i, d in enumerate(pdates)})
    closes["close"] = pd.to_numeric(closes["close"], errors="coerce")
    adj2 = _adj_on_dates(repo, pdates)
    fpiv = adj2.pivot_table(index="code", columns="trade_date", values="adj_factor",
                            aggfunc="last").reindex(columns=pdates).ffill(axis=1).bfill(axis=1) \
        if not adj2.empty else pd.DataFrame()
    piv = closes.pivot_table(index="code", columns="trade_date", values="close", aggfunc="last")
    mrows = []
    for d, nxt in zip(pdates[:-1], pdates[1:]):
        if d not in piv.columns or nxt not in piv.columns:
            continue
        pr = preds[preds["trade_date"].astype(str) == d][["code", "rank_pct"]].dropna()
        fr = piv[nxt] / piv[d] - 1.0
        if not fpiv.empty:
            f = fpiv.reindex(piv.index)
            fq = (piv[nxt] * f[nxt]) / (piv[d] * f[d]) - 1.0
            fq = fq.where(f[nxt].notna() & f[d].notna(), fr)
        else:
            fq = fr
        m = pr.merge(fr.rename("raw").reset_index(), on="code") \
              .merge(fq.rename("qfq").reset_index(), on="code").dropna()
        if len(m) < 20:
            continue
        top, bot = m[m["rank_pct"] >= 0.8], m[m["rank_pct"] <= 0.2]
        ndays = pdates.index(nxt) - pdates.index(d)
        import datetime as _dt
        span = (_dt.datetime.strptime(nxt, "%Y%m%d") - _dt.datetime.strptime(d, "%Y%m%d")).days
        mrows.append({"d0": d, "d1": nxt, "cal_days": span,
                      "n": len(m), "n_affected": int((m["raw"] - m["qfq"]).abs().gt(1e-4).sum()),
                      "top_raw": top["raw"].mean(), "bot_raw": bot["raw"].mean(),
                      "spread_raw": top["raw"].mean() - bot["raw"].mean(),
                      "top_qfq": top["qfq"].mean(), "bot_qfq": bot["qfq"].mean(),
                      "spread_qfq": top["qfq"].mean() - bot["qfq"].mean()})
    mdf = pd.DataFrame(mrows)
    if not mdf.empty:
        L += ["", "## 二、模型分档前瞻 raw vs 前复权", "",
              f"- {len(mdf)} 个调仓区间；区间日历天数分布 min/中位/max = "
              f"{mdf['cal_days'].min()}/{int(mdf['cal_days'].median())}/{mdf['cal_days'].max()}"
              f"（>7 天区间 {int((mdf['cal_days'] > 7).sum())} 个——口径混杂：长短区间等权平均）",
              f"- 每区间受复权影响标的中位数：{int(mdf['n_affected'].median())} 只", "",
              "| 口径 | 高分档均值 | 低分档均值 | 多空价差 | 价差>0 区间占比 |", "|---|---|---|---|---|",
              f"| raw | {mdf['top_raw'].mean():+.2%} | {mdf['bot_raw'].mean():+.2%} "
              f"| **{mdf['spread_raw'].mean():+.2%}** | {(mdf['spread_raw']>0).mean():.0%} |",
              f"| qfq | {mdf['top_qfq'].mean():+.2%} | {mdf['bot_qfq'].mean():+.2%} "
              f"| **{mdf['spread_qfq'].mean():+.2%}** | {(mdf['spread_qfq']>0).mean():.0%} |"]

    # 3) 买点时效：ref_price 入场（现状） vs 次日收盘入场
    ap = repo.read_sql("SELECT plan_date, code, action, ref_price FROM action_plan "
                       "WHERE action IN ('buy','add')")
    brow = []
    if not ap.empty:
        bcodes = sorted(set(ap["code"]))
        bph = ",".join(f":c{i}" for i in range(len(bcodes)))
        bwin = repo.read_sql(
            f"SELECT code, trade_date, close FROM stock_daily WHERE code IN ({bph}) "
            f"AND trade_date>=:s AND trade_date<=:e",
            {**{f"c{i}": c for i, c in enumerate(bcodes)},
             "s": str(ap["plan_date"].min()), "e": asof})
        bwin["close"] = pd.to_numeric(bwin["close"], errors="coerce")
        bdates = sorted(bwin["trade_date"].astype(str).unique())
        bfac = _fac_lookup(_adj_on_dates(repo, bdates, bcodes), bdates)
        for _, r in ap.iterrows():
            c = r["code"]
            g = bwin[(bwin["code"] == c) & (bwin["trade_date"] > r["plan_date"])] \
                .sort_values("trade_date")
            cur = bwin[bwin["code"] == c].sort_values("trade_date")
            if cur.empty:
                continue
            l_d, l_px = str(cur["trade_date"].iloc[-1]), float(cur["close"].iloc[-1])
            ref = pd.to_numeric(r["ref_price"], errors="coerce")
            ret_ref = l_px / float(ref) - 1 if np.isfinite(ref) and ref > 0 else np.nan
            if g.empty:
                continue
            e_d, e_px = str(g["trade_date"].iloc[0]), float(g["close"].iloc[0])
            f0, f1 = bfac(c, e_d), bfac(c, l_d)
            ok = np.isfinite(f0) and np.isfinite(f1) and f0 > 0
            ret_nx = (l_px * (f1 if ok else 1)) / (e_px * (f0 if ok else 1)) - 1
            brow.append({"plan_date": str(r["plan_date"]), "code": c,
                         "ret_refprice": ret_ref, "ret_nextclose_qfq": ret_nx})
        b = pd.DataFrame(brow).dropna(subset=["ret_nextclose_qfq"])
        if not b.empty:
            rr = b["ret_refprice"].dropna()
            L += ["", "## 三、买点时效入场口径（第四段）", "",
                  f"- 历史 buy/add 信号 {len(b)} 条",
                  f"- 现状口径（ref_price=信号日参考价，前视）：均 {rr.mean():+.1%}，胜率 {(rr>0).mean():.0%}"
                  if len(rr) else "- 现状口径无有效 ref_price",
                  f"- 修正口径（次日收盘入场·前复权，与第一段一致）：均 {b['ret_nextclose_qfq'].mean():+.1%}，"
                  f"胜率 {(b['ret_nextclose_qfq']>0).mean():.0%}"]

    md = "\n".join(L)
    print(md, flush=True)
    out = Path("results")
    out.mkdir(exist_ok=True)
    (out / "review_audit.md").write_text(md, encoding="utf-8")
    sig.to_csv(out / "review_audit_signals.csv", index=False)
    if not mdf.empty:
        mdf.to_csv(out / "review_audit_model.csv", index=False)
    print("\n已写 results/review_audit.{md,signals.csv,model.csv}", flush=True)


if __name__ == "__main__":
    main()
