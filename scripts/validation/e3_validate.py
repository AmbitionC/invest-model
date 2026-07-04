"""E3：量化「验证层」对投顾票有没有信息量？

预登记：
  H0: 量化对投顾票打的「价值陷阱/冲突」标记（建仓日 rank_pct 后 20% 分位 或 趋势破位）
      **与后续超额无关**。
  指标: 被标记 vs 未标记的投顾票，其后续(10 交易日)基准相对超额差异（越负说明标记越有效）。
  过关: 被标记组后续超额**显著低于**未标记组 → 量化验证层有效、可纳入定权；否则仅作展示。

复用 model_prediction(rank_pct) + risk.trend_ok_close。只读。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common import (BaseRepository, close_panel, first_on_or_after, fwd_return,
                    index_returns, md_table, pct, summ, trade_calendar)
from invest_model.portfolio.risk import RiskConfig, trend_ok_close

H = 10
BENCH = "000300.SH"
VERSION = "ic_v1"


def run(repo: BaseRepository) -> str:
    L = ["## E3 —— 量化验证层信息量（价值陷阱/冲突标记）", ""]
    if not repo.table_exists("advisor_reco"):
        return "\n".join(L + ["**数据不足**：无 advisor_reco。"])
    if not repo.table_exists("model_prediction"):
        return "\n".join(L + ["**数据不足**：无 model_prediction（需先建模产出 rank_pct）。"])
    recos = repo.read_sql("SELECT rec_date, code FROM advisor_reco WHERE direction='long'")
    if recos.empty:
        return "\n".join(L + ["**数据不足**：无 long 推荐。"])
    recos["rec_date"] = recos["rec_date"].astype(str)
    recos = recos.drop_duplicates(["rec_date", "code"])

    cal = trade_calendar(repo)
    cal_idx = {d: i for i, d in enumerate(cal)}
    codes = sorted(recos["code"].unique())

    # rank_pct：取每票在建仓日当日或之前最近一次预测
    preds = repo.read_sql(
        "SELECT trade_date, code, rank_pct FROM model_prediction WHERE version=:v",
        {"v": VERSION})
    if preds.empty:
        return "\n".join(L + [f"**数据不足**：model_prediction(version={VERSION}) 为空。"])
    preds["trade_date"] = preds["trade_date"].astype(str)
    preds["rank_pct"] = pd.to_numeric(preds["rank_pct"], errors="coerce")

    # 行情面板：入场 + 出场 + 趋势预热
    need, rows = set(), []
    for _, r in recos.iterrows():
        e = first_on_or_after(cal, r["rec_date"])
        if e is None or e not in cal_idx:
            continue
        ei = cal_idx[e]
        if ei + H >= len(cal):
            continue
        x = cal[ei + H]
        i0 = max(0, ei - 70)
        need.update(cal[i0:ei + 1]); need.add(x)
        rows.append({"code": r["code"], "rec_date": r["rec_date"], "entry": e, "exit": x, "ei": ei})
    panel = close_panel(repo, codes, sorted(need))
    bench = index_returns(repo, [BENCH], sorted(need)).get(BENCH, {})

    cfg = RiskConfig(trend_ma=60)
    recs = []
    for row in rows:
        pc = panel.get(row["code"], {})
        ret = fwd_return(pc, row["entry"], row["exit"])
        if ret is None:
            continue
        br = fwd_return(bench, row["entry"], row["exit"])
        exc = (ret - br) if br is not None else None
        if exc is None:
            continue
        # rank_pct：建仓日或之前最近
        pr = preds[(preds["code"] == row["code"]) & (preds["trade_date"] <= row["entry"])]
        rank = float(pr.sort_values("trade_date")["rank_pct"].iloc[-1]) if not pr.empty else np.nan
        # 趋势破位
        pre = pd.Series({d: pc.get(d) for d in cal[max(0, row["ei"] - 70):row["ei"] + 1]}).dropna()
        trend_broken = (not trend_ok_close(pre, cfg)) if len(pre) > 5 else False
        low_rank = bool(np.isfinite(rank) and rank <= 0.20)
        recs.append({"exc": exc, "flag": bool(low_rank or trend_broken),
                     "low_rank": low_rank, "trend_broken": trend_broken})
    df = pd.DataFrame(recs)
    if df.empty or df["flag"].nunique() < 2:
        return "\n".join(L + ["**数据不足**：可匹配 rank_pct/趋势的样本过少或无对照。"])

    flagged = df[df["flag"]]["exc"].tolist()
    clean = df[~df["flag"]]["exc"].tolist()
    sf, sc = summ(flagged), summ(clean)
    L.append(f"- 样本：{len(df)} 只投顾票（{H} 日窗口，超额vs沪深300）；"
             f"被标记 {sf['n']} / 未标记 {sc['n']}。")
    L.append("")
    L.append(md_table(["组", "n", "均值超额", "中位", "胜率"],
                      [["被标记(价值陷阱/破位)", sf["n"], pct(sf["mean"]), pct(sf["median"]), f"{sf['win']*100:.0f}%"],
                       ["未标记", sc["n"], pct(sc["mean"]), pct(sc["median"]), f"{sc['win']*100:.0f}%"]]))
    diff = (sf["mean"] - sc["mean"]) if (sf["n"] and sc["n"]) else float("nan")
    # 简单两样本 t
    t = float("nan")
    if sf["n"] > 2 and sc["n"] > 2:
        a, b = np.array(flagged), np.array(clean)
        sp = np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))
        t = (a.mean() - b.mean()) / sp if sp > 0 else float("nan")
    passed = np.isfinite(diff) and diff < 0 and np.isfinite(t) and t < -1.5
    L += ["", "### 结论",
          f"- 标记组 − 未标记组 均值超额差 = {pct(diff)}（t≈{t:+.1f}）。",
          f"- {'**过关**：被标记票后续显著更差 → 验证层有信息量，纳入定权（P3）。' if passed else '**未过关/存疑**：标记与后续超额无显著负相关 → 验证层仅作展示、不参与权重。'}"]
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
