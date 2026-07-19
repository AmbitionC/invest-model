"""模型防御端区分力验证（owner 命题 2026-07-18·判据跑数前写死·只读不落库）。

背景：周度复盘按"月度调仓区间总收益"评模型（多空价差 -0.6%），结论"宜降权"；
但 0717 崩盘日持仓抗跌性与模型排位完美单调（n=6）。两者可并存：模型可能是
防御型参谋（识别谁会跌）而非进攻型（识别谁会涨）。本验证用全历史数据裁决。

口径（预登记）：
  - 预测：model_prediction 最新 version 的全部 (trade_date, code, rank_pct)；
    对每个自然交易日 t，用"≤t 的最近一次预测"（陈旧超过 40 个自然日剔除该日）。
  - 收益：stock_daily 当日 pct_chg（未复权日涨跌，截面比较除权噪声可忽略且对组间等同）。
  - 市况分桶（按沪深300 当日涨跌）：大跌日 ≤-1.5% ｜ 大涨日 ≥+1.5% ｜ 其它。
  - 每日两个统计：① 分位组差 spread = 高分组(rank_pct≥0.8) 当日均收益 −
    低分组(rank_pct≤0.2) 当日均收益；② 截面 Spearman(rank_pct, 当日收益)。

判据（跑数前写死，勿按结果回调）：
  ① 大跌日样本 ≥15 天（不足不判定）；
  ② 大跌日 spread 日均 ≥ +0.30pp 且 spread>0 的天数占比 ≥60%；
  ③ 大跌日按时间分半，两半 spread 均值方向一致。
  三条全过 → 防御端区分力证实：复盘模型评价改为"进攻/防御分列"、
  参谋异议行维持加权使用；未过 → 如实记录，参谋异议行降级为普通展示、
  复盘"宜降权"结论照旧。
  （对照信息：同表输出大涨日/其它日 spread——若模型只在大跌日为正、
   大涨日为负，即"防御型参谋"画像的完整证据，仅作解读不入判据。）

用法（Actions）：python scripts/analysis/model_defense_validation.py [--out ...]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

BENCH = "000300.SH"
DOWN_TH, UP_TH = -0.015, 0.015
HI, LO = 0.8, 0.2
STALE_DAYS = 40


def main() -> None:
    ap = argparse.ArgumentParser(description="模型防御端区分力验证")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db))

    ver = repo.read_sql(
        "SELECT version, COUNT(*) n FROM model_prediction GROUP BY version "
        "ORDER BY MAX(trade_date) DESC, n DESC LIMIT 1")
    if ver.empty:
        print("⚠️ 无模型预测数据")
        return
    version = str(ver["version"].iloc[0])
    pred_dates = [str(x) for x in repo.read_sql(
        "SELECT DISTINCT trade_date FROM model_prediction WHERE version=:v ORDER BY trade_date",
        {"v": version})["trade_date"]]

    idx = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c AND trade_date>=:s "
        "ORDER BY trade_date", {"c": BENCH, "s": pred_dates[0]})
    idx_close = pd.Series(pd.to_numeric(idx["close"], errors="coerce").values,
                          index=idx["trade_date"].astype(str)).dropna()
    idx_ret = idx_close.pct_change().dropna()

    rows = []
    pred_cache_date, pred_cache = None, None
    for t, bret in idx_ret.items():
        use = [d for d in pred_dates if d <= t]
        if not use:
            continue
        pd_used = use[-1]
        if (pd.Timestamp(t) - pd.Timestamp(pd_used)).days > STALE_DAYS:
            continue
        if pd_used != pred_cache_date:
            pred_cache = repo.read_sql(
                "SELECT code, rank_pct FROM model_prediction WHERE version=:v AND trade_date=:d",
                {"v": version, "d": pd_used})
            pred_cache["rank_pct"] = pd.to_numeric(pred_cache["rank_pct"], errors="coerce")
            pred_cache = pred_cache.dropna()
            pred_cache_date = pd_used
        day = repo.read_sql(
            "SELECT code, pct_chg FROM stock_daily WHERE trade_date=:d", {"d": t})
        if day.empty:
            continue
        day["pct_chg"] = pd.to_numeric(day["pct_chg"], errors="coerce")
        m = pred_cache.merge(day.dropna(), on="code")
        if len(m) < 300:
            continue
        hi = m[m["rank_pct"] >= HI]["pct_chg"]
        lo = m[m["rank_pct"] <= LO]["pct_chg"]
        if len(hi) < 30 or len(lo) < 30:
            continue
        spread = float(hi.mean() - lo.mean())          # 单位：百分点（pct_chg 本身是百分数）
        # Spearman = 双侧秩后的 Pearson（避免引入 scipy 依赖）
        ic = float(m["rank_pct"].rank().corr(m["pct_chg"].rank()))
        bucket = "大跌日" if bret <= DOWN_TH else ("大涨日" if bret >= UP_TH else "其它")
        rows.append({"date": t, "bucket": bucket, "bench": bret,
                     "spread": spread, "ic": ic})

    df = pd.DataFrame(rows)
    L = [f"## 模型防御端区分力验证（version={version}，判据预登记）", "",
         f"- 有效交易日 {len(df)}（预测陈旧>{STALE_DAYS}天剔除）；"
         f"分组=模型分位 前20%(rank≥{HI:.0%}) vs 后20%(rank≤{LO:.0%})；"
         f"spread 单位为百分点/日", ""]
    if df.empty:
        L.append("⚠️ 无有效样本")
    else:
        L += ["| 市况桶（沪深300当日） | 天数 | 高−低组日均差 | 差>0 天数占比 | 日均截面IC |",
              "|---|---:|---:|---:|---:|"]
        for b in ("大跌日", "大涨日", "其它"):
            sub = df[df["bucket"] == b]
            if sub.empty:
                continue
            L.append(f"| {b} | {len(sub)} | {sub['spread'].mean():+.2f}pp "
                     f"| {(sub['spread'] > 0).mean():.0%} | {sub['ic'].mean():+.3f} |")
        dn = df[df["bucket"] == "大跌日"].sort_values("date")
        c1 = len(dn) >= 15
        c2 = bool(len(dn)) and dn["spread"].mean() >= 0.30 and (dn["spread"] > 0).mean() >= 0.60
        mid = len(dn) // 2
        c3 = len(dn) >= 2 and dn["spread"][:mid].mean() * dn["spread"][mid:].mean() > 0
        L += ["", "## 预登记判据裁决", "",
              "| 判据 | 实测 | 门槛 | 结论 |", "|---|---|---|---|",
              f"| ① 大跌日样本 | {len(dn)} | ≥15 | {'✅' if c1 else '❌'} |",
              f"| ② 大跌日日均差/为正占比 | "
              f"{dn['spread'].mean():+.2f}pp / {(dn['spread'] > 0).mean():.0%} | "
              f"≥+0.30pp 且 ≥60% | {'✅' if c2 else '❌'} |" if len(dn) else "| ② | 无样本 | — | ❌ |",
              f"| ③ 大跌日分半方向一致 | 前半 {dn['spread'][:mid].mean():+.2f} / "
              f"后半 {dn['spread'][mid:].mean():+.2f} | 同号 | {'✅' if c3 else '❌'} |"
              if len(dn) >= 2 else "| ③ | 样本不足 | — | ❌ |",
              "",
              ("**三条全过 → 防御端区分力证实：复盘模型评价改进攻/防御分列，参谋异议行维持加权使用**"
               if all([c1, c2, c3]) else
               "**未全过 → 如实记录：参谋异议行降级为普通展示，复盘『宜降权』结论维持**")]
    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
