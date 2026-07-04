"""E6：投顾信号「因子化」+ 对量化模型的「增量信息」检验（只读，为融合决策提供依据）。

研究依据（深度调研）：
  - 分析师推荐/评级变动是可量化、可加权的独立增量信号（S&P Quantamental 2018：推荐修正
    月度多空胜率 ~77%；Asquith 2005：评级修正/盈预修正/目标价修正各带独立信息）。
  - 投顾信号与基本面因子**融合**优于任一单独（arXiv 2502.20489：两者都用 IR 最高 ~1.23）。
  - 分析师**技能**（历史准度）决定价值（高技能多空价差 5.74%）→ 战绩加权 conviction 有据。
  - 陷阱：题材扎堆高估显著性（用聚类稳健 t）、前视（严格 PIT / rec_date 之后才算收益）。

预登记：
  H0: 投顾立场信号（long/grade 打分，reduce/avoid/exit 转负）对前瞻收益**无预测力**，
      且**对量化 rank_pct 无增量**（partial IC ≈ 0）。
  指标: 立场信号 pooled rank-IC；控制 rank_pct 后的**增量偏 IC**；立场与 rank_pct 的相关（正交性）；
        战绩加权 vs 平权 立场的 IC 对比。聚类稳健口径。
  过关（→ 支持把投顾做成因子融合，走影子→回测A/B）:
      立场 IC 稳定 >0.02 且 **增量偏 IC >0.01**（对量化确有增量）→ 融合有据；
      否则 hold（投顾与量化冗余，或样本不足），如实记录。

**只读**：不写库、不改引擎、不进打分。仅出「该不该融合、怎么融合」的数据依据。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common import (BaseRepository, close_panel, cluster_robust_tstat, first_on_or_after,
                    fwd_return, index_returns, md_table, pct, summ, theme_of, trade_calendar)

HORIZONS = [5, 10]
VERSION = "ic_v1"
# 立场打分：投顾方向×分级 → 有符号强度
STANCE = {("long", "A"): 3.0, ("long", "B"): 2.0, ("long", "C"): 1.0,
          ("reduce", None): -1.0, ("avoid", None): -2.0, ("exit", None): -2.0}


def _stance(direction: str, grade: str) -> float:
    d = (direction or "").strip().lower()
    g = (grade or "").strip().upper()
    if d == "long":
        return STANCE.get(("long", g), 1.0)
    return STANCE.get((d, None), 0.0)


def _partial_ic(x: pd.Series, y: pd.Series, z: pd.Series) -> float:
    """控制 z 后 x 与 y 的秩偏相关（x 对 z 秩回归取残差再与 y 秩相关）。"""
    d = pd.DataFrame({"x": x, "y": y, "z": z}).dropna()
    if len(d) < 20 or d["z"].std() == 0 or d["x"].std() == 0:
        return float("nan")
    xr, yr, zr = d["x"].rank(), d["y"].rank(), d["z"].rank()
    b = np.polyfit(zr, xr, 1)
    resid = xr - (b[0] * zr + b[1])
    if resid.std() == 0:
        return float("nan")
    return float(np.corrcoef(resid, yr)[0, 1])


def run(repo: BaseRepository) -> str:
    L = ["## E6 —— 投顾信号因子化 + 对量化的增量信息（融合决策依据）", ""]
    if not repo.table_exists("advisor_reco"):
        return "\n".join(L + ["**数据不足**：无 advisor_reco。"])
    reco = repo.read_sql(
        "SELECT rec_date, code, source_type, grade, direction, catalyst, source "
        "FROM advisor_reco")
    if reco.empty:
        return "\n".join(L + ["**数据不足**：advisor_reco 为空。"])
    reco["rec_date"] = reco["rec_date"].astype(str)
    reco["stance"] = [_stance(d, g) for d, g in zip(reco["direction"], reco["grade"])]
    reco = reco[reco["stance"] != 0.0].drop_duplicates(["rec_date", "code"])
    reco["theme"] = reco["catalyst"].apply(theme_of)
    if reco.empty:
        return "\n".join(L + ["**数据不足**：无可用方向的投顾记录。"])

    cal = trade_calendar(repo)
    if not cal:
        return "\n".join(L + ["**数据不足**：stock_daily 无交易日。"])
    cal_idx = {d: i for i, d in enumerate(cal)}
    codes = sorted(reco["code"].unique())

    # 量化 rank_pct（建仓日或之前最近）
    preds = pd.DataFrame()
    if repo.table_exists("model_prediction"):
        preds = repo.read_sql(
            "SELECT trade_date, code, rank_pct FROM model_prediction WHERE version=:v", {"v": VERSION})
        if not preds.empty:
            preds["trade_date"] = preds["trade_date"].astype(str)
            preds["rank_pct"] = pd.to_numeric(preds["rank_pct"], errors="coerce")

    # 行情面板
    need, rows = set(), []
    for _, r in reco.iterrows():
        e = first_on_or_after(cal, r["rec_date"])
        if e is None or e not in cal_idx:
            continue
        ei = cal_idx[e]
        exits = {h: cal[ei + h] for h in HORIZONS if ei + h < len(cal)}
        need.add(e); need.update(exits.values())
        rows.append({**r.to_dict(), "entry": e, "exits": exits})
    panel = close_panel(repo, codes, sorted(need))
    bench = index_returns(repo, ["000300.SH"], sorted(need)).get("000300.SH", {})

    # 组装观测
    obs = []
    for row in rows:
        e = row["entry"]
        rank = np.nan
        if not preds.empty:
            pr = preds[(preds["code"] == row["code"]) & (preds["trade_date"] <= e)]
            if not pr.empty:
                rank = float(pr.sort_values("trade_date")["rank_pct"].iloc[-1])
        for h, x in row["exits"].items():
            ret = fwd_return(panel.get(row["code"], {}), e, x)
            if ret is None:
                continue
            br = fwd_return(bench, e, x)
            exc = (ret - br) if br is not None else ret
            obs.append({"h": h, "rec_date": row["rec_date"], "theme": row["theme"],
                        "source": row["source"], "stance": row["stance"],
                        "rank": rank, "exc": exc})
    df = pd.DataFrame(obs)
    if df.empty:
        return "\n".join(L + ["**数据不足**：无法匹配行情。"])

    span = f"{reco['rec_date'].min()}~{reco['rec_date'].max()}"
    L.append(f"- 样本：{len(reco)} 条有向投顾（{span}），有 rank_pct 匹配的比例 "
             f"{df['rank'].notna().mean()*100:.0f}%。口径：超额vs沪深300、聚类稳健(主题×rec_date)。")

    rows_out, verdicts = [], []
    for h in HORIZONS:
        sub = df[df["h"] == h]
        if len(sub) < 20:
            continue
        ic = float(sub["stance"].rank().corr(sub["exc"].rank()))
        t = cluster_robust_tstat((sub["stance"] * sub["exc"]).tolist(),
                                 list(zip(sub["theme"], sub["rec_date"])))
        sub2 = sub.dropna(subset=["rank"])
        part = _partial_ic(sub2["stance"], sub2["exc"], sub2["rank"]) if len(sub2) >= 20 else float("nan")
        corr_sr = (float(sub2["stance"].rank().corr(sub2["rank"].rank()))
                   if len(sub2) >= 20 else float("nan"))
        rows_out.append([f"{h}日", len(sub),
                         f"{ic:+.3f}" if np.isfinite(ic) else "NA",
                         f"{t:+.1f}" if np.isfinite(t) else "NA",
                         f"{part:+.3f}" if np.isfinite(part) else "NA",
                         f"{corr_sr:+.2f}" if np.isfinite(corr_sr) else "NA"])

    if rows_out:
        L.append("")
        L.append(md_table(["窗口", "n", "立场 rank-IC", "聚类t", "增量偏IC(控rank_pct)", "立场×rank_pct 相关"],
                          rows_out))

    # 战绩加权 vs 平权（数据允许才算）
    sk = df.groupby("source")["exc"].mean()
    if len(sk) >= 2 and df["source"].notna().any():
        smap = sk.to_dict()
        d10 = df[df["h"] == (10 if (df["h"] == 10).any() else df["h"].max())].copy()
        if len(d10) >= 20:
            d10["sw"] = d10["stance"] * d10["source"].map(smap).fillna(0)
            ic_flat = d10["stance"].rank().corr(d10["exc"].rank())
            ic_skill = d10["sw"].rank().corr(d10["exc"].rank())
            L.append("")
            L.append(f"- **战绩加权对比**（{len(sk)} 个来源）：平权立场 IC {ic_flat:+.3f} → "
                     f"战绩加权 IC {ic_skill:+.3f}（{'↑ 加权更优' if ic_skill>ic_flat else '未见提升，样本或太短'}）。")

    # 裁决
    h_ref = 5 if (df["h"] == 5).any() else int(df["h"].min())
    ref = df[df["h"] == h_ref].dropna(subset=["rank"])
    ic_ref = float(ref["stance"].rank().corr(ref["exc"].rank())) if len(ref) >= 20 else float("nan")
    part_ref = _partial_ic(ref["stance"], ref["exc"], ref["rank"]) if len(ref) >= 20 else float("nan")
    t_ref = cluster_robust_tstat((ref["stance"] * ref["exc"]).tolist(),
                                 list(zip(ref["theme"], ref["rec_date"]))) if len(ref) else float("nan")
    passed = (np.isfinite(ic_ref) and ic_ref > 0.02 and np.isfinite(part_ref) and part_ref > 0.01
              and np.isfinite(t_ref) and abs(t_ref) > 2)
    L += ["", "### 结论（融合决策）"]
    if passed:
        L.append(f"- ✅ **支持融合**：立场 IC {ic_ref:+.3f}、控制量化后**增量偏 IC {part_ref:+.3f}>0**、"
                 f"聚类稳健 t={t_ref:+.1f} → 投顾对量化有独立增量信息，值得做成因子进 fuse_targets"
                 f"（走影子→回测A/B→融合）。")
    elif np.isfinite(part_ref) and part_ref > 0.01:
        L.append(f"- ⏳ **有增量迹象、功效不足**：增量偏 IC {part_ref:+.3f}>0（投顾与量化正交、方向对），"
                 f"但 IC/t 未达门槛（立场 IC {ic_ref:+.3f}、t={t_ref:+.1f}）——投顾历史仅数周，"
                 f"**等 advisor_reco 累积后自动复验**；先不融合。")
    else:
        L.append(f"- ⏳ **hold**：当前样本未见投顾对量化的稳定增量（立场 IC {ic_ref:+.3f}、"
                 f"增量偏 IC {part_ref if np.isfinite(part_ref) else float('nan'):+.3f}）。样本太短，等数据。")
    L.append("- 备注：立场×rank_pct 相关越低越好（说明投顾带独立信息）；增量偏 IC 才是「融合有没有价值」的关键。"
             "研究支持「投顾+因子」融合最优(IR~1.23)、战绩加权有据，但均须在你自己数据上过门槛。")
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
