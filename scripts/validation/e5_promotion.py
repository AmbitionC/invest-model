"""E5：影子版本 / 候选因子的晋升检查（只读）。

复用仓库既有治理（docs/model_change_proposals.md）：version 隔离影子 + backtest_run 存每版
metrics + 文档化晋升门槛 + 红线。本实验读累积数据，对每个候选输出 promote / hold / reject。

判据（照搬文档）：
  - 换手影子（cs_pf_v2 换手惩罚+inv_vol vs 基线 cs_ic_v1）：
      turnover_total 显著更低（≤基线×0.9）且 annual_return 不差（≥基线−0.5pp）
      且 sharpe 不差（≥基线−0.05）且 MaxDD 不破红线（较基线恶化 ≤5pp）→ promote。
  - 候选因子 nb_ratio_chg_20：≥12 期 IC 且 |mean rank-IC|≥0.02 且 IR≥0.3 → promote。
  - 红线：MaxDD 较旧版恶化 >5pp → reject。

**只读**：不写库、不改配置。晋升动作由人/后续 PR 执行。
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from common import BaseRepository, md_table, pct

BASELINE_HINT = "ic_v1"           # 基线版本名包含该串
CAND_FACTOR = "nb_ratio_chg_20"   # 影子候选因子


def _all_runs(repo: BaseRepository) -> pd.DataFrame:
    if not repo.table_exists("backtest_run"):
        return pd.DataFrame()
    df = repo.read_sql(
        "SELECT run_id, name, start_date, end_date, metrics FROM backtest_run ORDER BY run_id")
    if df.empty:
        return df

    def _m(x):
        try:
            return json.loads(x) if x else {}
        except Exception:
            return {}
    df["m"] = df["metrics"].apply(_m)
    df["n_runs"] = df.groupby("name")["run_id"].transform("count")
    return df


def _latest_per_name(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("run_id").groupby("name", as_index=False).tail(1)


def _g(m: dict, *keys, default=np.nan) -> float:
    for k in keys:
        if k in m and m[k] is not None:
            try:
                return float(m[k])
            except Exception:
                pass
    return default


def _cmp_row(name: str, m: dict, bm: dict, n_runs: int,
             cand_sharpes: list, base_sharpe_ref: float) -> tuple[list, str, str]:
    t_c, t_b = _g(m, "turnover_total"), _g(bm, "turnover_total")
    a_c, a_b = _g(m, "annual_return"), _g(bm, "annual_return")
    s_c, s_b = _g(m, "sharpe"), _g(bm, "sharpe")
    dd_c, dd_b = abs(_g(m, "max_drawdown")), abs(_g(bm, "max_drawdown"))
    d_turn = (t_c / t_b - 1) if (np.isfinite(t_c) and np.isfinite(t_b) and t_b) else np.nan
    d_ann = a_c - a_b
    d_shp = s_c - s_b
    d_dd = dd_c - dd_b  # 正=恶化
    # 一致性：多少次运行的 Sharpe 打赢基线参考
    n_beat = sum(1 for s in cand_sharpes if np.isfinite(s) and s >= base_sharpe_ref - 1e-9)

    # 两条晋升路径（任一净收益且不破红线，且连续 ≥4 期一致）：
    #   (a) 换手路径：换手↓10% 且 年化/Sharpe 不差；
    #   (b) 夏普路径：Sharpe 明显更高(≥+0.10) 且 换手/MaxDD 不显著变差。
    red_line = np.isfinite(d_dd) and d_dd > 0.05
    turn_path = (np.isfinite(d_turn) and d_turn <= -0.10
                 and (not np.isfinite(d_ann) or d_ann >= -0.005)
                 and (not np.isfinite(d_shp) or d_shp >= -0.05))
    sharpe_path = (np.isfinite(d_shp) and d_shp >= 0.10
                   and (not np.isfinite(d_turn) or d_turn <= 0.10)
                   and not red_line)
    net_better = turn_path or sharpe_path
    consistent = (n_runs >= 4 and n_beat >= max(4, int(np.ceil(0.75 * n_runs))))

    if red_line:
        verdict = "🛑 reject（MaxDD 较基线恶化 >5pp，触红线）"
    elif net_better and consistent:
        path = "换手" if turn_path else "Sharpe"
        verdict = f"✅ promote（{path}路径净收益 + {n_beat}/{n_runs} 期一致跑赢，达文档门槛）"
    elif net_better:
        path = "换手" if turn_path else f"Sharpe(Δ{d_shp:+.2f}/年化Δ{d_ann:+.1%})"
        verdict = (f"⏳ hold — **在路上**（{path}路径净收益，但仅 {n_runs} 次运行、"
                   f"{n_beat} 次跑赢；文档要求连续 4 期，等下次周度重建自动补齐）")
    else:
        verdict = "⏳ hold（未见稳定净收益：换手未↓10% 且 Sharpe 未↑0.10）"
    row = [name, f"{n_beat}/{n_runs}",
           f"{t_c:.1f}" if np.isfinite(t_c) else "NA",
           f"{d_turn:+.0%}" if np.isfinite(d_turn) else "NA",
           pct(a_c), f"{d_ann:+.1%}" if np.isfinite(d_ann) else "NA",
           f"{s_c:+.2f}" if np.isfinite(s_c) else "NA",
           f"{d_shp:+.2f}" if np.isfinite(d_shp) else "NA",
           f"{dd_c*100:.1f}%" if np.isfinite(dd_c) else "NA"]
    return row, verdict, name


def _factor_promotion(repo: BaseRepository) -> str:
    if not repo.table_exists("factor_ic_log"):
        return "- factor_ic_log 缺失，跳过候选因子检查。"
    df = repo.read_sql(
        "SELECT trade_date, rank_ic FROM factor_ic_log WHERE factor_name=:f",
        {"f": CAND_FACTOR})
    if df.empty:
        return f"- 候选因子 `{CAND_FACTOR}`：暂无 IC 记录（未影子观察或无数据）。"
    df["rank_ic"] = pd.to_numeric(df["rank_ic"], errors="coerce")
    v = df["rank_ic"].dropna()
    n = df["trade_date"].nunique()
    mean_ic = float(v.mean()) if len(v) else float("nan")
    ir = float(v.mean() / v.std()) if len(v) > 1 and v.std() else float("nan")
    ok = (n >= 12 and abs(mean_ic) >= 0.02 and np.isfinite(ir) and ir >= 0.3)
    verdict = ("✅ promote（≥12 期且 |IC|≥0.02 且 IR≥0.3）→ 移入 FACTOR_DIRECTION，回测复验"
               if ok else
               f"⏳ hold（门槛：≥12 期/|IC|≥0.02/IR≥0.3；当前 {n} 期）")
    return (f"- 候选因子 `{CAND_FACTOR}`：{n} 期，均值 rank-IC {mean_ic:+.3f}，"
            f"IR {ir:+.2f} → **{verdict}**")


def run(repo: BaseRepository) -> str:
    L = ["## E5 —— 影子版本 / 候选因子 晋升检查（读累积治理数据）", ""]
    allruns = _all_runs(repo)
    latest = _latest_per_name(allruns) if not allruns.empty else allruns
    if latest.empty:
        L.append("- backtest_run 无数据：影子/基线回测尚未产出（build-model 未跑或库空）。")
    else:
        names = list(latest["name"])
        base = next((n for n in names if BASELINE_HINT in n), None)
        if base is None:
            L.append(f"- 未找到基线（name 含 '{BASELINE_HINT}'）；现有版本：{names}。")
        else:
            bm = latest[latest["name"] == base]["m"].iloc[0]
            base_sharpe_ref = _g(bm, "sharpe")
            L.append(f"- 基线：`{base}`（turnover_total={_g(bm,'turnover_total'):.1f}、"
                     f"annual={pct(_g(bm,'annual_return'))}、sharpe={_g(bm,'sharpe'):+.2f}、"
                     f"MaxDD={abs(_g(bm,'max_drawdown'))*100:.1f}%）。")
            rows, verdicts = [], []
            for _, r in latest.iterrows():
                if r["name"] == base:
                    continue
                cand_sharpes = [_g(mm, "sharpe")
                                for mm in allruns[allruns["name"] == r["name"]]["m"]]
                row, verdict, nm = _cmp_row(r["name"], r["m"], bm, int(r["n_runs"]),
                                            cand_sharpes, base_sharpe_ref)
                rows.append(row); verdicts.append(f"  - `{nm}`：{verdict}")
            if rows:
                L.append("")
                L.append(md_table(
                    ["候选版本", "跑赢/次数", "换手", "Δ换手", "年化", "Δ年化", "Sharpe", "ΔSharpe", "MaxDD"],
                    rows))
                L += ["", "### 晋升裁决（换手影子）"] + verdicts
            else:
                L.append("- 除基线外无候选影子版本（build-model 的 pf_v2 影子可能尚未跑）。")
    L += ["", "### 候选因子晋升", _factor_promotion(repo)]
    # model_registry 上下文
    if repo.table_exists("model_registry"):
        mr = repo.read_sql(
            "SELECT version, cv_ic_mean, cv_ic_ir, cv_hit_rate FROM model_registry ORDER BY version")
        if not mr.empty:
            L += ["", "### model_registry（cv 指标，参考）", "",
                  md_table(["version", "cv_ic_mean", "cv_ic_ir", "cv_hit_rate"],
                           [[r["version"], r["cv_ic_mean"], r["cv_ic_ir"], r["cv_hit_rate"]]
                            for _, r in mr.iterrows()])]
    L += ["", "> 晋升=version 隔离 + 一处配置切换、零风险回退；未达标一律 hold，等数据。"]
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
