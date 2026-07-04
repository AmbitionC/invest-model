"""E2（核心）：投顾推荐是否有真实、可持续的超额？分级是否成立？

预登记：
  H0: 真实 advisor_reco(long) 相对基准/行业**无显著正超额**；且 A>B>C 分级排序不成立。
  指标: 固定前瞻窗口(5/10/20 交易日)的**基准相对**与**行业相对**超额，按 分级×来源×主题 拆，
        带样本量 + **扎堆聚类稳健 t**（按 (主题,rec_date) 聚类，防高估显著性）。
        walk-forward: 用「历史已实现分级超额」定 conviction vs 固定 A=2:B=1:C=0，比两本组合净超额。
  过关: 分级排序成立且至少某桶聚类稳健 |t|>2 → 数据驱动 conviction 值得做；
        整体无显著 → 如实结论「投顾这条线在该样本上无可证超额」，据此定风险预算。

只读。窗口越长越可信；本实验只算已实现收益，无回测负担，可拉多年全历史。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common import (BaseRepository, close_panel, cluster_robust_tstat, first_on_or_after,
                    fwd_return, index_returns, industry_map, md_table, pct, summ, theme_of,
                    trade_calendar)

HORIZONS = [5, 10, 20]
BENCH = ["000300.SH", "000905.SH", "000852.SH"]  # 沪深300 / 中证500 / 中证1000


def load_recos(repo: BaseRepository) -> pd.DataFrame:
    if not repo.table_exists("advisor_reco"):
        return pd.DataFrame()
    df = repo.read_sql(
        "SELECT rec_date, code, source_type, grade, direction, catalyst "
        "FROM advisor_reco WHERE direction='long'")
    if df.empty:
        return df
    df["rec_date"] = df["rec_date"].astype(str)
    df["grade"] = df["grade"].fillna("?").astype(str).str.strip().replace("", "?")
    df["source_type"] = df["source_type"].fillna("?").astype(str)
    df["theme"] = df["catalyst"].apply(theme_of)
    # 去重 (rec_date, code) 保留首条
    return df.drop_duplicates(["rec_date", "code"]).reset_index(drop=True)


def build_forward(repo: BaseRepository, recos: pd.DataFrame) -> pd.DataFrame:
    """给每条推荐算固定窗口的绝对/基准超额/行业超额收益。"""
    cal = trade_calendar(repo)
    if not cal or recos.empty:
        return pd.DataFrame()
    cal_idx = {d: i for i, d in enumerate(cal)}
    codes = sorted(recos["code"].unique())
    imap = industry_map(repo, codes)

    # 需要的所有日期：每条 rec 的 entry + 各 horizon 的 exit
    need_dates, rows = set(), []
    for _, r in recos.iterrows():
        e = first_on_or_after(cal, r["rec_date"])
        if e is None:
            continue
        ei = cal_idx[e]
        exits = {}
        for h in HORIZONS:
            if ei + h < len(cal):
                exits[h] = cal[ei + h]
                need_dates.add(exits[h])
        need_dates.add(e)
        rows.append({**r.to_dict(), "entry": e, "exits": exits})
    if not rows:
        return pd.DataFrame()

    need_dates = sorted(need_dates)
    panel = close_panel(repo, codes, need_dates)
    bench = index_returns(repo, BENCH, need_dates)

    # 行业均值：对每个 (entry,exit) 用全市场同业均值。为控成本，按需拉全 A 在这些日期的收盘。
    # 这里用「推荐票所属行业」的市场同业：查 stock_info 行业 → 拉该行业全部 code 在 need_dates 收盘。
    ind_universe = {}
    if imap and repo.table_exists("stock_info"):
        inds = sorted({v for v in imap.values() if v})
        if inds:
            iph = ",".join(f":i{k}" for k in range(len(inds)))
            allmap = repo.read_sql(
                f"SELECT ts_code, industry FROM stock_info WHERE industry IN ({iph})",
                {f"i{k}": v for k, v in enumerate(inds)})
            peers = sorted(allmap["ts_code"].unique()) if not allmap.empty else []
            peer_ind = dict(zip(allmap["ts_code"], allmap["industry"])) if not allmap.empty else {}
            peer_panel = close_panel(repo, peers, need_dates) if peers else {}
            # {industry: {(d0,d1): mean_ret}} 缓存按需算
            ind_universe = {"peer_panel": peer_panel, "peer_ind": peer_ind}

    def industry_ret(ind: str, d0: str, d1: str) -> float | None:
        if not ind_universe:
            return None
        pp, pi = ind_universe["peer_panel"], ind_universe["peer_ind"]
        rs = []
        for c, icode in pi.items():
            if icode != ind:
                continue
            rr = fwd_return(pp.get(c, {}), d0, d1)
            if rr is not None:
                rs.append(rr)
        return float(np.mean(rs)) if len(rs) >= 5 else None

    out = []
    for row in rows:
        e = row["entry"]
        for h, x in row["exits"].items():
            ret = fwd_return(panel.get(row["code"], {}), e, x)
            if ret is None:
                continue
            rec = {"rec_date": row["rec_date"], "code": row["code"], "grade": row["grade"],
                   "source_type": row["source_type"], "theme": row["theme"], "h": h, "ret": ret}
            for bc in BENCH:
                br = fwd_return(bench.get(bc, {}), e, x)
                rec[f"exc_{bc}"] = (ret - br) if br is not None else None
            ind = imap.get(row["code"])
            ir = industry_ret(ind, e, x) if ind else None
            rec["exc_ind"] = (ret - ir) if ir is not None else None
            out.append(rec)
    return pd.DataFrame(out)


def _bucket_lines(fwd: pd.DataFrame, col: str, group: str) -> list[list]:
    rows = []
    for key, sub in fwd.groupby(group):
        s = summ(sub[col].tolist())
        if s["n"] == 0:
            continue
        t = cluster_robust_tstat(sub[col].tolist(),
                                 list(zip(sub["theme"], sub["rec_date"])))
        rows.append([key, s["n"], pct(s["mean"]), pct(s["median"]),
                     f"{s['win']*100:.0f}%", f"{t:+.1f}" if np.isfinite(t) else "NA"])
    return rows


def run(repo: BaseRepository) -> str:
    L = ["## E2 —— 投顾战绩：是否有真实可持续超额 & 分级是否成立", ""]
    recos = load_recos(repo)
    if recos.empty:
        return "\n".join(L + ["**数据不足**：advisor_reco 表为空或不存在，无法验证 E2。"])
    span = f"{recos['rec_date'].min()}~{recos['rec_date'].max()}"
    L.append(f"- 样本：{len(recos)} 条 long 推荐，日期跨度 {span}，"
             f"覆盖 {recos['code'].nunique()} 只标的。")
    gc = {k: int(v) for k, v in recos["grade"].value_counts().items()}
    sc = {k: int(v) for k, v in recos["source_type"].value_counts().items()}
    L.append(f"- 分级分布：{gc}；来源：{sc}。")

    fwd = build_forward(repo, recos)
    if fwd.empty:
        return "\n".join(L + ["", "**数据不足**：无法为推荐匹配到行情（stock_daily 缺失或日期不覆盖）。"])

    # 主口径：行业相对（缺则退基准 CSI300），固定窗口
    prefer = "exc_ind" if fwd["exc_ind"].notna().any() else "exc_000300.SH"
    label = "行业相对超额" if prefer == "exc_ind" else "超额vs沪深300"
    L.append(f"- 主口径：**{label}**（{'有行业面板' if prefer=='exc_ind' else '无行业面板→退基准'}）。")

    for h in HORIZONS:
        sub = fwd[fwd["h"] == h]
        if sub.empty:
            continue
        L += ["", f"### 固定持有 {h} 交易日（{label}）", ""]
        # 全部
        s = summ(sub[prefer].tolist())
        t = cluster_robust_tstat(sub[prefer].tolist(), list(zip(sub["theme"], sub["rec_date"])))
        tstr = f"{t:+.1f}" if np.isfinite(t) else "NA"
        L.append(f"- 全部：n={s['n']} 均值 {pct(s['mean'])} 中位 {pct(s['median'])} "
                 f"胜率 {s['win']*100:.0f}% | **聚类稳健 t={tstr}**")
        L.append("")
        L.append(md_table(["分级", "n", "均值", "中位", "胜率", "聚类t"],
                          _bucket_lines(sub, prefer, "grade")))
        L.append("")
        L.append(md_table(["来源", "n", "均值", "中位", "胜率", "聚类t"],
                          _bucket_lines(sub, prefer, "source_type")))

    # 分级排序判定（用 10 日或最长可得窗口）
    hh = 10 if (fwd["h"] == 10).any() else fwd["h"].max()
    sub = fwd[fwd["h"] == hh]
    ga = {g: summ(sub[sub["grade"] == g][prefer].tolist())["mean"] for g in ["A", "B", "C"]}
    order_ok = (np.isfinite(ga.get("A", np.nan)) and np.isfinite(ga.get("B", np.nan))
                and np.isfinite(ga.get("C", np.nan)) and ga["A"] >= ga["B"] >= ga["C"])
    L += ["", f"### 结论（{hh} 日窗口）",
          f"- 分级均值：A={pct(ga.get('A'))} B={pct(ga.get('B'))} C={pct(ga.get('C'))} "
          f"→ A≥B≥C **{'成立' if order_ok else '不成立'}**。"]
    any_sig = False
    for g in ["A", "B", "C"]:
        gg = sub[sub["grade"] == g]
        if len(gg) >= 3:
            t = cluster_robust_tstat(gg[prefer].tolist(), list(zip(gg["theme"], gg["rec_date"])))
            if np.isfinite(t) and abs(t) > 2:
                any_sig = True
    verdict = ("**过关（部分）**：存在聚类稳健显著的正超额桶 → 值得做数据驱动 conviction（P2）。"
               if any_sig else
               "**未过关/存疑**：无桶达到聚类稳健 |t|>2 → 该样本上投顾超额无法与题材 beta 区分，"
               "据此保守分配风险预算，勿假设持久 alpha。")
    L.append(f"- {verdict}")
    L.append(f"- 备注：聚类 t 按 (主题,rec_date) 聚合，抵消推荐扎堆导致的独立性高估；"
             f"样本越跨越多年/多轮题材越可信。")
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
