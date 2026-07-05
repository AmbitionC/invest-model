"""E7：题材/行业拥挤度监控（只读风控 overlay）。

研究依据（深度调研，issue 报告）：拥挤是量化/题材策略的核心失效模式——共享输入 → 组合雷同 →
一起踩踏（2025 Quant Unwind）。你实测也吃过这个亏：投顾 3 周样本高度扎堆半导体+算力等少数题材，
聚类稳健 t 一压就掉。故把「当前可动用标的的题材/行业集中度」做成常态监控,越集中越脆弱。

指标（越高越危险）：
  - 题材/行业 **HHI**（Herfindahl，Σ 份额²，1/N~1）与 **Top-3 份额**；
  - 覆盖：当期有效投顾 long 推荐 + （若有）量化组合 portfolio_target 的行业分布。
预警线（经验，可调）：Top-3 题材份额 > 60% 或 行业 HHI > 0.20 → ⚠️ 拥挤，建议限制单题材仓位/分散。

**只读**：不改组合、不下单，只出风险提示。真正限仓由 fuse_targets 的 industry_cap / sleeve_cap 执行。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common import BaseRepository, industry_map, md_table, theme_of


def _hhi(shares: pd.Series) -> float:
    s = shares[shares > 0]
    if s.empty:
        return float("nan")
    p = s / s.sum()
    return float((p ** 2).sum())


def _conc_line(name: str, counts: pd.Series) -> tuple[list, bool]:
    tot = int(counts.sum())
    if tot == 0:
        return [name, 0, "NA", "NA", "NA"], False
    p = counts / tot
    top3 = p.sort_values(ascending=False).head(3)
    hhi = _hhi(counts)
    top3_share = float(top3.sum())
    top3_str = "、".join(f"{k}{v*100:.0f}%" for k, v in top3.items())
    crowded = (top3_share > 0.60) or (hhi > 0.20)
    return [name, tot, f"{hhi:.3f}", f"{top3_share*100:.0f}%", top3_str], crowded


def run(repo: BaseRepository) -> str:
    L = ["## E7 —— 题材/行业拥挤度监控（风控 overlay，只读）", ""]
    rows, flags = [], []

    # 1) 当期有效投顾 long 推荐的题材 + 行业集中度
    if repo.table_exists("advisor_reco"):
        asof = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")
        asof = str(asof["d"].iloc[0]) if not asof.empty and asof["d"].iloc[0] is not None else "99999999"
        reco = repo.read_sql(
            "SELECT code, grade, direction, catalyst, valid_until, rec_date "
            "FROM advisor_reco WHERE direction='long'")
        if not reco.empty:
            reco["rec_date"] = reco["rec_date"].astype(str)
            # 仍有效：valid_until 空=长期；否则 asof<=valid_until。且取每 code 最新。
            def _valid(vu):
                s = str(vu or "").strip(); return (not s) or (asof <= s)
            reco = reco[reco["valid_until"].apply(_valid)]
            reco = reco.sort_values("rec_date").groupby("code", as_index=False).tail(1)
            if not reco.empty:
                reco["theme"] = reco["catalyst"].apply(theme_of)
                r, c = _conc_line("投顾long·题材", reco["theme"].value_counts())
                rows.append(r); flags.append(c)
                imap = industry_map(repo, sorted(reco["code"].unique()))
                if imap:
                    reco["ind"] = reco["code"].map(imap).fillna("NA")
                    r, c = _conc_line("投顾long·行业", reco["ind"].value_counts())
                    rows.append(r); flags.append(c)

    # 2) 量化组合 portfolio_target 的行业集中度（若有）
    if repo.table_exists("portfolio_target"):
        pt = repo.read_sql(
            "SELECT code, weight FROM portfolio_target WHERE trade_date="
            "(SELECT MAX(trade_date) FROM portfolio_target)")
        if not pt.empty:
            imap = industry_map(repo, sorted(pt["code"].unique()))
            if imap:
                pt["ind"] = pt["code"].map(imap).fillna("NA")
                pt["weight"] = pd.to_numeric(pt["weight"], errors="coerce").fillna(0)
                w_by_ind = pt.groupby("ind")["weight"].sum()
                r, c = _conc_line("量化组合·行业(按权重)", w_by_ind)
                rows.append(r); flags.append(c)

    if not rows:
        return "\n".join(L + ["- 数据不足：无 advisor_reco / portfolio_target 可评估。"])

    L.append(md_table(["口径", "标的/项数", "HHI", "Top-3 份额", "Top-3 明细"], rows))
    crowded = any(flags)
    L += ["", "### 结论",
          ("- ⚠️ **拥挤预警**：存在 Top-3 份额>60% 或 HHI>0.20 的口径 → 建议收紧单题材/单行业仓位"
           "（fuse_targets 的 industry_cap/sleeve_cap 或人工分散），防同题材踩踏。"
           if crowded else
           "- ✅ 当前集中度尚可（无口径触预警线）。继续每周监控。"),
          "- 备注：拥挤是量化/题材策略核心失效模式（共享输入→组合雷同→一起踩踏）；这是只读提示，"
          "限仓由组合层执行。你实测的投顾题材扎堆正是此项要长期盯的。"]
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
