"""E11：因子分族体检（P14 预登记验证）——交易类 vs 会计类因子在本系统截面上的区分力对比。

H0（零假设）：会计类因子族与交易类因子族在本系统投顾池截面上的 rank-IC
  无实质差异（即公开证据的"存活异象皆交易类"不适用于本系统的小截面）。

出处：docs/strategy_research_202607.md 证据 #4/#5（Hou-Qiao-Zhang 清华五道口 WP、
  Li et al. Management Science 2024：严格检验下 A 股存活异象几乎全是交易类指标）
  与证据 #1（小市值溢价=壳价值制度红利，注册制后消退）。

口径（诚实声明）：
  - factor_ic_log 按交易日记录、前瞻窗口 horizon 日 → 相邻记录高度重叠，
    直接对全序列做 t 检验会**虚高显著性**。本脚本按 horizon 间隔抽不重叠子样本
    计算 t（保守口径），并同时列全样本均值供参考。
  - 本系统截面 = 投顾池（几十只），非全市场——学术结论只作先验，
    裁决只看自家数据。样本期短（2023 起），功效有限，判据从宽、动作从轻
    （最重动作=提案降权，不直接改因子池）。

因子分族：
  交易类  mom_60 / mom_120 / reversal_5 / lowvol_20 / low_turnover
  会计类  ep / bp / sp / roe / roa / gross_margin / rev_yoy / profit_yoy
  制度类  small_size（壳价值消退假设单独观察：近 12 期 vs 早期符号与强度）

判据（预登记，写死勿改）：
  ① 族对比：会计类族平均 |rank-IC| < 交易类族的 1/2，且会计类中不重叠 |t|≥2
     的因子占比 <1/3 → 支持「会计类降权」提案进入评审（不直接改）。
  ② small_size：近 12 期均值符号与早期相反 且 不重叠 |t|≥1.5 →
     建议先验降为 0 并转入候选影子复核。
  ③ 任一族样本（不重叠）<8 期 → 该项不判定。

用法：python scripts/validation/e11_factor_families.py [--db ...] [--out results/e11.md]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from common import get_repo  # noqa: E402

FAMILIES: dict[str, list[str]] = {
    "交易类": ["mom_60", "mom_120", "reversal_5", "lowvol_20", "low_turnover"],
    "会计类": ["ep", "bp", "sp", "roe", "roa", "gross_margin", "rev_yoy", "profit_yoy"],
}
SIZE_FACTOR = "small_size"
MIN_PERIODS = 8
RECENT_N = 12


def t_stat(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    if len(x) < 2:
        return float("nan")
    sd = x.std(ddof=1)
    return float(x.mean() / (sd / np.sqrt(len(x)))) if sd > 0 else float("nan")


def _load_ic(repo) -> pd.DataFrame:
    if not repo.table_exists("factor_ic_log"):
        return pd.DataFrame()
    df = repo.read_sql(
        "SELECT trade_date, factor_name, horizon, rank_ic FROM factor_ic_log "
        "ORDER BY trade_date")
    if df.empty:
        return df
    df["rank_ic"] = pd.to_numeric(df["rank_ic"], errors="coerce")
    df["horizon"] = pd.to_numeric(df["horizon"], errors="coerce")
    df = df.dropna(subset=["rank_ic", "horizon"])
    # 取记录最多的 horizon 作主窗口（口径统一）
    if df.empty:
        return df
    main_h = int(df.groupby("horizon").size().idxmax())
    return df[df["horizon"] == main_h].assign(main_h=main_h)


def factor_stats(df: pd.DataFrame, name: str) -> dict | None:
    sub = df[df["factor_name"] == name].sort_values("trade_date")
    if sub.empty:
        return None
    gap = int(sub["main_h"].iloc[0])
    full = sub["rank_ic"].to_numpy(dtype=float)
    sparse = sub["rank_ic"].to_numpy(dtype=float)[::max(1, gap)]
    return {"n": len(full), "n_eff": len(sparse), "mean": float(np.nanmean(full)),
            "t": t_stat(sparse),
            "recent_mean": float(np.nanmean(full[-RECENT_N * max(1, gap):]))
            if len(full) else float("nan"),
            "series": sub}


def run(repo) -> str:
    lines = ["## E11 —— 因子分族体检：交易类 vs 会计类（P14 预登记验证）", ""]
    lines.append(f"- 判据（预登记）：会计类平均|IC|<交易类 1/2 且会计类显著(|t|≥2)占比<1/3 "
                 f"→ 提「会计类降权」评审；small_size 近 {RECENT_N} 期符号翻转且|t|≥1.5 "
                 f"→ 建议转候选影子。不重叠样本 <{MIN_PERIODS} 期不判定。t 为不重叠抽样保守口径。")
    df = _load_ic(repo)
    if df.empty:
        lines.append("- ⚠️ factor_ic_log 无数据——先跑 build-model / 日更积累 IC。**不判定。**")
        return "\n".join(lines)
    lines.append(f"- 主窗口 horizon={int(df['main_h'].iloc[0])} 日，"
                 f"记录期 {df['trade_date'].min()}~{df['trade_date'].max()}。")

    fam_abs, fam_sig, fam_neff = {}, {}, {}
    for fam, names in FAMILIES.items():
        lines.append(f"\n### {fam}")
        lines.append("\n| 因子 | 记录数 | 不重叠期数 | rank-IC 均值 | t(不重叠) |")
        lines.append("|---|---|---|---|---|")
        abss, sigs, neffs = [], [], []
        for name in names:
            st = factor_stats(df, name)
            if st is None:
                lines.append(f"| {name} | 0 | — | — | — |")
                continue
            lines.append(f"| {name} | {st['n']} | {st['n_eff']} | "
                         f"{st['mean']:+.4f} | {st['t']:+.2f} |")
            abss.append(abs(st["mean"]))
            sigs.append(1 if (np.isfinite(st["t"]) and abs(st["t"]) >= 2) else 0)
            neffs.append(st["n_eff"])
        fam_abs[fam] = float(np.mean(abss)) if abss else float("nan")
        fam_sig[fam] = (sum(sigs) / len(sigs)) if sigs else float("nan")
        fam_neff[fam] = int(min(neffs)) if neffs else 0
        lines.append(f"- 族平均|rank-IC|={fam_abs[fam]:.4f}，显著因子占比 {fam_sig[fam]:.0%}，"
                     f"最小不重叠期数 {fam_neff[fam]}")

    # ── small_size 单独观察（壳价值消退假设）──
    lines.append(f"\n### 制度类：{SIZE_FACTOR}（壳价值消退假设）")
    st = factor_stats(df, SIZE_FACTOR)
    size_flip = None
    if st is None or st["n_eff"] < MIN_PERIODS:
        lines.append("- 样本不足，不判定。")
    else:
        early_mean = float(np.nanmean(
            st["series"]["rank_ic"].to_numpy(dtype=float)[: -RECENT_N]) if st["n"] > RECENT_N
            else float("nan"))
        lines.append(f"- 全样本均值 {st['mean']:+.4f}（t={st['t']:+.2f}），"
                     f"近 {RECENT_N} 期均值 {st['recent_mean']:+.4f}，早期均值 "
                     f"{early_mean:+.4f}。")
        size_flip = (np.isfinite(early_mean) and np.isfinite(st["recent_mean"])
                     and early_mean * st["recent_mean"] < 0
                     and np.isfinite(st["t"]) and abs(st["t"]) >= 1.5)
        lines.append(f"- 符号翻转判定：{'✅ 触发（建议转候选影子）' if size_flip else '❌ 未触发（维持现状）'}")

    # ── 裁决 ──
    lines.append("\n### 裁决（预登记判据）")
    if min(fam_neff.get("交易类", 0), fam_neff.get("会计类", 0)) < MIN_PERIODS:
        lines.append("- **P14 裁决**：⏳ 不重叠样本不足，不判定——随日更积累后自动增强。")
    else:
        acct_weak = (np.isfinite(fam_abs["会计类"]) and np.isfinite(fam_abs["交易类"])
                     and fam_abs["会计类"] < 0.5 * fam_abs["交易类"]
                     and fam_sig["会计类"] < 1 / 3)
        lines.append(f"- 族对比：会计类|IC|/交易类|IC| = "
                     f"{fam_abs['会计类'] / fam_abs['交易类']:.2f}"
                     if fam_abs.get("交易类") else "- 族对比：数据异常")
        lines.append(f"- **P14 裁决**：{'✅ 触发「会计类降权」提案（进评审，不直接改）' if acct_weak else '❌ 未触发——两族区分力无实质差异或会计类仍有效，维持现状'}")
    lines.append("- 提醒：IC_IR 加权本就会自动降权弱因子——本体检的价值是把「自动降权」"
                 "显性化成结构性认知，动作永远走提案评审。")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="E11 因子分族体检（P14 预登记）")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    md = run(get_repo(args.db))
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
