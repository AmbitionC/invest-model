"""E10：防御底仓层（P13 预登记验证）——红利低波底仓能否用可接受的收益代价换回撤改善？

H0（零假设）：在进攻组合上叠加 20-30% 红利低波底仓（月度再平衡），
  对组合的最大回撤没有实质改善，或改善以不可接受的收益损失为代价。

出处：docs/strategy_research_202607.md 证据 #8（中证红利低波 2022-2024 熊市防御，
  经官方逐年数据交叉验证）。已知偏差：2019-2020 成长牛是红利风格逆风期，
  本验证**强制包含**该段，防止只挑顺风窗口。

口径（诚实声明）：
  - 底仓用**价格指数**（tushare 无全收益权限时），少计约 4-6%/年股息 →
    底仓收益被系统性**低估**，若价格口径都过关，计息后只会更优（保守方向）。
  - 进攻代理用中证500 + 创业板指（贴近题材/周期持仓风格），非真实组合——
    真实组合另有个股风控，本验证只测"结构效应"。

方法：
  - 全样本（index_daily 有多深用多深，目标 2015~今）；月度再平衡到固定权重
    w_def ∈ {0%, 20%, 30%}；日收益 = w·底仓 + (1-w)·进攻（月内权重漂移）。
  - 指标：年化、最大回撤、最长水下天数；分段年化：2019-2020（红利逆风）、
    2022-2024（红利顺风）。

过关判据（预登记，写死勿改）：w_def=30% 相对纯进攻，在**两个**进攻代理下同时满足：
  ① 全样本最大回撤改善 ≥3pp；② 全样本年化损失 ≤1.5pp（价格口径）；
  ③ 2019-2020 逆风段年化损失 ≤4pp。

用法：python scripts/validation/e10_defensive_sleeve.py [--db ...] [--out results/e10.md]
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

# 底仓候选（按序取第一个有数据的）：中证红利低波 > 中证红利（均为价格指数）
DEFENSIVE_CODES = ["H30269.CSI", "000922.CSI"]
OFFENSE_CODES = ["000905.SH", "399006.SZ"]     # 中证500 / 创业板指（题材风格代理）
MIX_WEIGHTS = [0.0, 0.2, 0.3]
HEADWIND = ("20190101", "20201231")            # 红利逆风段（成长牛）
TAILWIND = ("20220101", "20241231")            # 红利顺风段（熊市）
MIN_DAYS = 500                                  # 共同样本 <2 年不判定

# 预登记判据
PASS_MAXDD_IMPROVE = 0.03
PASS_ANN_COST = 0.015
PASS_HEADWIND_COST = 0.04


def _closes(repo, code: str) -> pd.Series:
    df = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c ORDER BY trade_date",
        {"c": code})
    if df.empty:
        return pd.Series(dtype=float)
    s = pd.to_numeric(df.set_index("trade_date")["close"], errors="coerce").dropna()
    return s[~s.index.duplicated(keep="last")]


def mix_nav(off: pd.Series, dfn: pd.Series, w_def: float) -> pd.Series:
    """月度再平衡混合净值。off/dfn 为对齐后的收盘序列（index=YYYYMMDD 升序）。

    每月首个交易日再平衡到 (1-w, w)，月内权重随价格漂移——与实际执行一致，
    避免"每日再平衡"高估再平衡收益。
    """
    ro, rd = off.pct_change().fillna(0.0), dfn.pct_change().fillna(0.0)
    months = pd.Series([d[:6] for d in off.index], index=off.index)
    nav, vo, vd = [], 1.0 - w_def, w_def
    prev_m = None
    for i, d in enumerate(off.index):
        if months.iloc[i] != prev_m and prev_m is not None:   # 月初再平衡
            tot = vo + vd
            vo, vd = tot * (1.0 - w_def), tot * w_def
        prev_m = months.iloc[i]
        vo *= 1.0 + ro.iloc[i]
        vd *= 1.0 + rd.iloc[i]
        nav.append(vo + vd)
    return pd.Series(nav, index=off.index)


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    return float((nav / peak - 1.0).min())


def underwater_days(nav: pd.Series) -> int:
    """最长水下天数（按交易日计）。"""
    peak, longest, cur = -np.inf, 0, 0
    for v in nav.to_numpy(dtype=float):
        if v >= peak:
            peak, cur = v, 0
        else:
            cur += 1
            longest = max(longest, cur)
    return longest


def annualized(nav: pd.Series) -> float:
    if len(nav) < 2 or nav.iloc[0] <= 0:
        return float("nan")
    yrs = len(nav) / 244.0
    return float((nav.iloc[-1] / nav.iloc[0]) ** (1.0 / yrs) - 1.0) if yrs > 0 else float("nan")


def _seg_ann(nav: pd.Series, lo: str, hi: str) -> float:
    seg = nav[(nav.index >= lo) & (nav.index <= hi)]
    return annualized(seg) if len(seg) >= 100 else float("nan")


def run(repo) -> str:
    lines = ["## E10 —— 防御底仓层：红利低波混合（P13 预登记验证）", ""]
    lines.append(f"- 判据（预登记）：30% 底仓 vs 纯进攻，双进攻代理同时满足 "
                 f"全样本 MaxDD 改善≥{PASS_MAXDD_IMPROVE:.0%} 且 年化损失≤{PASS_ANN_COST:.1%} "
                 f"且 2019-2020 逆风段年化损失≤{PASS_HEADWIND_COST:.0%}。底仓为价格指数，"
                 "少计股息 → 结论偏保守。")
    dfn_code, dfn = None, pd.Series(dtype=float)
    for c in DEFENSIVE_CODES:
        s = _closes(repo, c)
        if len(s) >= MIN_DAYS:
            dfn_code, dfn = c, s
            break
    if dfn_code is None:
        lines.append(f"- ⚠️ 底仓指数无数据（候选 {DEFENSIVE_CODES}）——先 bump "
                     "ops/index-backfill.trigger 回填后重跑。**E10 数据不足，不判定。**")
        return "\n".join(lines)

    ok_flags = []
    for oc in OFFENSE_CODES:
        off = _closes(repo, oc)
        common = off.index.intersection(dfn.index)
        if len(common) < MIN_DAYS:
            lines.append(f"- ⚠️ {oc}×{dfn_code} 共同样本 {len(common)} 日（<{MIN_DAYS}），跳过。")
            ok_flags.append(None)
            continue
        o, d = off.loc[common], dfn.loc[common]
        lines.append(f"\n### 进攻 {oc} × 底仓 {dfn_code}"
                     f"（{common[0]}~{common[-1]}，{len(common)} 交易日）")
        lines.append("\n| 底仓比例 | 年化 | 最大回撤 | 最长水下(日) | 2019-20逆风年化 | 2022-24顺风年化 |")
        lines.append("|---|---|---|---|---|---|")
        stats = {}
        for w in MIX_WEIGHTS:
            nav = mix_nav(o, d, w)
            stats[w] = (annualized(nav), max_drawdown(nav), underwater_days(nav),
                        _seg_ann(nav, *HEADWIND), _seg_ann(nav, *TAILWIND))
            a, dd, uw, hw, tw = stats[w]
            lines.append(f"| {w:.0%} | {a:+.2%} | {dd:.2%} | {uw} | "
                         f"{hw:+.2%} | {tw:+.2%} |" if np.isfinite(a) else
                         f"| {w:.0%} | — | — | — | — | — |")
        a0, dd0, _, hw0, _ = stats[0.0]
        a3, dd3, _, hw3, _ = stats[0.3]
        dd_imp = dd3 - dd0                      # 回撤为负值，改善=dd3 更接近 0
        ann_cost = a0 - a3
        hw_cost = (hw0 - hw3) if (np.isfinite(hw0) and np.isfinite(hw3)) else float("nan")
        ok = (dd_imp >= PASS_MAXDD_IMPROVE and ann_cost <= PASS_ANN_COST
              and (not np.isfinite(hw_cost) or hw_cost <= PASS_HEADWIND_COST))
        ok_flags.append(ok)
        lines.append(f"- 30% 底仓：MaxDD 改善 {dd_imp:+.2%}，年化代价 {ann_cost:+.2%}，"
                     f"逆风段代价 {hw_cost:+.2%} → {'✅ 达标' if ok else '❌ 未达标'}")

    lines.append("\n### 裁决（预登记判据）")
    decided = [f for f in ok_flags if f is not None]
    if not decided:
        lines.append("- **P13 裁决**：⏳ 数据不足，不判定。")
    elif all(decided) and len(decided) == len(OFFENSE_CODES):
        lines.append("- **P13 裁决**：✅ 过关——可进下一步（计划层底仓建议 + 影子观察），"
                     "仍不直接改生产。")
    else:
        lines.append("- **P13 裁决**：❌ 未过关/部分代理缺数据——保持现状。")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="E10 防御底仓验证（P13 预登记）")
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
