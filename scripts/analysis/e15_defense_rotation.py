"""E15：顶部触发防守切换（P19·高低切对冲腿）预登记验证（2026-07-16 判据写死）。

命题（P19，出处：重远 0716 通胀文 + 投顾 0715/0716 高低切独立同向）：
P16 顶部特征减半已生产、减出资金现落现金——历史上顶部触发点之后，
切"防守资产"是否显著优于切现金？

口径（全部沿用已登记框架，不新设、不扫参）：
  周期     = multicycle 前向分段（_detect_all_cycles，E12 正式口径）× 预登记宽基白名单
  触发点   = 每周期首个 C 触发日（_exit_top：浮盈≥15% 后 波动80分位 + 量比1.5）
  防守代理 = H30269 中证红利低波（库内既有，登记时固定，不得事后换更好看的代理）
  处置（触发日次日收盘执行，单边成本 0.05%，至周期终点）：
    A 继续持有原指数｜B 切现金（现行为，收益=-1×成本）｜C 切 H30269（成本=卖+买 2×单边）

判据（P19 段预登记写死，勿按结果回调）：
  ① 有效触发周期数 ≥ 8（触发须落在周期内；H30269 无覆盖的周期剔除并如实报告）
  ② C 相对 B 平均超额收益 ≥ +3pp 且 逐周期胜率 ≥ 60%
  ③ 按周期时间排序分半，两半 C−B 均值方向一致
  ④ C 相对 A 的最大回撤改善 ≥ B 相对 A 回撤改善的 70%（均值口径；B 为现金、回撤=0，
     即要求 平均|dd_C| ≤ 30%×平均|dd_A|——防守腿不得把避险功能丢掉大头）
  四条全过 → 建议晋升：计划层 P16 减半行追加"防守去向"提示（人工执行、标的投顾定）；
  任一不过 → 维持现金去向，如实记录。

局限：指数级、防守代理单一（H30269 起始约 2013+，早期周期覆盖不足会被剔除）、
成本简化；触发点与 E12 共享故继承其周期口径歧义已固定为前向分段。

用法（Actions）：python scripts/analysis/e15_defense_rotation.py [--out ...]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from chinext_cycle_backtest import _load, _exit_top  # noqa: E402
from multicycle_exit_backtest import _detect_all_cycles, INDEX_CANDIDATES  # noqa: E402

DEFENSE = "H30269"       # 中证红利低波（登记时固定）
COST_SIDE = 0.0005


def _mdd(nav: np.ndarray) -> float:
    """最大回撤（正数幅度）。"""
    if len(nav) < 2:
        return 0.0
    peak = np.maximum.accumulate(nav)
    return float(np.max(1.0 - nav / peak))


def main() -> None:
    ap = argparse.ArgumentParser(description="E15 顶部触发防守切换验证")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db)
    repo = BaseRepository(engine)

    dfd = _load(repo, DEFENSE)
    if dfd.empty:
        print(f"⚠️ 防守代理 {DEFENSE} 无数据，E15 无法运行")
        return
    dseries = pd.Series(dfd["close"].values, index=dfd["trade_date"].astype(str))

    rows = []
    n_cycles = n_trig = n_dropped = 0
    for code, name in INDEX_CANDIDATES:
        df = _load(repo, code)
        if len(df) < 300:
            continue
        dates = df["trade_date"].astype(str).to_numpy()
        c = df["close"].to_numpy(dtype=float)
        for s, e in _detect_all_cycles(df):
            n_cycles += 1
            t, reason = _exit_top(df, s)
            if t >= e or "未现顶部特征" in reason:
                continue                      # 周期内未触发 → E15 不评（如实计数）
            n_trig += 1
            x = t + 1                         # 次日收盘执行
            if x >= e:
                n_dropped += 1
                continue
            seg_dates = dates[x:e + 1]
            dseg = dseries.reindex(seg_dates).astype(float)
            if dseg.isna().mean() > 0.05 or not np.isfinite(dseg.iloc[0]) \
                    or not np.isfinite(dseg.iloc[-1]):
                n_dropped += 1                # 防守代理覆盖不足 → 剔除并计数
                continue
            dseg = dseg.ffill()
            nav_a = c[x:e + 1] / c[x]
            nav_c = (dseg / dseg.iloc[0]).to_numpy()
            ret_a = float(nav_a[-1] - 1)
            ret_b = -COST_SIDE
            ret_c = float(nav_c[-1] - 1) - 2 * COST_SIDE
            rows.append({
                "idx": f"{name}", "trig": dates[t], "end": dates[e],
                "days": e - x,
                "A": ret_a, "B": ret_b, "C": ret_c, "CmB": ret_c - ret_b,
                "ddA": _mdd(nav_a), "ddC": _mdd(nav_c),
            })

    L = ["## E15：顶部触发防守切换（P19）预登记验证", "",
         f"- 周期总数 {n_cycles} · 周期内触发 {n_trig} · 剔除（执行日越界/代理覆盖不足）{n_dropped}"
         f" · **有效样本 {len(rows)}**",
         f"- 防守代理 {DEFENSE}（登记固定）；执行=触发次日收盘；单边成本 {COST_SIDE:.2%}",
         ""]
    if rows:
        rows.sort(key=lambda r: r["trig"])
        L += ["| 指数 | 触发日 | 周期终点 | 持有日 | A持有 | C防守 | C−B | A回撤 | C回撤 |",
              "|---|---|---|---:|---:|---:|---:|---:|---:|"]
        for r in rows:
            L.append(f"| {r['idx']} | {r['trig']} | {r['end']} | {r['days']} "
                     f"| {r['A']:+.1%} | {r['C']:+.1%} | {r['CmB']:+.1%} "
                     f"| -{r['ddA']:.1%} | -{r['ddC']:.1%} |")
        cmb = np.array([r["CmB"] for r in rows])
        dda = np.array([r["ddA"] for r in rows])
        ddc = np.array([r["ddC"] for r in rows])
        mid = len(rows) // 2
        h1, h2 = cmb[:mid].mean(), cmb[mid:].mean()
        c1 = len(rows) >= 8
        c2 = cmb.mean() >= 0.03 and (cmb > 0).mean() >= 0.60
        c3 = len(rows) >= 2 and h1 * h2 > 0
        # ④ B 的回撤改善 = ddA − 0 = ddA；要求 (ddA−ddC)均值 ≥ 0.7×(ddA)均值
        c4 = (dda - ddc).mean() >= 0.70 * dda.mean()
        L += ["", "## 预登记判据裁决（写死于 P19 段）", "",
              "| 判据 | 实测 | 门槛 | 结论 |", "|---|---|---|---|",
              f"| ① 有效触发周期数 | {len(rows)} | ≥8 | {'✅' if c1 else '❌'} |",
              f"| ② C−B 均值 / 胜率 | {cmb.mean():+.1%} / {(cmb > 0).mean():.0%} "
              f"| ≥+3pp 且 ≥60% | {'✅' if c2 else '❌'} |",
              f"| ③ 分半方向一致 | 前半 {h1:+.1%} / 后半 {h2:+.1%} | 同号 | {'✅' if c3 else '❌'} |",
              f"| ④ 回撤改善保留 | {(dda - ddc).mean():.1%} vs 0.7×{dda.mean():.1%}"
              f"={0.7 * dda.mean():.1%} | ≥ | {'✅' if c4 else '❌'} |",
              "",
              ("**四条全过 → 建议晋升：P16 减半行追加防守去向提示（人工执行、标的投顾定，owner 拍板）**"
               if all([c1, c2, c3, c4]) else
               "**未全过 → 维持现金去向，分项如实记录**")]
    else:
        L.append("⚠️ 无有效样本（触发点均无防守代理覆盖），E15 无法判定——考虑回填更早的红利低波数据后重跑")
    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
