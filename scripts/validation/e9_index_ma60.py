"""E9：指数 MA60 事件闸 —— 有效跌破是不是趋势结束、回踩是不是好买点？(预登记)

H0（零假设）：指数「有效跌破 MA60」后的前瞻收益与随机日无异，
  「MA60 上行时回踩 MA60」买入的胜率与随机日无异 —— 若 H0 被推翻
  （跌破事件后 60 日前瞻显著更差且跨指数方向一致；回踩买点 60 日胜率 ≥55%
  且优于基线），则 P12 事件闸值得开影子。

出处：life-teachers 重远投资观 2026-07-10《散户亏损失控》：
  「趋势市中，大型指数有效跌破60日均线时，趋势结束的概率最高」
  「在上升趋势上，指数调整到60日均线时，买入的止损代价最小」（博主口径，本脚本验证）

方法（事件研究，事件窗口不重叠防伪独立）：
  - 指数：默认 000300.SH（沪深300）+ 399006.SZ（创业板指），index_daily 全历史
  - 跌破事件：收盘 < MA60×(1−x)，x ∈ {0.5%, 1%, 2%}，且事件前一日不处于跌破态
    （新鲜跌破）；取事件后 20/60 交易日前瞻收益；事件间隔 < 窗口的丢弃后者
  - 回踩事件：MA60 上行（今值 > 20 日前值）且 |收盘/MA60 − 1| ≤ 1%，同样新鲜+不重叠
  - 基线：全样本随机日（同窗口不重叠抽样）的前瞻收益分布
  - 统计：Welch t（事件 vs 基线）；样本 <8 个事件只列示不判定（功效不足）

过关判据（预登记，写死勿改）：
  跌破臂：60 日前瞻均值差 t ≤ −2 且两指数方向一致；
  回踩臂：60 日胜率 ≥ 55% 且 ≥ 基线胜率 + 5pp，至少一个指数达标、另一不反向。

用法：python scripts/validation/e9_index_ma60.py [--db ...] [--out results/e9.md]
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

INDEXES = ["000300.SH", "399006.SZ"]
BREAK_PCTS = [0.005, 0.01, 0.02]
HORIZONS = [20, 60]
PULLBACK_BAND = 0.01
MIN_EVENTS = 8


def _index_closes(repo, code: str) -> pd.Series:
    df = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c ORDER BY trade_date",
        {"c": code})
    if df.empty:
        return pd.Series(dtype=float)
    return pd.to_numeric(df.set_index("trade_date")["close"], errors="coerce").dropna()


def _forward_ret(c: pd.Series, i: int, h: int) -> float | None:
    if i + h >= len(c):
        return None
    base = float(c.iloc[i])
    return float(c.iloc[i + h]) / base - 1.0 if base > 0 else None


def _dedup(idx: list[int], min_gap: int) -> list[int]:
    """事件下标去重叠：相邻事件间隔 < min_gap 的丢弃后者。"""
    out: list[int] = []
    for i in idx:
        if not out or i - out[-1] >= min_gap:
            out.append(i)
    return out


def _welch_t(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 2 or len(b) < 2:
        return float("nan")
    va, vb = a.var(ddof=1) / len(a), b.var(ddof=1) / len(b)
    denom = np.sqrt(va + vb)
    return float((a.mean() - b.mean()) / denom) if denom > 0 else float("nan")


def _baseline(c: pd.Series, h: int) -> np.ndarray:
    """基线：全样本每隔 h 日取一个不重叠前瞻窗口。"""
    rets = [_forward_ret(c, i, h) for i in range(60, len(c) - h, h)]
    return np.array([r for r in rets if r is not None])


def analyze_index(repo, code: str, lines: list[str]) -> dict:
    c = _index_closes(repo, code)
    verdict = {"break": None, "pullback": None}
    if len(c) < 120:
        lines.append(f"- {code}：样本不足（{len(c)} 根），跳过")
        return verdict
    ma60 = c.rolling(60).mean()
    lines.append(f"\n### {code}（{c.index[0]}~{c.index[-1]}，{len(c)} 交易日）")

    # ── 跌破臂 ──────────────────────────────────────────────
    lines.append("\n| 跌破阈值x | 窗口 | 事件数 | 事件均值 | 基线均值 | Welch t |")
    lines.append("|---|---|---|---|---|---|")
    best_t60 = None
    for x in BREAK_PCTS:
        below = c < ma60 * (1 - x)
        fresh = below & ~below.shift(1, fill_value=False)     # 新鲜跌破
        idx_all = [i for i, f in enumerate(fresh.values) if f and i >= 60]
        for h in HORIZONS:
            idx = _dedup(idx_all, h)
            ev = np.array([r for i in idx if (r := _forward_ret(c, i, h)) is not None])
            base = _baseline(c, h)
            if len(ev) < MIN_EVENTS:
                lines.append(f"| {x:.1%} | {h}日 | {len(ev)} | —（功效不足） | "
                             f"{base.mean():+.2%} | — |")
                continue
            t = _welch_t(ev, base)
            lines.append(f"| {x:.1%} | {h}日 | {len(ev)} | {ev.mean():+.2%} | "
                         f"{base.mean():+.2%} | {t:+.1f} |")
            if h == 60 and x == 0.01:
                best_t60 = t
    verdict["break"] = best_t60

    # ── 回踩臂（MA60 上行 + 贴近 MA60）────────────────────────
    rising = ma60 > ma60.shift(20)
    near = (c / ma60 - 1).abs() <= PULLBACK_BAND
    cond = (rising & near).fillna(False)
    fresh = cond & ~cond.shift(1, fill_value=False)
    idx_all = [i for i, f in enumerate(fresh.values) if f and i >= 80]
    lines.append("\n| 回踩买点 | 窗口 | 事件数 | 胜率 | 基线胜率 | 事件均值 |")
    lines.append("|---|---|---|---|---|---|")
    win60 = base_win60 = None
    for h in HORIZONS:
        idx = _dedup(idx_all, h)
        ev = np.array([r for i in idx if (r := _forward_ret(c, i, h)) is not None])
        base = _baseline(c, h)
        if len(ev) < MIN_EVENTS:
            lines.append(f"| MA60上行±{PULLBACK_BAND:.0%} | {h}日 | {len(ev)} | "
                         f"—（功效不足） | {(base > 0).mean():.0%} | — |")
            continue
        wr, bwr = (ev > 0).mean(), (base > 0).mean()
        lines.append(f"| MA60上行±{PULLBACK_BAND:.0%} | {h}日 | {len(ev)} | "
                     f"{wr:.0%} | {bwr:.0%} | {ev.mean():+.2%} |")
        if h == 60:
            win60, base_win60 = wr, bwr
    verdict["pullback"] = (win60, base_win60)
    return verdict


def run(repo) -> str:
    lines = ["## E9 —— 指数 MA60 事件闸：有效跌破 & 回踩买点（P12 预登记验证）", ""]
    lines.append("- 口径：事件窗口不重叠、新鲜事件、Welch t vs 全样本不重叠基线；"
                 f"事件 <{MIN_EVENTS} 个只列示不判定。主判据窗口：60 日、x=1%。")
    verdicts = {}
    for code in INDEXES:
        try:
            verdicts[code] = analyze_index(repo, code, lines)
        except Exception as e:  # noqa: BLE001
            lines.append(f"- {code} 分析失败：{e}")
            verdicts[code] = {"break": None, "pullback": None}

    # ── 裁决（预登记判据）──────────────────────────────────
    lines.append("\n### 裁决（预登记判据：跌破 t≤−2 双指数同向；回踩胜率≥55% 且超基线5pp）")
    bts = [v["break"] for v in verdicts.values() if v["break"] is not None]
    break_pass = len(bts) == len(INDEXES) and all(t <= -2 for t in bts)
    pbs = [v["pullback"] for v in verdicts.values()
           if v["pullback"] and v["pullback"][0] is not None]
    pb_ok = [wr >= 0.55 and wr >= bwr + 0.05 for wr, bwr in pbs]
    pb_bad = [wr < bwr for wr, bwr in pbs]
    pullback_pass = bool(pbs) and any(pb_ok) and not any(pb_bad)
    lines.append(f"- 跌破臂：{'✅ 过关' if break_pass else '❌ 未过关/功效不足'}"
                 f"（60日 x=1% t: {['%.1f' % t for t in bts] or '样本不足'}）")
    lines.append(f"- 回踩臂：{'✅ 过关' if pullback_pass else '❌ 未过关/功效不足'}")
    lines.append(f"- **P12 裁决**：{'✅ 可进影子' if (break_pass or pullback_pass) else '⏳ hold（等数据回填至 2015 提功效）'}"
                 "——过关臂对应开关单独晋升，未过关臂保持关闭。")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="E9 指数 MA60 事件闸验证（P12 预登记）")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = get_repo(args.db)
    md = run(repo)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
