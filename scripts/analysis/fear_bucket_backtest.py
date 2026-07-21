"""恐慌分桶 + 极度恐慌买宽基 回测（用户命题·E17 预登记·只读不落库）。

用户命题两问：
  ① 恐慌分桶：越恐慌是否越该重仓？（75-80 / 80-85 / 85+ 各自的前瞻收益形状）
  ② 策略：极度恐慌(≥75)只买宽基指数（沪深300 000300 / 创业板指 399006 / 科创50 000688），
     恐慌回落卖。效果如何？宽基篮子 vs 单指数、vs 个股 谁更稳？

## 判据预登记（E17·跑数前写死，勿按结果回调）

信号：市场级 fear（benchmark 000300，与生产同口径）；fear(t) EOD 才知 → **次日收盘执行，无前视**。
  序列优先 fear_daily、不足用 fear_gauge 逐日重算（同函数无前视）。

分桶：B1=[75,80) / B2=[80,85) / B3=[85,100]。两口径并报：
  - 日级：每个 fear 落某桶的交易日，测 5/10/20/40 日前瞻收益（样本多、自相关高）；
  - episode 级：每次 fresh-cross≥75 按该段峰值 fear 归桶，测其后前瞻（样本少、独立）。

宽基篮子：三指数等权（各归一到入场日=1，短持有近似等权）。

E17 四条判据：
  ① 分桶单调性：宽基池化(3指数合并) 20 日前瞻收益 B1≤B2≤B3 且每桶 n≥5
     → 成立=支持"越恐慌越重仓"线性阶梯；不成立(驼峰/递减)=仓位曲线非线性、≥某档要减速。
  ② 极值买宽基的前瞻边际：池化 5/10/20 日前瞻超额>0（vs 无条件）且至少两窗口胜率≥55%。
  ③ 宽基篮子择时(≥75买/≤50卖)：胜率≥55% 且 平均单笔>0。
  ④ 宽基更稳：篮子最差单笔回撤 优于 三单指数各自最差（分散是否降尾部）。
  ≥3 过＝方向成立、可据此定仓位阶梯并推进 P22；否则记研究、维持只监测不自动交易。

只读 index_daily/fear_daily/stock_daily，不落库、不改生产。需在有 DB 环境（Actions/FC）跑。
  python scripts/analysis/fear_bucket_backtest.py [--recompute-days 900] [--out results/fear_bucket.md]
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
from scripts.analysis.fear_meanrev_backtest import (  # noqa: E402
    _agg, _episode_trades, _fear_series, _index_closes,
)

BROAD = {"000300.SH": "沪深300", "399006.SZ": "创业板指", "000688.SH": "科创50"}
BUCKETS = [(75, 80, "75-80"), (80, 85, "80-85"), (85, 101, "85+")]
FWD_WINS = [5, 10, 20, 40]
BUY, SELL = 75, 50


def _bucket_of(f: float) -> str | None:
    for lo, hi, name in BUCKETS:
        if lo <= f < hi:
            return name
    return None


def _daylevel_buckets(fear: pd.Series, closes: dict[str, pd.Series]) -> dict:
    """日级：把每个（恐慌落桶的）交易日的前瞻收益归桶，跨指数池化。"""
    acc = {name: {w: [] for w in FWD_WINS} for _, _, name in BUCKETS}
    uncond = {w: [] for w in FWD_WINS}
    for code, px in closes.items():
        dates = [d for d in px.index if d in fear.index]
        dates.sort()
        f = fear.reindex(dates).to_numpy(dtype=float)
        p = px.reindex(dates).to_numpy(dtype=float)
        n = len(dates)
        for w in FWD_WINS:
            for i in range(n - w):
                r = p[i + w] / p[i] - 1
                uncond[w].append(r)
                b = _bucket_of(f[i])
                if b:
                    acc[b][w].append(r)
    return acc, uncond


def _episode_buckets(fear: pd.Series, closes: dict[str, pd.Series]) -> dict:
    """episode 级：每次 fresh-cross≥75，按该段峰值 fear 归桶，测入场后前瞻。"""
    acc = {name: {w: [] for w in FWD_WINS} for _, _, name in BUCKETS}
    for code, px in closes.items():
        dates = [d for d in px.index if d in fear.index]
        dates.sort()
        f = fear.reindex(dates).to_numpy(dtype=float)
        p = px.reindex(dates).to_numpy(dtype=float)
        n = len(dates)
        i = 1
        while i < n:
            if f[i] >= BUY and f[i - 1] < BUY:
                j = i
                peak = f[i]
                while j < n and f[j] >= SELL:      # 该恐慌段（≥50 维持）
                    peak = max(peak, f[j]); j += 1
                b = _bucket_of(min(peak, 100.0)) or ("85+" if peak >= 85 else None)
                if b:
                    for w in FWD_WINS:
                        if i + w < n:
                            acc[b][w].append(p[i + w] / p[i] - 1)
                i = j
            else:
                i += 1
    return acc


def _basket(closes: dict[str, pd.Series]) -> pd.Series:
    """三指数等权篮子（各归一），取三者共同交易日。"""
    common = None
    for px in closes.values():
        s = set(px.index)
        common = s if common is None else (common & s)
    common = sorted(common) if common else []
    if not common:
        return pd.Series(dtype=float)
    norm = []
    for px in closes.values():
        c = px.reindex(common).astype(float)
        norm.append(c / c.iloc[0])
    basket = sum(norm) / len(norm)
    return basket


def _fmt_bucket_table(acc: dict, uncond: dict | None, L: list, title: str) -> None:
    L.append(f"\n**{title}**：")
    head = "| 桶 | " + " | ".join(f"{w}日(均/胜/n)" for w in FWD_WINS) + " |"
    L.append(head)
    L.append("|" + "---|" * (len(FWD_WINS) + 1))
    for _, _, name in BUCKETS:
        cells = []
        for w in FWD_WINS:
            a = np.array(acc[name][w])
            if len(a):
                cells.append(f"{a.mean():+.1%}/{(a > 0).mean():.0%}/{len(a)}")
            else:
                cells.append("–")
        L.append(f"| {name} | " + " | ".join(cells) + " |")
    if uncond:
        cells = []
        for w in FWD_WINS:
            a = np.array(uncond[w])
            cells.append(f"{a.mean():+.1%}/{(a > 0).mean():.0%}/{len(a)}" if len(a) else "–")
        L.append(f"| 无条件 | " + " | ".join(cells) + " |")


def run(repo: BaseRepository, recompute_days: int) -> str:
    fear, src = _fear_series(repo, recompute_days)
    L = ["# 恐慌分桶 + 极度恐慌买宽基 回测（沪深300/创业板指/科创50）", ""]
    L.append(f"- 恐慌序列：{src}")
    if len(fear) < 120:
        L.append("\n⚠️ 恐慌样本不足，先 bump ops/fear-backfill.trigger 或加大 --recompute-days。")
        return "\n".join(L)
    closes = {c: _index_closes(repo, c) for c in BROAD}
    closes = {c: s for c, s in closes.items() if not s.empty}
    if not closes:
        L.append("\n⚠️ 宽基指数 index_daily 覆盖不足。")
        return "\n".join(L)
    names = "、".join(f"{BROAD[c]}({c})" for c in closes)
    L.append(f"- 宽基池：{names}；桶 75-80/80-85/85+；次日收盘执行（无前视）。")

    # ① 分桶（日级 + episode 级）
    acc_d, uncond = _daylevel_buckets(fear, closes)
    _fmt_bucket_table(acc_d, uncond, L, "分桶·日级前瞻收益（池化3指数，均值/胜率/样本）")
    acc_e = _episode_buckets(fear, closes)
    _fmt_bucket_table(acc_e, None, L, "分桶·episode级（每次入场按段峰值归桶）")

    # 单调性判定（日级 20 日）
    m20 = {name: (np.mean(acc_d[name][20]) if acc_d[name][20] else float("nan"))
           for _, _, name in BUCKETS}
    n20 = {name: len(acc_d[name][20]) for _, _, name in BUCKETS}
    vals = [m20["75-80"], m20["80-85"], m20["85+"]]
    mono = (all(n20[k] >= 5 for k in n20)
            and np.all(np.diff([v for v in vals if np.isfinite(v)]) >= 0)
            and sum(np.isfinite(vals)) == 3)
    shape = ("单调递增（越恐慌越强）" if mono else
             "非单调" + ("（驼峰：中间高）" if np.isfinite(vals[1]) and vals[1] == max(v for v in vals if np.isfinite(v)) else "（≥某档转弱）"))
    L.append(f"\n- 20日前瞻均值 桶序：75-80 {m20['75-80']:+.1%}(n{n20['75-80']}) → "
             f"80-85 {m20['80-85']:+.1%}(n{n20['80-85']}) → 85+ {m20['85+']:+.1%}(n{n20['85+']}) "
             f"⟹ **{shape}**")

    # ③ 宽基篮子择时 + 各单指数对照
    basket = _basket(closes)
    L.append("\n**极度恐慌买宽基·择时回测（≥75买/≤50卖，一 episode 一进出）**：")
    L.append("| 标的 | 笔数 | 胜率 | 平均单笔 | 复利 | 均持天 | 在场占比 | 同期买入持有 | 最差笔 |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    worst = {}
    for label, px in [("宽基等权篮子", basket)] + [(BROAD[c], closes[c]) for c in closes]:
        tr, dates = _episode_trades(fear, px, BUY, SELL)
        a = _agg(tr, dates, px)
        if a["n"] == 0:
            L.append(f"| {label} | 0 | – | – | – | – | – | – | – |")
            continue
        worst[label] = a["worst"]
        L.append(f"| {label} | {a['n']}{'*' if a['n_open'] else ''} | {a['win']:.0%} | "
                 f"{a['mean']:+.1%} | {a['comp']:+.1%} | {a['avg_days']:.0f} | {a['in_frac']:.0%} | "
                 f"{a['bh_active']:+.1%} | {a['worst']:+.1%} |")
    L.append("  （* = 含 1 笔样本末未平仓）")

    # ② 前瞻边际（池化 ≥75 vs 无条件）
    edge_ok = 0
    L.append("\n**极值(≥75)买宽基 vs 无条件·池化前瞻超额**：")
    L.append("| 窗 | 极值后(均/胜/n) | 无条件均值 | 超额 |")
    L.append("|---|---|---|---|")
    for w in FWD_WINS:
        cond = [r for _, _, nm in BUCKETS for r in acc_d[nm][w]]
        c = np.array(cond); u = np.array(uncond[w])
        exc = (c.mean() - u.mean()) if len(c) and len(u) else float("nan")
        L.append(f"| {w}日 | {c.mean():+.1%}/{(c > 0).mean():.0%}/{len(c)} | {u.mean():+.1%} | {exc:+.1%} |"
                 if len(c) else f"| {w}日 | – | – | – |")
        if len(c) and exc > 0 and (c > 0).mean() >= 0.55:
            edge_ok += 1

    # ④ 宽基更稳
    basket_worst = worst.get("宽基等权篮子")
    single_worst = [worst[BROAD[c]] for c in closes if BROAD[c] in worst]
    steadier = (basket_worst is not None and single_worst
                and basket_worst >= max(single_worst))   # 回撤更浅（更接近0）

    # ── 判据裁决 ──
    L.append("\n## 判据裁决（E17 预登记四条）")
    cond1 = mono
    cond2 = edge_ok >= 2
    bt, bd = _episode_trades(fear, basket, BUY, SELL)
    ba = _agg(bt, bd, basket)
    cond3 = ba.get("n", 0) > 0 and ba.get("win", 0) >= 0.55 and ba.get("mean", -1) > 0
    cond4 = bool(steadier)
    passed = sum([cond1, cond2, cond3, cond4])
    L.append(f"- ① 分桶单调递增（支持越恐慌越重仓）：{'✅' if cond1 else '❌ '+shape}")
    L.append(f"- ② 极值买宽基前瞻超额（≥2窗口正且胜率≥55%）：{'✅' if cond2 else '❌'}")
    L.append(f"- ③ 宽基篮子择时胜率≥55%且平均>0：{'✅' if cond3 else '❌'}")
    L.append(f"- ④ 宽基篮子比单指数尾部更稳（最差笔更浅）：{'✅' if cond4 else '❌'}")
    L.append(f"- **{passed}/4 过**——≥3 则方向成立、据此定仓位阶梯并推进 P22；否则记研究、维持只监测不自动交易。")

    L.append("\n### 仓位阶梯含义")
    if cond1:
        L.append("- 分桶单调递增 → 仓位可随恐慌线性加码（75-80 轻、80-85 中、85+ 重）。")
    else:
        L.append("- 分桶**非单调** → **不可**照\"越恐慌越重仓\"线性加码；" + shape +
                 "，≥85 档常是系统性踩踏中途，仓位应封顶/减速、更分散、留子弹补跌。")

    L.append("\n### 注意项")
    L.append("- 指数不可直接买；实盘用 510300/159919(沪深300)、159915(创业板)、588000(科创50) ETF，另计跟踪误差+费率。")
    L.append("- 恐慌为 EOD 慢变量，已强制次日收盘执行、无前视；宽基篮子=三指数等权归一近似。")
    L.append("- 单市场历史 n 有限（尤其 85+ 桶与 episode 级样本少），非统计裁决；作方向探索，晋升须多周期样本外复核。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="恐慌分桶+极度恐慌买宽基回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--recompute-days", type=int, default=900)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())
    md = run(repo, args.recompute_days)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
