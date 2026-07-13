"""全样本多周期·离场策略对照回测（把 chinext n=1 个案升级为统计裁决）。

背景：单指数单周期（创业板本轮）只能做直觉校准。本脚本在**全样本（2015→今）× 多只宽基指数**
上，自动检测**所有** MA60 牛市周期，对每个周期用同一入场点（首次突破 MA60、MA60 上行）跑同一组离场：

  A  重远·裸破 MA60（收盘破线即清）
  A' 重远·破 MA60（带 1% 缓冲）
  B  系统盈利模型（生产 risk.py 单次首离场）
  B2 系统 + 再入场（reentry 逐段复利，公平对比重远"持有到破位"）
  C  顶部识别探测器（P16 候选，判据预登记）
  基线 买入持有到周期终点

然后**跨周期聚合**：中位持有收益 / 平均自峰值回撤(give-back) / 平均捕获率 / 对买入持有的胜率，
并做三组对抗性对照：
  (1) 控回撤：系统 B 的 give-back 是否系统性优于重远 A'（第1题）
  (2) 总收益：系统 B2 是否能赢买入持有（还是紧止损在多数周期反而跑输）
  (3) E9 v2 核心：重远 MA60 的捕获率，在"干净趋势市"周期 vs "震荡市"周期是否有显著差异
      —— 用周期内"在 MA60 上方天数占比"中位数切分。这是 E9 无条件检验未过关后的条件化假设。

预登记（跑数前写死，勿按结果回调）：周期须 峰值收益≥MIN_CYCLE_PEAK 且 时长≥MIN_DAYS 才计入；
区制容忍 base_gap 日短暂回踩；C 判据沿用 chinext 脚本常量。

只读、不落库、不改生产。需在有 DB 的环境（Actions/FC）跑。
  python scripts/analysis/multicycle_exit_backtest.py [--db ...] [--out results/multicycle.md]
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
from invest_model.portfolio.risk import RiskConfig  # noqa: E402
from chinext_cycle_backtest import (  # noqa: E402  复用生产口径的离场函数，不重写
    _load, _stats, _exit_system, _exit_top, _system_with_reentry, _exit_reain_ma60,
)

# 预登记周期资格（跑数前写死）
MIN_CYCLE_PEAK = 0.15    # 周期内峰值收益须 ≥15% 才算"牛市级"周期（滤掉微幅波动）
MIN_DAYS = 30            # 周期时长须 ≥30 交易日
BASE_GAP = 10            # 区制容忍：牛市途中 ≤10 连续日跌破 MA60 视作回踩，不算周期结束

# 预登记宽基指数白名单（用库里存在的那些）
INDEX_CANDIDATES = [
    ("000001.SH", "上证综指"), ("399001.SZ", "深证成指"), ("399006.SZ", "创业板指"),
    ("000300.SH", "沪深300"), ("000905.SH", "中证500"), ("000016.SH", "上证50"),
    ("000688.SH", "科创50"), ("399005.SZ", "中小板指"), ("000852.SH", "中证1000"),
]


def _detect_all_cycles(df: pd.DataFrame) -> list[tuple[int, int]]:
    """全样本检测所有 MA60 牛市周期。

    起点：收盘上穿 MA60（昨破今站上）且 MA60 上行（ma60[i]>ma60[i-5]）。
    终点：其后第一段"持续 >BASE_GAP 日跌破 MA60"的**首破日**（有效跌破）；≤BASE_GAP 日回踩不算结束。
    资格：峰值收益≥MIN_CYCLE_PEAK 且 时长≥MIN_DAYS。周期不重叠，收尾后从终点续扫。
    """
    c = df["close"].to_numpy(dtype=float)
    ma60 = df["close"].rolling(60).mean().to_numpy()
    n = len(c)
    finite = np.isfinite(ma60)
    below = (~finite) | (c < ma60)
    cycles: list[tuple[int, int]] = []
    i = 65
    while i < n - 1:
        rising = finite[i] and finite[i - 5] and ma60[i] > ma60[i - 5]
        breakout = finite[i] and (not below[i]) and below[i - 1]
        if breakout and rising:
            s = i
            gap = 0
            e = None
            j = s + 1
            while j < n:
                if below[j]:
                    gap += 1
                    if gap > BASE_GAP:
                        e = j - gap + 1          # 该段首个破位日 = 有效跌破日
                        break
                else:
                    gap = 0
                j += 1
            if e is None:
                e = n - 1                        # 仍在进行中的周期，止于样本末
            peak = c[s:e + 1].max() / c[s] - 1
            if (e - s) >= MIN_DAYS and peak >= MIN_CYCLE_PEAK:
                cycles.append((s, e))
                i = e + 1
                continue
            i = max(e, s) + 1
            continue
        i += 1
    return cycles


def _above_frac(df: pd.DataFrame, s: int, e: int) -> float:
    """周期内收盘在 MA60 上方的天数占比 —— 趋势"干净度"代理（越高=越干净的趋势）。"""
    c = df["close"].to_numpy(dtype=float)[s:e + 1]
    ma60 = df["close"].rolling(60).mean().to_numpy()[s:e + 1]
    ok = np.isfinite(ma60)
    if ok.sum() == 0:
        return float("nan")
    return float((c[ok] >= ma60[ok]).mean())


def _run_cycle(df: pd.DataFrame, ma60: np.ndarray, s: int, e: int, cfg: RiskConfig) -> dict:
    """单周期跑全部策略，返回各策略指标 + 周期元信息。"""
    ea, ra = _exit_reain_ma60(df, ma60, s, e, 0.0)
    ea1, ra1 = _exit_reain_ma60(df, ma60, s, e, 0.01)
    eb, rb = _exit_system(df, s, cfg)
    eb = min(eb, e)                              # 不越过周期终点，公平对齐
    ec, rc = _exit_top(df.iloc[:e + 1], s)
    ec = min(ec, e)
    A = _stats(df["close"], s, ea, "A", ra)
    A1 = _stats(df["close"], s, ea1, "A'", ra1)
    B = _stats(df["close"], s, eb, "B", rb)
    C = _stats(df["close"], s, ec, "C", rc)
    b2 = _system_with_reentry(df, s, e, cfg)
    hold_ret = float(df["close"].iloc[e]) / float(df["close"].iloc[s]) - 1
    peak_ret = float(df["close"].iloc[s:e + 1].max()) / float(df["close"].iloc[s]) - 1
    c_fired = "未" not in rc                       # C 是否真触发（"至样本末未现顶部特征"=未触发）
    return {
        "s": s, "e": e, "days": e - s,
        "start": str(df["trade_date"].iloc[s]), "end": str(df["trade_date"].iloc[e]),
        "peak_ret": peak_ret, "hold_ret": hold_ret,
        "above_frac": _above_frac(df, s, e),
        "A": A, "A1": A1, "B": B, "C": C, "b2": b2["总收益"], "c_fired": c_fired,
    }


def _agg(vals: list[float]) -> tuple[float, float]:
    a = np.array([v for v in vals if v is not None and np.isfinite(v)], dtype=float)
    if a.size == 0:
        return float("nan"), float("nan")
    return float(np.median(a)), float(np.mean(a))


def run(repo: BaseRepository) -> str:
    codes = repo.read_sql("SELECT DISTINCT code FROM index_daily", {})
    have = set(codes["code"].tolist()) if not codes.empty else set()
    universe = [(c, name) for c, name in INDEX_CANDIDATES if c in have]

    L = ["# 全样本多周期·离场策略对照回测（重远 MA60 vs 系统 vs 顶部识别）", ""]
    L.append(f"- 指数池（库中存在者）：{', '.join(f'{n}({c})' for c, n in universe) or '（无）'}")
    L.append(f"- 预登记周期资格：峰值收益≥{MIN_CYCLE_PEAK:.0%} 且 时长≥{MIN_DAYS}日；"
             f"区制回踩容忍≤{BASE_GAP}日；C 判据沿用 chinext 常量")
    if not universe:
        L.append("\n⚠️ index_daily 无宽基指数样本，先 bump ops/index-backfill.trigger 回填。")
        return "\n".join(L)

    cfg = RiskConfig()
    all_cycles: list[dict] = []
    per_index_rows = []
    for code, name in universe:
        df = _load(repo, code)
        if len(df) < 200:
            per_index_rows.append(f"| {name} | {code} | 样本不足({len(df)}) | — |")
            continue
        ma60 = df["close"].rolling(60).mean().to_numpy()
        cyc = _detect_all_cycles(df)
        span = f"{df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}"
        per_index_rows.append(f"| {name} | {code} | {span}（{len(df)}日） | {len(cyc)} |")
        for s, e in cyc:
            r = _run_cycle(df, ma60, s, e, cfg)
            r["code"], r["name"] = code, name
            all_cycles.append(r)

    L.append("\n## 一、样本与周期检测")
    L.append("\n| 指数 | 代码 | 样本区间 | 检出周期数 |")
    L.append("|---|---|---|---|")
    L.extend(per_index_rows)
    L.append(f"\n**共检出 {len(all_cycles)} 个合格牛市周期**（跨 {len(universe)} 只宽基）。")

    if not all_cycles:
        L.append("\n⚠️ 未检出合格周期，检查回填数据或放宽预登记阈值（阈值本次不动，维持写死）。")
        return "\n".join(L)

    # 全周期清单
    L.append("\n<details><summary>全部周期明细（点开）</summary>\n")
    L.append("| 指数 | 入场 | 终点 | 天数 | 峰值 | 持有到终点 | MA60上方占比 |")
    L.append("|---|---|---|---|---|---|---|")
    for r in sorted(all_cycles, key=lambda x: (x["name"], x["start"])):
        L.append(f"| {r['name']} | {r['start']} | {r['end']} | {r['days']} | "
                 f"{r['peak_ret']:+.0%} | {r['hold_ret']:+.0%} | {r['above_frac']:.0%} |")
    L.append("\n</details>")

    # 二、跨周期聚合
    def col(key, sub="持有收益"):
        return [r[key][sub] if isinstance(r[key], dict) else r[key] for r in all_cycles]

    n = len(all_cycles)
    hold_base = [r["hold_ret"] for r in all_cycles]
    L.append("\n## 二、跨周期聚合（每策略在 %d 个周期上的表现）" % n)
    L.append("\n| 策略 | 中位持有收益 | 平均持有收益 | 平均自峰值回撤 | 平均捕获率 | 对买入持有胜率 |")
    L.append("|---|---|---|---|---|---|")

    def winrate(vals):
        w = sum(1 for v, h in zip(vals, hold_base) if np.isfinite(v) and v > h)
        return w / n

    for key, disp in [("A", "A 重远·裸破MA60"), ("A1", "A' 重远·破MA60(1%)"),
                      ("B", "B 系统(单段)"), ("C", "C 顶部识别")]:
        rets = col(key, "持有收益")
        gb = col(key, "自峰值回撤")
        cap = col(key, "捕获率")
        med_r, mean_r = _agg(rets)
        _, mean_gb = _agg(gb)
        _, mean_cap = _agg(cap)
        L.append(f"| {disp} | {med_r:+.1%} | {mean_r:+.1%} | {mean_gb:+.1%} | "
                 f"{mean_cap:.0%} | {winrate(rets):.0%} |")
    # B2 与买入持有基线
    b2v = [r["b2"] for r in all_cycles]
    med_b2, mean_b2 = _agg(b2v)
    med_h, mean_h = _agg(hold_base)
    # 买入持有到终点的自峰值回撤 = (1+持有)/(1+峰值)-1；捕获率 = 持有/峰值
    hold_gb = [(1 + r["hold_ret"]) / (1 + r["peak_ret"]) - 1 for r in all_cycles]
    hold_cap = [r["hold_ret"] / r["peak_ret"] if r["peak_ret"] > 0 else float("nan") for r in all_cycles]
    L.append(f"| B2 系统+再入场 | {med_b2:+.1%} | {mean_b2:+.1%} | — | — | {winrate(b2v):.0%} |")
    L.append(f"| 基线·买入持有到终点 | {med_h:+.1%} | {mean_h:+.1%} | "
             f"{_agg(hold_gb)[1]:+.1%} | {_agg(hold_cap)[1]:.0%} | — |")

    # 三、三组对抗性对照
    L.append("\n## 三、三组对抗性对照")

    # (1) 控回撤：B give-back vs A' give-back（越接近0越好）
    pair = [(r["B"]["自峰值回撤"], r["A1"]["自峰值回撤"]) for r in all_cycles]
    b_better = sum(1 for b, a in pair if np.isfinite(b) and np.isfinite(a) and b > a)  # b>a 即回吐更少
    L.append(f"\n**(1) 控回撤（第1题：少把顶部利润还回去）**：系统 B 自峰值回撤在 "
             f"{b_better}/{n} 个周期上优于重远 A'（B 均值 {_agg([p[0] for p in pair])[1]:+.1%} "
             f"vs A' 均值 {_agg([p[1] for p in pair])[1]:+.1%}）。")

    # (2) 总收益：B2 vs 买入持有
    b2_win = sum(1 for v, h in zip(b2v, hold_base) if np.isfinite(v) and v > h)
    L.append(f"\n**(2) 总收益（紧止损+reentry 能否赢买入持有）**：B2 在 {b2_win}/{n} 个周期上跑赢买入持有"
             f"（B2 均值 {mean_b2:+.1%} vs 买入持有 {mean_h:+.1%}）。")

    # (3) E9 v2 核心：重远捕获率 干净趋势 vs 震荡（按 above_frac 中位切分）
    af = np.array([r["above_frac"] for r in all_cycles], dtype=float)
    med_af = np.nanmedian(af)
    clean = [r for r in all_cycles if np.isfinite(r["above_frac"]) and r["above_frac"] >= med_af]
    choppy = [r for r in all_cycles if np.isfinite(r["above_frac"]) and r["above_frac"] < med_af]
    cap_clean = _agg([r["A1"]["捕获率"] for r in clean])[1]
    cap_choppy = _agg([r["A1"]["捕获率"] for r in choppy])[1]
    gb_clean = _agg([r["A1"]["自峰值回撤"] for r in clean])[1]
    gb_choppy = _agg([r["A1"]["自峰值回撤"] for r in choppy])[1]
    L.append(f"\n**(3) E9 v2 核心假设（重远 MA60 在干净趋势市更管用）**："
             f"按周期 MA60 上方天数占比中位数 {med_af:.0%} 切分——")
    L.append(f"- 干净趋势组（占比≥{med_af:.0%}，n={len(clean)}）：重远 A' 平均捕获率 **{cap_clean:.0%}**，"
             f"平均自峰值回撤 {gb_clean:+.1%}")
    L.append(f"- 震荡组（占比<{med_af:.0%}，n={len(choppy)}）：重远 A' 平均捕获率 **{cap_choppy:.0%}**，"
             f"平均自峰值回撤 {gb_choppy:+.1%}")
    L.append(f"- 差值（干净−震荡）：捕获率 {cap_clean - cap_choppy:+.0%}。"
             f"{'✅ 方向支持条件化假设' if cap_clean > cap_choppy else '❌ 未见干净趋势更优，条件化假设本样本不成立'}"
             f"（注：这是描述性切分，非 E9 v2 正式预登记检验；正式检验判据见 model_change_proposals P12 段）。")

    # C 触发率
    fired = [r for r in all_cycles if r["c_fired"]]
    L.append(f"\n**顶部识别（C）触发率**：{len(fired)}/{n} 个周期真正触发了顶部特征。")
    if fired:
        cap_c = _agg([r["C"]["捕获率"] for r in fired])[1]
        vs_hold = sum(1 for r in fired if r["C"]["持有收益"] > r["hold_ret"])
        L.append(f"- 触发的周期里：C 平均捕获率 {cap_c:.0%}，其中 {vs_hold}/{len(fired)} 个跑赢买入持有。"
                 f"{'（未触发的周期 C 退化为买入持有，不计入 C 的有效证据）'}")
    else:
        L.append("- 本样本 C 从未触发（都是温水顶/无放量 blow-off）→ 顶部识别对本指数池**无有效证据**，"
                 "需含 2015 式暴涨暴跌顶的样本才能检验（维持 P16 候选、不晋升）。")

    # 四、诚实裁决
    L.append("\n## 四、裁决（诚实版）")
    L.append(f"- **控回撤**：系统 B 若在多数周期 give-back 优于重远（本次 {b_better}/{n}），"
             f"则「少还顶部利润」是系统的稳定优势——与第1题一致。")
    L.append(f"- **总收益**：若 B2 多数周期跑输买入持有（本次胜率 {winrate(b2v):.0%}），"
             f"说明紧止损+reentry 的震荡损耗在牛市里系统性拖累总收益，是明确代价而非个例。")
    L.append(f"- **E9 v2 方向**：干净趋势 vs 震荡的捕获率差 {cap_clean - cap_choppy:+.0%} "
             f"给「趋势市中条件化 MA60」提供{'正向' if cap_clean > cap_choppy else '反向'}描述性证据；"
             f"但样本量 n={n} 偏小，正式晋升仍须走 E9 v2 预登记检验（判据已写死、勿按本结果回调）。")
    L.append("- ⚠️ 指数级、宽基口径，非个股；周期检测依赖 MA60 区制启发式；"
             f"n={n} 属中小样本，作**方向性统计参考**，不等同于个股实盘裁决。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="全样本多周期离场对照回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())
    md = run(repo)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
