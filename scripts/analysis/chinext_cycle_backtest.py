"""创业板指本轮牛市（MA60 突破→跌破）离场策略对照回测。

用户命题：以创业板指（399006.SZ）本轮牛市为样本（重远口径：首次突破 60 日均线建仓、
有效跌破 60 日均线宣告趋势结束），**同一入场点**下对比三种离场，看谁更接近顶、少回吐：

  A｜重远基线：突破 MA60 建仓 → 有效跌破 MA60 清仓（单一均线）
  B｜系统盈利模型：直接调用生产 risk.py（evaluate_holding + profit_protect + armed_ladder，
     RiskConfig 默认口径），逐日模拟，任一触发全清即离场——这是实盘真实用的那套逻辑，不重写
  C｜顶部识别探测器（P16 候选，判据预登记写死、勿事后调）：浮盈达标后，
     20 日已实现波动升到近 250 日 ≥80 分位（波动骤放大）且 5/60 日量比 ≥1.5（放量）→ 顶部特征离场

口径：指数当"单一持仓"处理，cost=入场收盘价；报告 入场/离场 日期价、持有收益、峰值、
自峰值回撤（give-back）、持有期最大回撤、持有天数、**捕获率=持有收益/峰值收益**（留住了顶部多少）。

只读、不落库、不改生产。数据来自 index_daily（399006.SZ），需在有 DB 的环境（Actions/FC）跑。
  python scripts/analysis/chinext_cycle_backtest.py [--db ...] [--code 399006.SZ] [--out results/chinext.md]
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
from invest_model.portfolio.risk import (  # noqa: E402
    RiskConfig, armed_ladder, evaluate_holding, profit_protect,
    replay_hold_tier, replay_ladder_tier, replay_pp_tier,
)

# 顶部识别（C）预登记参数——跑数前写死，勿按结果回调
TOP_VOL_PCTL = 0.80          # 20 日已实现波动的近 250 日分位阈值
TOP_VOL_WIN = 20
TOP_VOL_LOOKBACK = 250
TOP_VOLUME_RATIO = 1.5       # 5 日均量 / 60 日均量
TOP_MIN_PROFIT = 0.15        # 仅在浮盈曾达此值后才允许顶部离场（避开建仓初期噪声）
BREAK_BUFFER = 0.0           # 重远"有效跌破"缓冲（0=收盘破线即算；另附 1% 口径对照）


def _load(repo: BaseRepository, code: str) -> pd.DataFrame:
    df = repo.read_sql(
        "SELECT trade_date, close, volume FROM index_daily WHERE code=:c ORDER BY trade_date",
        {"c": code})
    if df.empty:
        return df
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
    return df.dropna(subset=["close"]).reset_index(drop=True)


def _detect_cycle(df: pd.DataFrame, base_gap: int = 10) -> tuple[int, int]:
    """检测本轮牛市：终点=最后一次有效跌破 MA60；入场=该轮"站上 MA60 区制"的起点。

    修正要点：不能只找"最近一次突破"——顶部破位前常有 1~2 日假突破会被误当起点。
    改用**区制回溯**：从终点前一日往回走，容忍牛市途中 ≤base_gap 日的短暂回落（正常回踩），
    一旦遇到连续 >base_gap 日在 MA60 下方的"底部基座"，就认定牛市在基座之后启动。
    返回 (s=入场, e=终点) 行下标。数据驱动、不手挑日期。
    """
    c = df["close"].to_numpy(dtype=float)
    ma60 = df["close"].rolling(60).mean().to_numpy()
    n = len(c)
    below = (c < ma60 * (1 - BREAK_BUFFER)) | ~np.isfinite(ma60)
    # 终点：最后一个"新鲜跌破"（昨在线上、今破位）
    e = None
    for i in range(n - 1, 60, -1):
        if np.isfinite(ma60[i]) and below[i] and not below[i - 1]:
            e = i
            break
    if e is None:
        e = n - 1
    # 入场：从 e-1 往回走，容忍 ≤base_gap 日回落；遇到 >base_gap 连续在线下的基座即停
    s = e - 1
    gap = 0
    i = e - 1
    while i > 61:
        if below[i]:
            gap += 1
            if gap > base_gap:         # 命中底部基座 → 牛市在其之后
                break
        else:
            gap = 0
            s = i                      # 记录最近的"在线上"日
        i -= 1
    # s 回推到该区制第一个站上 MA60 的突破日
    while s > 61 and not below[s - 1]:
        s -= 1
    return s, e


def _stats(closes: pd.Series, s: int, exit_i: int, label: str, reason: str) -> dict:
    seg = closes.iloc[s:exit_i + 1].to_numpy(dtype=float)
    entry_px, exit_px = seg[0], seg[-1]
    peak_i = int(np.argmax(seg))
    peak_px = seg[peak_i]
    ret = exit_px / entry_px - 1
    peak_ret = peak_px / entry_px - 1
    giveback = exit_px / peak_px - 1                       # 自峰值回撤（≤0）
    dd = float((seg / np.maximum.accumulate(seg) - 1).min())  # 持有期最大回撤
    capture = ret / peak_ret if peak_ret > 0 else float("nan")
    return {"策略": label, "离场原因": reason,
            "入场": entry_px, "离场": exit_px, "持有收益": ret,
            "峰值收益": peak_ret, "自峰值回撤": giveback,
            "持有期MaxDD": dd, "捕获率": capture, "持有天数": exit_i - s}


def _exit_reain_ma60(df, ma60, s: int, e: int, x: float) -> tuple[int, str]:
    """A｜重远：入场后首次有效跌破 MA60（收盘<MA60×(1-x)）。"""
    c = df["close"].to_numpy(dtype=float)
    for i in range(s + 1, len(c)):
        if np.isfinite(ma60[i]) and c[i] < ma60[i] * (1 - x):
            return i, f"有效跌破MA60(x={x:.0%})"
    return e, "至样本末未破位"


def _exit_system(df, s: int, cfg: RiskConfig) -> tuple[int, str]:
    """B｜系统盈利模型：逐日调用生产 risk.py，任一全清触发即离场。"""
    dates = df["trade_date"].astype(str).tolist()
    closes = df["close"]
    entry_date = dates[s]
    cost = float(closes.iloc[s])
    warm_from = max(0, s - 150)                            # 均线预热窗
    for i in range(s + 1, len(df)):
        hist = closes.iloc[warm_from:i + 1]
        hist.index = df["trade_date"].iloc[warm_from:i + 1].astype(str).values
        hold = closes.iloc[s:i + 1]
        hold.index = df["trade_date"].iloc[s:i + 1].astype(str).values
        prev = replay_hold_tier(hist.iloc[:-1], cost, cfg, replay_from=entry_date)
        dec = evaluate_holding(hist, cost, cfg, prev_tier=prev)
        if dec.action == "exit":
            return i, f"risk.py:{dec.reason}"
        pp_prev = replay_pp_tier(hold.iloc[:-1], cost, cfg)
        ppd = profit_protect(hold, cost, cfg, prev_tier=pp_prev)
        if ppd is not None and ppd.action == "exit":
            return i, f"risk.py:{ppd.reason}"
        lad = armed_ladder(hist, entry_date, cost, cfg)
        if lad is not None and lad.action == "exit":
            return i, f"risk.py:{lad.reason}"
    return len(df) - 1, "至样本末未触发全清"


def _system_with_reentry(df, s: int, e: int, cfg: RiskConfig) -> dict:
    """B2｜系统盈利模型 + 再入场：实盘止盈离场后 45 日内创新高则接回，逐段复利到周期终点。

    公平对比重远"持有到破 MA60"——B 单次离场早，但实盘会 reentry 多次接回。
    段内收益 = 离场价/入场价；段间空仓不计收益；最后一段到样本末或周期终点 e。
    返回 {总收益, 段数, 在场天数, 各段离场原因}。
    """
    closes = df["close"]
    n = len(df)
    total = 1.0
    segs = 0
    days_in = 0
    reasons = []
    cur = s
    while cur < e:
        exit_i, reason = _exit_system(df.iloc[:e + 1], cur, cfg)
        exit_i = min(exit_i, e)
        seg_ret = float(closes.iloc[exit_i]) / float(closes.iloc[cur])
        total *= seg_ret
        segs += 1
        days_in += exit_i - cur
        reasons.append(f"{df['trade_date'].iloc[exit_i]}:{reason}({seg_ret - 1:+.0%})")
        # 再入场：45 日内收盘创出「上一段峰值」新高（复用生产 reentry 语义）
        seg_peak = float(closes.iloc[cur:exit_i + 1].max())
        re = None
        for j in range(exit_i + 1, min(exit_i + 46, e + 1)):
            if float(closes.iloc[j]) > seg_peak:
                re = j
                break
        if re is None:
            break
        cur = re
    return {"总收益": total - 1, "段数": segs, "在场天数": days_in, "明细": reasons}


def _exit_top(df, s: int) -> tuple[int, str]:
    """C｜顶部识别探测器（预登记）：浮盈达标后 波动骤放大 且 放量 → 离场。"""
    c = df["close"].astype(float)
    v = df["volume"].astype(float)
    ret = c.pct_change()
    vol20 = ret.rolling(TOP_VOL_WIN).std() * np.sqrt(250)
    vol_rank = vol20.rolling(TOP_VOL_LOOKBACK, min_periods=60).apply(
        lambda w: (w.iloc[-1] >= w).mean(), raw=False)
    vratio = v.rolling(5).mean() / v.rolling(60).mean()
    cost = float(c.iloc[s])
    peak = cost
    has_vol = v.iloc[s:].notna().sum() > 20
    for i in range(s + 1, len(df)):
        peak = max(peak, float(c.iloc[i]))
        if peak / cost - 1 < TOP_MIN_PROFIT:
            continue
        vol_hot = np.isfinite(vol_rank.iloc[i]) and vol_rank.iloc[i] >= TOP_VOL_PCTL
        vol_up = (not has_vol) or (np.isfinite(vratio.iloc[i]) and vratio.iloc[i] >= TOP_VOLUME_RATIO)
        if vol_hot and vol_up:
            tag = "波动骤升+放量" if has_vol else "波动骤升(无量数据)"
            return i, f"顶部特征:{tag}"
    return len(df) - 1, "至样本末未现顶部特征"


def run(repo: BaseRepository, code: str) -> str:
    df = _load(repo, code)
    L = [f"# 创业板指本轮牛市离场策略对照回测（{code}）", ""]
    if len(df) < 120:
        L.append(f"⚠️ {code} index_daily 样本不足（{len(df)} 行），先 bump ops/index-backfill.trigger 回填。")
        return "\n".join(L)
    ma60 = df["close"].rolling(60).mean().to_numpy()
    s, e = _detect_cycle(df)
    d0, d1 = df["trade_date"].iloc[s], df["trade_date"].iloc[e]
    L.append(f"- 样本：{df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}（{len(df)} 交易日）；"
             f"量数据 {'有' if df['volume'].notna().sum() > 100 else '缺（C 退化为仅波动）'}")
    L.append(f"- **本轮周期（数据驱动检测）**：入场 {d0}（首次突破 MA60、MA60 上行）→ "
             f"终点 {d1}（有效跌破 MA60），共 {e - s} 交易日")
    L.append(f"- 入场价 {df['close'].iloc[s]:.2f} → 终点价 {df['close'].iloc[e]:.2f}；"
             f"区间峰值 {df['close'].iloc[s:e+1].max():.2f}")
    L.append(f"- 口径：同一入场点，只比离场；B 直接调用生产 risk.py（RiskConfig 默认）；"
             f"C 判据预登记（波动≥{TOP_VOL_PCTL:.0%}分位 且 5/60量比≥{TOP_VOLUME_RATIO}，浮盈≥{TOP_MIN_PROFIT:.0%}后）")

    cfg = RiskConfig()
    rows = []
    ea, ra = _exit_reain_ma60(df, ma60, s, e, 0.0)
    rows.append(_stats(df["close"], s, ea, "A 重远·破MA60", ra))
    ea1, ra1 = _exit_reain_ma60(df, ma60, s, e, 0.01)
    rows.append(_stats(df["close"], s, ea1, "A' 重远·破MA60(x=1%)", ra1))
    eb, rb = _exit_system(df, s, cfg)
    rows.append(_stats(df["close"], s, eb, "B 系统盈利模型", rb))
    ec, rc = _exit_top(df, s)
    rows.append(_stats(df["close"], s, ec, "C 顶部识别", rc))

    L.append("\n| 策略 | 离场日 | 离场原因 | 持有收益 | 峰值收益 | 自峰值回撤 | 持有MaxDD | 捕获率 | 天数 |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    idx_by_label = {"A 重远·破MA60": ea, "A' 重远·破MA60(x=1%)": ea1, "B 系统盈利模型": eb, "C 顶部识别": ec}
    for r in rows:
        exit_d = df["trade_date"].iloc[idx_by_label[r["策略"]]]
        L.append(f"| {r['策略']} | {exit_d} | {r['离场原因']} | {r['持有收益']:+.1%} | "
                 f"{r['峰值收益']:+.1%} | {r['自峰值回撤']:+.1%} | {r['持有期MaxDD']:.1%} | "
                 f"{r['捕获率']:.0%} | {r['持有天数']} |")

    # B2：系统 + 再入场（公平对比重远的"持有到破位"）
    b2 = _system_with_reentry(df, s, e, cfg)
    full_peak = df["close"].iloc[s:e + 1].max() / df["close"].iloc[s] - 1
    L.append(f"\n**B2｜系统盈利模型 + 再入场**（reentry：止盈离场后 45 日创新高接回，逐段复利）：")
    L.append(f"- 全周期总收益 **{b2['总收益']:+.1%}**（{b2['段数']} 段进出、在场 {b2['在场天数']} 天 / "
             f"周期 {e - s} 天）；同期"
             f"买入持有到终点 {df['close'].iloc[e] / df['close'].iloc[s] - 1:+.1%}、峰值 {full_peak:+.1%}")
    L.append(f"- 段内明细：{' → '.join(b2['明细'][:6])}{' …' if len(b2['明细']) > 6 else ''}")

    L.append("\n### 读法")
    L.append("- **B vs B2**：B 是单次首离场（早、但只是第一段）；B2 含实盘 reentry 接回，"
             "才是系统真实的全周期捕获——单看 B 会低估系统。")
    L.append("- **捕获率** = 持有收益 / 峰值收益：越高说明离场越接近顶、留住的利润越多。")
    L.append("- **自峰值回撤** = 离场价相对区间峰值：越接近 0 说明越少把顶部利润还回去（重远第1题的核心）。")
    L.append("- B 直接是实盘 risk.py 逻辑；A 是重远单一 MA60 基线；C 是顶部探测器候选（判据预登记）。")
    L.append("- ⚠️ 单指数单周期 n=1，只作个案演示与直觉校准，不构成统计裁决（统计显著性见 E9/E12）。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="创业板指本轮牛市离场策略对照回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--code", default="399006.SZ")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())
    md = run(repo, args.code)
    print(md)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
