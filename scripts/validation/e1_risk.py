"""E1：持仓级、事件驱动的止盈止损，在「投顾持有制组合」上是否降尾部而不杀均值？

预登记：
  H0: 持仓级止盈止损（硬止损 + 均线移动 + 盈利保护，以建仓价为基准、与调仓日历解耦）
      **不能**在降大亏/尾部的同时保住平均收益。
  背景: results/risk_overlay_comparison.md 已证「日频止损 × 月度调仓」结构性毁收益(-26%)；
        本实验换成**持有制逐仓**口径（投顾票买入后一直持有直到止盈止损触发），这才是模型主职场景。
  指标: 每仓 **净收益**(扣往返成本)分布 —— 均值/中位/胜率/尾部(最差5%)/大亏率(<-15%)/平均持有天数，
        三臂对照：A 不设止损(固定持有) / B 趋势过滤入场 / C 持仓级止损。
  过关: C 相对 A **大亏率与尾部显著下降、均值不显著恶化** → P1 值得实现；否则据实缩减。

复用 invest_model.portfolio.risk 纯函数（step_tier / pp_step / keep_from_step / trend_ok_close）。只读。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common import (BaseRepository, close_panel, first_on_or_after, md_table, pct,
                    trade_calendar)
from invest_model.portfolio.risk import (RiskConfig, keep_from_step, pp_step, step_tier,
                                         trend_ok_close)

WARMUP = 70          # 建仓前预热交易日（暖 MA5/10/20 + MA60 趋势过滤）
MAX_HOLD = 40        # 最长持有交易日
ROUND_TRIP_COST = 0.0021  # 往返成本≈ 买 8bp + 卖(费+印花+滑点)~13-18bp


def _entries(repo: BaseRepository) -> pd.DataFrame:
    """入场集合：优先投顾 long 推荐（持有制主场景）。"""
    if not repo.table_exists("advisor_reco"):
        return pd.DataFrame()
    df = repo.read_sql("SELECT rec_date, code FROM advisor_reco WHERE direction='long'")
    if df.empty:
        return df
    df["rec_date"] = df["rec_date"].astype(str)
    return df.drop_duplicates(["rec_date", "code"]).reset_index(drop=True)


def _simulate_one(closes: pd.Series, entry_date: str, cfg: RiskConfig,
                  use_stop: bool) -> dict | None:
    """单仓逐日模拟。closes：含预热的收盘序列(index=trade_date 升序)。

    use_stop=False：固定持有 MAX_HOLD 日后按收盘离场（不设止损）。
    use_stop=True：每日按硬止损 + 均线移动止盈 + 盈利保护逐档减/清仓。
    返回 {net_ret, hold_days} 或 None。
    """
    s = pd.to_numeric(closes, errors="coerce").dropna()
    keys = [str(k) for k in s.index]
    if entry_date not in keys:
        return None
    ei = keys.index(entry_date)
    cost = float(s.iloc[ei])
    if not np.isfinite(cost) or cost <= 0:
        return None
    vals = s.to_numpy(dtype=float)
    ma5 = s.rolling(5).mean().to_numpy()
    ma10 = s.rolling(10).mean().to_numpy()
    ma20 = s.rolling(20).mean().to_numpy()

    remaining = 1.0
    realized = 0.0        # Σ 卖出比例 × 卖出价
    tier = 0              # 移动止盈档
    pp = 0                # 盈利保护档
    peak = cost
    last_i = min(ei + MAX_HOLD, len(vals) - 1)
    hold_days = last_i - ei
    for i in range(ei, last_i + 1):
        c = vals[i]
        if not np.isfinite(c):
            continue
        peak = max(peak, c)
        if use_stop and cfg.enabled and i > ei:  # 建仓当日不触发
            # 1) 硬止损
            if cfg.hard_stop_pct and c / cost - 1 <= -cfg.hard_stop_pct:
                realized += remaining * c; remaining = 0.0; hold_days = i - ei; break
            # 2) 盈利保护（自峰值回撤）
            if cfg.profit_protect:
                npp = pp_step(c, peak, cost, cfg, pp)
                if npp > pp:
                    if npp >= 2:
                        realized += remaining * c; remaining = 0.0; hold_days = i - ei; pp = npp; break
                    sell = remaining * (1 - cfg.pp_trim_keep)
                    realized += sell * c; remaining -= sell; pp = npp
            # 3) 均线移动止盈
            if cfg.ma_trailing:
                nt = step_tier(c, ma5[i], ma10[i], ma20[i], tier, full=cfg.trail_full)
                if nt > tier:
                    keep = keep_from_step(tier, nt)
                    sell = remaining * (1 - keep)
                    realized += sell * c; remaining -= sell; tier = nt
                    if remaining <= 1e-9:
                        hold_days = i - ei; break
    if remaining > 1e-9:  # 到期/触底仍持有 → 末日收盘清
        realized += remaining * vals[last_i]
    gross = realized / cost - 1.0
    return {"net_ret": gross - ROUND_TRIP_COST, "hold_days": hold_days}


def _arm_stats(rets: list[float], holds: list[float], name: str) -> list:
    v = np.asarray([x for x in rets if x is not None and np.isfinite(x)], dtype=float)
    if len(v) == 0:
        return [name, 0, "NA", "NA", "NA", "NA", "NA", "NA"]
    tail = float(np.percentile(v, 5))
    bigloss = float((v < -0.15).mean())
    return [name, len(v), pct(v.mean()), pct(float(np.median(v))),
            f"{(v>0).mean()*100:.0f}%", pct(tail), f"{bigloss*100:.0f}%",
            f"{np.mean(holds):.0f}"]


def run(repo: BaseRepository) -> str:
    L = ["## E1 —— 持仓级止盈止损（投顾持有制组合）", ""]
    ent = _entries(repo)
    if ent.empty:
        return "\n".join(L + ["**数据不足**：无 advisor_reco 入场集合，E1 跳过。"])
    cal = trade_calendar(repo)
    if not cal:
        return "\n".join(L + ["**数据不足**：stock_daily 无交易日。"])
    cal_idx = {d: i for i, d in enumerate(cal)}
    codes = sorted(ent["code"].unique())

    # 每仓需要 [entry-WARMUP, entry+MAX_HOLD] 的收盘；合并各仓所需日期
    need = set()
    ent_rows = []
    for _, r in ent.iterrows():
        e = first_on_or_after(cal, r["rec_date"])
        if e is None or e not in cal_idx:
            continue
        i0 = max(0, cal_idx[e] - WARMUP); i1 = min(len(cal) - 1, cal_idx[e] + MAX_HOLD)
        win = cal[i0:i1 + 1]
        need.update(win)
        ent_rows.append({"code": r["code"], "entry": e, "win": win})
    panel = close_panel(repo, codes, sorted(need))
    L.append(f"- 入场：{len(ent_rows)} 仓（投顾 long），最长持有 {MAX_HOLD} 交易日，往返成本 {ROUND_TRIP_COST*100:.2f}%。")

    cfg = RiskConfig(enabled=True, hard_stop_pct=0.08, ma_trailing=True, trail_full=True,
                     profit_protect=True, pp_trigger=0.15, pp_trim_dd=0.08, pp_exit_dd=0.12)

    arms = {"A 不设止损(持有到期)": [], "B 趋势过滤入场": [], "C 持仓级止损": []}
    holds = {k: [] for k in arms}
    for row in ent_rows:
        ser = pd.Series({d: panel.get(row["code"], {}).get(d) for d in row["win"]})
        ser = ser.dropna()
        if row["entry"] not in ser.index:
            continue
        # A
        a = _simulate_one(ser, row["entry"], cfg, use_stop=False)
        if a: arms["A 不设止损(持有到期)"].append(a["net_ret"]); holds["A 不设止损(持有到期)"].append(a["hold_days"])
        # B：仅当建仓日趋势过滤通过才入场（否则该仓不参与）
        pre = ser[[k for k in ser.index if str(k) <= row["entry"]]]
        if trend_ok_close(pre, cfg) and a:
            arms["B 趋势过滤入场"].append(a["net_ret"]); holds["B 趋势过滤入场"].append(a["hold_days"])
        # C
        c = _simulate_one(ser, row["entry"], cfg, use_stop=True)
        if c: arms["C 持仓级止损"].append(c["net_ret"]); holds["C 持仓级止损"].append(c["hold_days"])

    rows = [_arm_stats(arms[k], holds[k], k) for k in arms]
    L += ["", md_table(["臂", "仓数", "均值", "中位", "胜率", "尾部(最差5%)", "大亏率<-15%", "均持有天"], rows), ""]

    A = np.asarray([x for x in arms["A 不设止损(持有到期)"] if np.isfinite(x)])
    C = np.asarray([x for x in arms["C 持仓级止损"] if np.isfinite(x)])
    if len(A) > 5 and len(C) > 5:
        d_big = (C < -0.15).mean() - (A < -0.15).mean()
        d_tail = np.percentile(C, 5) - np.percentile(A, 5)
        d_mean = C.mean() - A.mean()
        passed = (d_big < -0.02 and d_tail > 0.0 and d_mean > -0.01)
        L.append(f"### 结论")
        L.append(f"- C vs A：大亏率 {d_big*100:+.0f}pp、尾部 {pct(d_tail)}、均值 {pct(d_mean)}。")
        L.append(f"- {'**过关**：持仓级止损降尾部/大亏而不显著杀均值 → P1 值得实现。' if passed else '**未过关/存疑**：止损未能在保住均值的前提下降尾部 → 缩减 P1，量化书维持趋势过滤、止损仅留人工实盘计划。'}")
    else:
        L.append("### 结论\n- 样本不足以判定（需更多投顾入场仓 + 足够行情窗口）。")
    L.append("- 备注：这是逐仓持有制口径，刻意区别于已被证伪的「日频止损×月度调仓」。")
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
