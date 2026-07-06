"""E8：破MA20清仓 vs 回踩MA20买点 —— MA20 止损是否误洗刚回踩建的新仓？(预登记)

H0（零假设）：现状「收盘破 MA20 即清仓」(risk.evaluate_holding 第3步 ma_trailing，
  对未盈利新仓无差别生效) 相比「给新仓一个缓冲」，**不会**造成可观的过早误洗、
  也不会更差 —— 若 H0 被推翻(缓冲臂在不杀均值/不升大亏的前提下显著降误洗)，则 P10 值得实现。

背景（缺陷定位，见 risk.py）：
  买点 = 「回踩≈MA20，企稳放量则买点」；止损 = 「收盘破 MA20 清仓」。两者锚**同一条 MA20**、
  中间零缓冲。且 risk.py 的 MA5/MA10 梯子(replay_ladder_tier)明确「盈利后才收紧，
  否则洗掉刚启动的票」——唯独 MA20 清仓漏了同款「新仓/未盈利」保护。回踩买入的仓
  一遇假跌破就被清，买卖打架。

方法：逐仓事件驱动模拟(同 E1 口径)。入场=投顾 long。四臂只改 MA20 处理，硬止损 -8%
  四臂始终兜底(真下跌不豁免)：
    A strict  破MA20即清(现状)
    B grace   建仓 N 日内不启用 MA20 清仓(新仓宽限)
    C buffer  收盘破 MA20 超 x% 才清(幅度缓冲)
    D trim    未盈利破MA20只减半一次、盈利后才清(把 MA5/10 梯子的“盈利后收紧”移到 MA20)
  指标：逐仓净收益(扣往返成本) 均值/中位/胜率/尾部(最差5%)/大亏率<-15%/均持有天，
       + MA20 触发离场占比 + **误洗率**(被 MA20 清后、若只留硬止损持有到期反更赚>2% 的占比)。
  过关：某缓冲臂 vs A —— 均值不降(≥A-0.5pp) 且 大亏率不升 且 误洗率显著下降 → P10 值得实现。

功效声明：入场集=投顾 long，历史短(见报告“功效声明”)→当前结论偏弱、随数据累积增强；
  MA20/硬止损全在真实行情上模拟，方法本身可随 universe 回踩事件扩展提功效(v2)。
只读 DB，不改业务逻辑。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from common import (BaseRepository, close_panel, first_on_or_after, md_table, pct,
                    trade_calendar)

WARMUP = 30           # 建仓前预热交易日(暖 MA20)
MAX_HOLD = 40         # 最长持有交易日
ROUND_TRIP_COST = 0.0021
HARD_STOP = 0.08
PP_TRIGGER = 0.15     # 与 RiskConfig 默认一致：浮盈达此才算“盈利”
GRACE_DAYS = 5
BUFFER_PCT = 0.02
WASH_GAP = 0.02       # 误洗判定：hold-to-end 比实际臂多赚 > 2%


def _entries(repo: BaseRepository) -> pd.DataFrame:
    if not repo.table_exists("advisor_reco"):
        return pd.DataFrame()
    df = repo.read_sql("SELECT rec_date, code FROM advisor_reco WHERE direction='long'")
    if df.empty:
        return df
    df["rec_date"] = df["rec_date"].astype(str)
    return df.drop_duplicates(["rec_date", "code"]).reset_index(drop=True)


def _hold_to_end(vals: np.ndarray, ei: int, last_i: int, cost: float) -> float:
    """只硬止损、否则持有到 last_i 的毛收益（MA20 无关）——误洗对照基准。"""
    for i in range(ei + 1, last_i + 1):
        c = vals[i]
        if np.isfinite(c) and c / cost - 1 <= -HARD_STOP:
            return c / cost - 1
    return vals[last_i] / cost - 1


def _sim(closes: pd.Series, entry_date: str, mode: str) -> dict | None:
    """单仓逐日模拟。mode 只改 MA20 处理；strict/grace/buffer/trim。"""
    s = pd.to_numeric(closes, errors="coerce").dropna()
    keys = [str(k) for k in s.index]
    if entry_date not in keys:
        return None
    ei = keys.index(entry_date)
    cost = float(s.iloc[ei])
    if not np.isfinite(cost) or cost <= 0:
        return None
    vals = s.to_numpy(dtype=float)
    ma20 = s.rolling(20).mean().to_numpy()
    last_i = min(ei + MAX_HOLD, len(vals) - 1)
    remaining = 1.0
    realized = 0.0
    peak = cost
    trimmed = False
    exit_i = last_i
    ma20_hit = False
    for i in range(ei + 1, last_i + 1):     # 建仓当日不触发
        c = vals[i]
        if not np.isfinite(c):
            continue
        peak = max(peak, c)
        if c / cost - 1 <= -HARD_STOP:      # 硬止损兜底（四臂共用）
            realized += remaining * c
            remaining = 0.0
            exit_i = i
            break
        m = ma20[i]
        if not np.isfinite(m):
            continue
        below = c < m
        trig = False
        if mode == "strict":
            trig = below
        elif mode == "grace":
            trig = below and (i - ei) > GRACE_DAYS
        elif mode == "buffer":
            trig = c < m * (1 - BUFFER_PCT)
        elif mode == "trim":
            if below and peak / cost - 1 >= PP_TRIGGER:
                trig = True
            elif below and not trimmed:
                sell = remaining * 0.5
                realized += sell * c
                remaining -= sell
                trimmed = True
        if trig:
            realized += remaining * c
            remaining = 0.0
            exit_i = i
            ma20_hit = True
            break
    if remaining > 1e-9:
        realized += remaining * vals[last_i]
    net = realized / cost - 1.0 - ROUND_TRIP_COST
    hte = _hold_to_end(vals, ei, last_i, cost) - ROUND_TRIP_COST
    washed = bool(ma20_hit and (hte - net > WASH_GAP))
    return {"net": net, "hold_days": exit_i - ei, "ma20_hit": ma20_hit, "washed": washed}


def _stats(recs: list[dict], name: str) -> list:
    v = np.asarray([r["net"] for r in recs if r and np.isfinite(r["net"])], dtype=float)
    if len(v) == 0:
        return [name, 0, "NA", "NA", "NA", "NA", "NA", "NA", "NA"]
    hits = [r for r in recs if r["ma20_hit"]]
    washed = sum(1 for r in recs if r["washed"])
    hitrate = f"{len(hits) / len(recs) * 100:.0f}%"
    washrate = f"{washed / len(hits) * 100:.0f}%" if hits else "—"
    return [name, len(v), pct(v.mean()), pct(float(np.median(v))),
            f"{(v > 0).mean() * 100:.0f}%", pct(float(np.percentile(v, 5))),
            f"{(v < -0.15).mean() * 100:.0f}%", hitrate, washrate]


def _wash_rate(recs: list[dict]) -> float:
    hits = [r for r in recs if r["ma20_hit"]]
    if not hits:
        return float("nan")
    return sum(1 for r in recs if r["washed"]) / len(hits)


def run(repo: BaseRepository) -> str:
    L = ["## E8 —— 破MA20清仓 vs 回踩MA20买点（新仓误洗检验）", ""]
    ent = _entries(repo)
    if ent.empty:
        return "\n".join(L + ["**数据不足**：无 advisor_reco long 入场集合，E8 跳过。"])
    cal = trade_calendar(repo)
    if not cal:
        return "\n".join(L + ["**数据不足**：stock_daily 无交易日。"])
    cal_idx = {d: i for i, d in enumerate(cal)}
    need: set = set()
    rows = []
    for _, r in ent.iterrows():
        e = first_on_or_after(cal, r["rec_date"])
        if e is None or e not in cal_idx:
            continue
        i0 = max(0, cal_idx[e] - WARMUP)
        i1 = min(len(cal) - 1, cal_idx[e] + MAX_HOLD)
        win = cal[i0:i1 + 1]
        need.update(win)
        rows.append({"code": r["code"], "entry": e, "win": win})
    panel = close_panel(repo, sorted(ent["code"].unique()), sorted(need))
    L.append(f"- 入场：{len(rows)} 仓（投顾 long），最长持有 {MAX_HOLD} 日，往返成本 "
             f"{ROUND_TRIP_COST * 100:.2f}%；硬止损 -{HARD_STOP:.0%} 四臂共用兜底。")
    arms = {"A 破MA20即清(现状)": "strict", "B 建仓5日宽限": "grace",
            "C 破MA20超2%才清": "buffer", "D 未盈利破MA20只减半": "trim"}
    res: dict = {k: [] for k in arms}
    for row in rows:
        ser = pd.Series({d: panel.get(row["code"], {}).get(d) for d in row["win"]}).dropna()
        if row["entry"] not in ser.index:
            continue
        for k, mode in arms.items():
            rec = _sim(ser, row["entry"], mode)
            if rec:
                res[k].append(rec)
    table = [_stats(res[k], k) for k in arms]
    L += ["", md_table(["臂", "仓数", "均值", "中位", "胜率", "尾部(最差5%)",
                         "大亏率<-15%", "MA20触发占比", "误洗率"], table), ""]
    A = np.asarray([r["net"] for r in res["A 破MA20即清(现状)"] if np.isfinite(r["net"])])
    wa = _wash_rate(res["A 破MA20即清(现状)"])
    if len(A) > 5:
        L.append("### 结论")
        best = None
        for k in ["B 建仓5日宽限", "C 破MA20超2%才清", "D 未盈利破MA20只减半"]:
            v = np.asarray([r["net"] for r in res[k] if np.isfinite(r["net"])])
            if len(v) < 5:
                continue
            d_mean = v.mean() - A.mean()
            d_big = (v < -0.15).mean() - (A < -0.15).mean()
            wk = _wash_rate(res[k])
            d_wash = (wk - wa) if (np.isfinite(wk) and np.isfinite(wa)) else float("nan")
            ok = (d_mean >= -0.005 and d_big <= 0.0 and np.isfinite(d_wash) and d_wash < -0.05)
            L.append(f"- {k} vs A：均值 {pct(d_mean)}、大亏率 {d_big * 100:+.0f}pp、"
                     f"误洗率 {pct(d_wash) if np.isfinite(d_wash) else 'NA'} → "
                     f"{'✅ 过关候选' if ok else '未过关'}")
            if ok:
                best = k
        L.append(f"- **裁决**：{'存在缓冲臂在不杀均值/不升大亏下显著降误洗 → P10 值得实现（走影子→A/B）。' if best else '当前样本未见缓冲臂显著优于现状（或样本不足）→ hold，随投顾/回踩样本累积每周重跑。'}")
    else:
        L.append("### 结论\n- 样本不足以判定（投顾入场仓 < 6）；随数据累积每周重跑。")
    L.append("- 误洗率＝被 MA20 清仓后、若只留硬止损持有到期反多赚 >2% 的占比（直接量化"
             "“回踩买入被假跌破洗掉”的代价）。功效随投顾历史/回踩事件样本累积增强。")
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
