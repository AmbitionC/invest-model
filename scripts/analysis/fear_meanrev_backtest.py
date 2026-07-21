"""恐慌极值均值回归择时回测（用户命题·一次性分析·只读不落库）。

用户命题：恐慌值达极值后买入创业板指(399006.SZ)/科创50(000688.SH)，恐慌值回落后卖出，
这种短线择时怎么样？并回答：要做这功能，恐慌值监测间隔多久合适。

## 判据预登记（跑数前写死，勿按结果回调）

信号：市场级恐慌值 fear（0–100，越高越恐慌；benchmark=000300.SH，与生产同口径）。
  纯 EOD 无未来函数——fear(t) 于 t 日收盘后才知，故一律**次日收盘执行**（close[t+1]），杜绝前视。
  fear 序列优先读 fear_daily（生产官方值）；不足则用 fear_gauge 逐日重算（同一函数、无前视）。

标的：创业板指 399006.SZ、科创50 000688.SH（指数收盘作收益代理；实盘须用 159915/588000 ETF，
  另计跟踪误差+费率，见报告注意项）。

择时（单周期单仓·一 episode 一进出）：
  入场：fear 首次上穿 ≥ THR_BUY（前一日 <THR_BUY），次日收盘买入。
  出场：fear 回落 ≤ THR_SELL，次日收盘卖出。样本末未平仓者按最后收盘标记（open）。
  网格：THR_BUY ∈ {70,75,80} × THR_SELL ∈ {55,50,45}。
  对照出场：固定持有 N∈{5,10,20} 交易日（择时 vs 死扛的锚）。

判定这套择时是否**有边际**（预登记，四条）：
  ① 极值入场的前瞻收益 > 无条件前瞻收益（5/10/20/40 日至少两个窗口正超额，且样本≥5）；
  ② 网格主格(75/50)交易胜率 ≥ 55% 且 平均单笔 > 0；
  ③ 择时复利 ≥ 同期买入持有（在场时间显著更短算加分：≥买入持有的 80% 收益但在场<50% 天数即算过）；
  ④ 多标的一致（两个指数方向一致，不是单指数偶然）。
  四条过 ≥3＝方向成立可登记提案+预登记 E 验证；否则记入研究不新增提案。

监测间隔：报告 fear 的日间变动幅度/自相关/极值 episode 持续天数，量化「日频够不够、
  盘中哨兵有无必要」，据此给间隔建议（非拍脑袋）。

只读 index_daily / fear_daily / stock_daily，不落库、不改生产。需在有 DB 的环境（Actions/FC）跑。
  python scripts/analysis/fear_meanrev_backtest.py [--recompute-days 900] [--out results/fear_meanrev.md]
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
from invest_model.signals.fear import fear_gauge  # noqa: E402

# ── 预登记网格与判据（写死）──
THR_BUYS = [70, 75, 80]
THR_SELLS = [55, 50, 45]
FIXED_HOLDS = [5, 10, 20]
FWD_WINS = [5, 10, 20, 40]
MAIN_BUY, MAIN_SELL = 75, 50       # 主格
CODES = {"399006.SZ": "创业板指", "000688.SH": "科创50"}
BENCH = "000300.SH"


def _fear_series(repo: BaseRepository, recompute_days: int) -> tuple[pd.Series, str]:
    """返回 (fear 按 trade_date 的 Series, 数据来源说明)。

    优先 fear_daily（官方存证）。若覆盖不足 recompute_days，则用 fear_gauge 逐日重算补齐
    （纯 EOD、与生产同函数、无前视）：预载 stock_daily/index_daily 一次，逐日切片喂函数。
    """
    fd = repo.read_sql("SELECT trade_date, score FROM fear_daily ORDER BY trade_date")
    stored = pd.Series(dtype=float)
    if not fd.empty:
        stored = pd.Series(pd.to_numeric(fd["score"], errors="coerce").values,
                           index=fd["trade_date"].astype(str).values).dropna()

    # 交易日历（用 benchmark index_daily 的日期，A 股统一日历）
    cal = repo.read_sql(
        "SELECT DISTINCT trade_date FROM index_daily WHERE code=:c ORDER BY trade_date",
        {"c": BENCH})["trade_date"].astype(str).tolist()
    if not cal:
        return stored, f"fear_daily {len(stored)} 日（无 index_daily 日历、未重算）"

    target = cal[-recompute_days:] if recompute_days > 0 else cal
    missing = [d for d in target if d not in stored.index]
    if not missing:
        cov = [d for d in target if d in stored.index]
        return stored.reindex(cov).dropna(), f"fear_daily 全覆盖 {len(cov)} 日（{cov[0]}~{cov[-1]}）"

    # 需重算：预载窗口 = [最早目标日 - 200 自然日缓冲, 末日]
    span_start = (pd.to_datetime(min(missing)) - pd.Timedelta(days=430)).strftime("%Y%m%d")
    span_end = max(target)
    sdf = repo.read_sql(
        "SELECT code, trade_date, close, pct_chg FROM stock_daily "
        "WHERE trade_date>=:s AND trade_date<=:d", {"s": span_start, "d": span_end})
    idf = repo.read_sql(
        "SELECT code, trade_date, close FROM index_daily "
        "WHERE code=:c AND trade_date>=:s AND trade_date<=:d",
        {"c": BENCH, "s": span_start, "d": span_end})
    if sdf.empty or idf.empty:
        return stored.reindex([d for d in target if d in stored.index]).dropna(), \
            f"fear_daily {len(stored)} 日（stock_daily/index_daily 不足以重算）"
    sdf["trade_date"] = sdf["trade_date"].astype(str)
    idf["trade_date"] = idf["trade_date"].astype(str)

    recomputed = {}
    for d in target:
        if d in stored.index:
            recomputed[d] = float(stored[d])
            continue
        try:
            g = fear_gauge(repo.engine, dt=d, benchmark=BENCH, stock_df=sdf, idx_df=idf)
            recomputed[d] = float(g["score"])
        except Exception:  # noqa: BLE001  某日数据不全 → 跳过
            continue
    ser = pd.Series(recomputed).sort_index()
    n_re = sum(1 for d in target if d not in stored.index and d in recomputed)
    src = (f"fear 序列 {len(ser)} 日（{ser.index[0]}~{ser.index[-1]}）："
           f"fear_daily 命中 {len(ser) - n_re} 日 + fear_gauge 重算 {n_re} 日")
    return ser, src


def _index_closes(repo: BaseRepository, code: str) -> pd.Series:
    df = repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code=:c ORDER BY trade_date",
        {"c": code})
    if df.empty:
        return pd.Series(dtype=float)
    return pd.Series(pd.to_numeric(df["close"], errors="coerce").values,
                     index=df["trade_date"].astype(str).values).dropna()


def _episode_trades(fear: pd.Series, px: pd.Series, thr_buy: float, thr_sell: float):
    """一 episode 一进出：fear 上穿≥buy→次日收盘买；fear 回落≤sell→次日收盘卖。

    在共同交易日上运行；执行价用「信号次日」收盘（无前视）。返回 trades 列表。
    """
    dates = [d for d in px.index if d in fear.index]      # 两者都有的交易日，已排序
    dates.sort()
    f = fear.reindex(dates).to_numpy(dtype=float)
    p = px.reindex(dates).to_numpy(dtype=float)
    n = len(dates)
    trades = []
    i = 1
    in_pos = False
    entry_i = -1
    while i < n:
        if not in_pos:
            crossed = f[i] >= thr_buy and f[i - 1] < thr_buy
            if crossed and i + 1 < n:                     # 次日收盘买
                entry_i = i + 1
                in_pos = True
                i = entry_i                               # 从买入日之后继续找卖点
                continue
        else:
            if f[i] <= thr_sell:
                exit_i = i + 1 if i + 1 < n else i        # 次日收盘卖（末日则当日）
                trades.append({
                    "entry_date": dates[entry_i], "exit_date": dates[exit_i],
                    "entry_px": p[entry_i], "exit_px": p[exit_i],
                    "ret": p[exit_i] / p[entry_i] - 1,
                    "days": exit_i - entry_i,
                    "seg_dd": float((p[entry_i:exit_i + 1] /
                                     np.maximum.accumulate(p[entry_i:exit_i + 1]) - 1).min()),
                    "open": False,
                })
                in_pos = False
        i += 1
    if in_pos and entry_i >= 0:                            # 样本末未平仓
        trades.append({
            "entry_date": dates[entry_i], "exit_date": dates[-1],
            "entry_px": p[entry_i], "exit_px": p[-1],
            "ret": p[-1] / p[entry_i] - 1, "days": (n - 1) - entry_i,
            "seg_dd": float((p[entry_i:] / np.maximum.accumulate(p[entry_i:]) - 1).min()),
            "open": True,
        })
    return trades, dates


def _agg(trades: list[dict], dates: list[str], px: pd.Series) -> dict:
    if not trades:
        return {"n": 0}
    rets = np.array([t["ret"] for t in trades])
    days = np.array([t["days"] for t in trades])
    comp = float(np.prod(1 + rets) - 1)                   # episode 不重叠→顺序复利
    in_days = int(days.sum())
    span_days = len(dates)
    # 同期买入持有（策略活跃区间：首入场日→末出场日）
    d0, d1 = trades[0]["entry_date"], trades[-1]["exit_date"]
    bh = float(px[d1] / px[d0] - 1)
    ann = float((1 + comp) ** (250.0 / max(in_days, 1)) - 1) if comp > -1 else float("nan")
    return {
        "n": len(trades), "win": float((rets > 0).mean()),
        "mean": float(rets.mean()), "median": float(np.median(rets)),
        "comp": comp, "ann_inmkt": ann, "avg_days": float(days.mean()),
        "in_days": in_days, "span_days": span_days,
        "in_frac": in_days / max(span_days, 1), "bh_active": bh,
        "worst": float(rets.min()), "best": float(rets.max()),
        "n_open": sum(1 for t in trades if t["open"]),
    }


def _fixed_hold(fear: pd.Series, px: pd.Series, thr_buy: float, hold: int) -> dict:
    """对照：极值入场后固定持有 hold 日。所有 fresh-cross 独立计（可重叠），取均值。"""
    dates = [d for d in px.index if d in fear.index]
    dates.sort()
    f = fear.reindex(dates).to_numpy(dtype=float)
    p = px.reindex(dates).to_numpy(dtype=float)
    n = len(dates)
    rets = []
    for i in range(1, n):
        if f[i] >= thr_buy and f[i - 1] < thr_buy and i + 1 + hold < n:
            e = i + 1
            rets.append(p[e + hold] / p[e] - 1)
    if not rets:
        return {"n": 0}
    r = np.array(rets)
    return {"n": len(r), "win": float((r > 0).mean()), "mean": float(r.mean()),
            "median": float(np.median(r))}


def _forward_edge(fear: pd.Series, px: pd.Series, thr: float) -> dict:
    """极值入场前瞻收益 vs 无条件前瞻收益（隔离均值回归 alpha）。"""
    dates = [d for d in px.index if d in fear.index]
    dates.sort()
    f = fear.reindex(dates).to_numpy(dtype=float)
    p = px.reindex(dates).to_numpy(dtype=float)
    n = len(dates)
    out = {"thr": thr, "n_signal": 0, "wins": {}}
    sig = [i for i in range(1, n) if f[i] >= thr and f[i - 1] < thr]
    out["n_signal"] = len(sig)
    for w in FWD_WINS:
        uncond = np.array([p[i + w] / p[i] - 1 for i in range(n - w)])
        cond = np.array([p[i + w] / p[i] - 1 for i in sig if i + w < n])
        out["wins"][w] = {
            "cond_mean": float(cond.mean()) if len(cond) else float("nan"),
            "cond_win": float((cond > 0).mean()) if len(cond) else float("nan"),
            "cond_n": len(cond),
            "uncond_mean": float(uncond.mean()) if len(uncond) else float("nan"),
            "excess": (float(cond.mean() - uncond.mean())
                       if len(cond) and len(uncond) else float("nan")),
        }
    return out


def _interval_stats(fear: pd.Series) -> dict:
    """监测间隔诊断：日间变动、自相关、极值 episode 持续天数、周频漏检。"""
    f = fear.dropna()
    d = f.diff().dropna()
    # 极值 episode：连续 fear≥75 的段长
    hi = (f >= 75).astype(int).to_numpy()
    runs = []
    c = 0
    for x in hi:
        if x:
            c += 1
        elif c:
            runs.append(c); c = 0
    if c:
        runs.append(c)
    # 周频（每 5 交易日采样）漏检的 fresh-cross≥75 数
    arr = f.to_numpy(dtype=float)
    daily_cross = sum(1 for i in range(1, len(arr)) if arr[i] >= 75 and arr[i - 1] < 75)
    weekly = arr[::5]
    weekly_cross = sum(1 for i in range(1, len(weekly)) if weekly[i] >= 75 and weekly[i - 1] < 75)
    return {
        "abs_chg_mean": float(d.abs().mean()), "abs_chg_p90": float(d.abs().quantile(0.90)),
        "abs_chg_max": float(d.abs().max()),
        "autocorr1": float(f.autocorr(lag=1)) if len(f) > 2 else float("nan"),
        "ep_n": len(runs), "ep_mean": float(np.mean(runs)) if runs else 0.0,
        "ep_median": float(np.median(runs)) if runs else 0.0,
        "ep_max": int(max(runs)) if runs else 0,
        "daily_cross75": daily_cross, "weekly_cross75": weekly_cross,
    }


def run(repo: BaseRepository, recompute_days: int) -> str:
    fear, src = _fear_series(repo, recompute_days)
    L = ["# 恐慌极值均值回归择时回测（创业板指 / 科创50）", ""]
    L.append(f"- 恐慌序列：{src}")
    if len(fear) < 120:
        L.append(f"\n⚠️ 恐慌样本不足（{len(fear)} 日）。先 bump `ops/fear-backfill.trigger`（--days 拉长）"
                 "或用 --recompute-days 让 fear_gauge 重算更长历史后再跑。")
        return "\n".join(L)
    L.append(f"- 判据预登记：入场次日收盘执行（无前视）；网格 THR_BUY{THR_BUYS}×THR_SELL{THR_SELLS}；"
             f"主格 {MAIN_BUY}/{MAIN_SELL}。四条边际判据见脚本头。")

    # ── 每标的：网格 + 固定持有对照 + 前瞻边际 ──
    edge_pass = {}
    for code, name in CODES.items():
        px = _index_closes(repo, code)
        if px.empty or len([d for d in px.index if d in fear.index]) < 120:
            L.append(f"\n## {name}（{code}）\n- ⚠️ index_daily 覆盖不足，跳过。")
            edge_pass[code] = None
            continue
        common0 = min(d for d in px.index if d in fear.index)
        common1 = max(d for d in px.index if d in fear.index)
        L.append(f"\n## {name}（{code}）")
        L.append(f"- 回测区间：{common0}~{common1}（与恐慌序列交集）")

        L.append("\n**择时网格**（一 episode 一进出；复利=顺序不重叠）：")
        L.append("| THR买/卖 | 笔数 | 胜率 | 平均单笔 | 中位 | 复利 | 在场年化 | 均持天 | 在场占比 | 同期买入持有 | 最差笔 |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|")
        main = None
        for tb in THR_BUYS:
            for ts in THR_SELLS:
                tr, dates = _episode_trades(fear, px, tb, ts)
                a = _agg(tr, dates, px)
                if a["n"] == 0:
                    L.append(f"| {tb}/{ts} | 0 | – | – | – | – | – | – | – | – | – |")
                    continue
                L.append(f"| {tb}/{ts} | {a['n']}{'*' if a['n_open'] else ''} | {a['win']:.0%} | "
                         f"{a['mean']:+.1%} | {a['median']:+.1%} | {a['comp']:+.1%} | "
                         f"{a['ann_inmkt']:+.0%} | {a['avg_days']:.0f} | {a['in_frac']:.0%} | "
                         f"{a['bh_active']:+.1%} | {a['worst']:+.1%} |")
                if tb == MAIN_BUY and ts == MAIN_SELL:
                    main = a
        L.append("  （笔数带 * = 含 1 笔样本末未平仓；在场年化=按在场天数折年、非日历年化）")

        # 固定持有对照
        L.append("\n**对照·极值入场后固定持有 N 日**（fresh-cross≥75，独立计均值）：")
        L.append("| 持有 | 信号数 | 胜率 | 平均 | 中位 |")
        L.append("|---|---|---|---|---|")
        for h in FIXED_HOLDS:
            fh = _fixed_hold(fear, px, MAIN_BUY, h)
            if fh["n"]:
                L.append(f"| {h}日 | {fh['n']} | {fh['win']:.0%} | {fh['mean']:+.1%} | {fh['median']:+.1%} |")
            else:
                L.append(f"| {h}日 | 0 | – | – | – |")

        # 前瞻边际
        fe = _forward_edge(fear, px, MAIN_BUY)
        L.append(f"\n**前瞻边际·极值(≥{MAIN_BUY})入场 vs 无条件**（隔离均值回归 alpha，信号 {fe['n_signal']} 次）：")
        L.append("| 前瞻窗 | 极值后均值 | 极值后胜率 | 无条件均值 | 超额 |")
        L.append("|---|---|---|---|---|")
        pos_excess = 0
        for w in FWD_WINS:
            e = fe["wins"][w]
            L.append(f"| {w}日 | {e['cond_mean']:+.2%} | {e['cond_win']:.0%} | "
                     f"{e['uncond_mean']:+.2%} | {e['excess']:+.2%} |")
            if np.isfinite(e["excess"]) and e["excess"] > 0 and e["cond_n"] >= 5:
                pos_excess += 1
        # 记录该标的边际判据
        edge_pass[code] = {
            "main": main, "fwd_pos_windows": pos_excess, "n_signal": fe["n_signal"],
        }

    # ── 监测间隔诊断（市场级、与标的无关）──
    iv = _interval_stats(fear)
    L.append("\n## 监测间隔诊断（恐慌值本身的动态）")
    L.append(f"- 日间变动 |ΔF|：均值 **{iv['abs_chg_mean']:.1f}** / 90分位 {iv['abs_chg_p90']:.1f} / "
             f"最大 {iv['abs_chg_max']:.1f}（满分 100）")
    L.append(f"- 一阶自相关 **{iv['autocorr1']:.2f}**（越接近 1 越慢变、越可日频跟）")
    L.append(f"- 极值 episode（连续 fear≥75）：{iv['ep_n']} 段，均 **{iv['ep_mean']:.1f}** 日 / "
             f"中位 {iv['ep_median']:.0f} 日 / 最长 {iv['ep_max']} 日")
    L.append(f"- fresh-cross≥75 计数：日频监测 **{iv['daily_cross75']}** 次 vs 周频(每5日)采样 "
             f"{iv['weekly_cross75']} 次 → 周频漏检 {iv['daily_cross75'] - iv['weekly_cross75']} 次")

    # ── 边际判据裁决 ──
    L.append("\n## 判据裁决（预登记四条）")
    codes_ok = [c for c in edge_pass if edge_pass[c]]
    if codes_ok:
        c2 = codes_ok[0]
        m = edge_pass[c2]["main"]
        cond1 = all(edge_pass[c]["fwd_pos_windows"] >= 2 and edge_pass[c]["n_signal"] >= 5
                    for c in codes_ok)
        cond2 = m is not None and m.get("win", 0) >= 0.55 and m.get("mean", -1) > 0
        cond3 = (m is not None and m.get("comp", -1) >= m.get("bh_active", 1e9)) or \
                (m is not None and m.get("comp", -1) >= 0.8 * m.get("bh_active", 1e9)
                 and m.get("in_frac", 1) < 0.5)
        # ④ 多标的一致：主格平均单笔同号
        means = [edge_pass[c]["main"]["mean"] for c in codes_ok
                 if edge_pass[c]["main"] is not None]
        cond4 = len(means) >= 2 and (all(x > 0 for x in means) or all(x < 0 for x in means))
        passed = sum([cond1, cond2, cond3, cond4])
        L.append(f"- ① 前瞻超额（≥2窗口正·样本≥5·两标的）：{'✅' if cond1 else '❌'}")
        L.append(f"- ② 主格({MAIN_BUY}/{MAIN_SELL})胜率≥55%且平均>0：{'✅' if cond2 else '❌'}")
        L.append(f"- ③ 择时复利≥买入持有（或≥80%收益&在场<50%天）：{'✅' if cond3 else '❌'}")
        L.append(f"- ④ 多标的方向一致：{'✅' if cond4 else '❌'}")
        L.append(f"- **{passed}/4 过**——≥3 则方向成立、可登记提案+预登记 E；否则记研究不新增提案。")
    else:
        L.append("- 无有效标的样本，未裁决。")

    L.append("\n### 注意项")
    L.append("- 指数不可直接买；实盘用 159915(创业板ETF)/588000(科创50ETF)，另计跟踪误差+双边费率(约0.02%~0.1%)+冲击成本，短线频繁进出会侵蚀边际。")
    L.append("- 恐慌值为 EOD 慢变量（分量多为收盘口径）；本回测已强制次日收盘执行，无前视。")
    L.append("- 科创50 样本自 2020-07 指数发布起，极值 episode 少、n 小，单标的结论弱于创业板指。")
    L.append("- 单市场历史 n 有限，非统计裁决；作方向探索与直觉校准，晋升须走 E 预登记多周期样本外。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="恐慌极值均值回归择时回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--recompute-days", type=int, default=900,
                    help="fear_daily 不足时用 fear_gauge 重算的目标交易日数（0=只用 fear_daily 全量）")
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
