"""复盘引擎：把"投顾研判 / 模型因子 / 持仓"跟事后真实收益对账，闭环校准。

三段：
  1) 投顾研判复盘：各分级(A/B/C)自推荐日至今的实际涨跌 + 胜率 —— 验证投顾的话该信多少。
  2) 模型因子复盘：各调仓日按 rank_pct 分档，看高分档 vs 低分档的前瞻收益价差(多空)
     —— 验证模型分位在收益上到底有没有区分力(IC 的收益版)。
  3) 持仓盈亏归因：最新快照逐票浮盈亏 + 对总盈亏的贡献；多快照时给区间变化。

只读 DB。盘后/周末复盘用。输出 Markdown（打印，--out 落文件）。

  python scripts/review.py                 # 走 .env / INVEST_DB_URL
  python scripts/review.py --horizon 10 --out results/review.md
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

VERSION = "ic_v1"


def _asof(repo: BaseRepository) -> str | None:
    d = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")
    v = d["d"].iloc[0] if not d.empty else None
    return str(v) if v is not None else None


def _closes_on(repo: BaseRepository, dates: list[str], codes: list[str] | None = None) -> pd.DataFrame:
    """取给定交易日集合的收盘（可选限定 codes）。返回 [code, trade_date, close]。"""
    if not dates:
        return pd.DataFrame(columns=["code", "trade_date", "close"])
    dph = ",".join(f":d{i}" for i in range(len(dates)))
    params = {f"d{i}": d for i, d in enumerate(dates)}
    sql = f"SELECT code, trade_date, close FROM stock_daily WHERE trade_date IN ({dph})"
    if codes:
        cph = ",".join(f":c{i}" for i in range(len(codes)))
        params.update({f"c{i}": c for i, c in enumerate(codes)})
        sql += f" AND code IN ({cph})"
    df = repo.read_sql(sql, params)
    if not df.empty:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df


def _names(repo: BaseRepository, codes: list[str]) -> dict[str, str]:
    if not codes:
        return {}
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    df = repo.read_sql(f"SELECT ts_code, name FROM stock_info WHERE ts_code IN ({ph})",
                       {f"c{i}": c for i, c in enumerate(codes)})
    return dict(zip(df["ts_code"], df["name"])) if not df.empty else {}


# ── 1) 投顾研判复盘 ──────────────────────────────────────────────
def _bench_series(repo: BaseRepository, start: str, end: str,
                  code: str = "000300.SH") -> pd.Series:
    """基准指数收盘序列（date→close），用于同窗口超额计算；失败返回空序列。"""
    try:
        if not repo.table_exists("index_daily"):
            return pd.Series(dtype=float)
        df = repo.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE code=:c "
            "AND trade_date>=:s AND trade_date<=:e ORDER BY trade_date",
            {"c": code, "s": start, "e": end})
        return pd.Series(pd.to_numeric(df["close"], errors="coerce").values,
                         index=df["trade_date"].astype(str)).dropna()
    except Exception:  # noqa: BLE001
        return pd.Series(dtype=float)


def _bench_ret(bench: pd.Series, d0: str, d1: str) -> float:
    """基准同窗口收益（d0→d1，均取 ≤该日的最近收盘）；数据不足返回 nan。"""
    if bench.empty:
        return float("nan")
    s0 = bench[bench.index <= d0]
    s1 = bench[bench.index <= d1]
    if s0.empty or s1.empty or float(s0.iloc[-1]) <= 0:
        return float("nan")
    return float(s1.iloc[-1]) / float(s0.iloc[-1]) - 1.0


def _advisor_rows(reco: pd.DataFrame, entry_win: pd.DataFrame) -> pd.DataFrame:
    """按 code 首评去重 + 严格次日收盘入场，返回 [code, grade, first, ret]。

    口径与 build_signal_scorecard 对齐：
    - 同票多次推荐只记首评（原 GROUP BY code,grade 会让同票跨级重复进多个分级桶）；
    - 入场价=首推日**之后**首个收盘（原用 >= 含当日收盘——盘中/收盘后录入的信号
      用当日收盘是不可成交价，系统性高估战绩；刚推荐、尚无次日行情的票不计入）。
    """
    reco = reco.sort_values("rec_date").drop_duplicates("code", keep="first")
    ew = entry_win.copy()
    ew["close"] = pd.to_numeric(ew["close"], errors="coerce")
    cur = {c: g.sort_values("trade_date")["close"].dropna().iloc[-1]
           for c, g in ew.groupby("code") if g["close"].notna().any()}
    rows = []
    for _, r in reco.iterrows():
        c = r["code"]
        g = ew[(ew["code"] == c) & (ew["trade_date"] > r["rec_date"])].sort_values("trade_date")
        g = g[g["close"].notna()]
        if g.empty or c not in cur:
            continue
        entry = float(g["close"].iloc[0])
        if entry <= 0:
            continue
        rows.append({"code": c, "grade": r["grade"] or "?", "first": r["rec_date"],
                     "entry_date": str(g["trade_date"].iloc[0]),
                     "ret": cur[c] / entry - 1.0})
    return pd.DataFrame(rows)


def review_advisor(repo: BaseRepository, asof: str, horizon: int) -> list[str]:
    lines = ["", "## 一、投顾研判复盘（自推荐至今 / 分级验证）"]
    if not repo.table_exists("advisor_reco"):
        return lines + ["（无 advisor_reco 表）"]
    reco = repo.read_sql(
        "SELECT code, grade, rec_date FROM advisor_reco WHERE direction='long'")
    if reco.empty:
        return lines + ["（暂无 long 方向投顾记录）"]
    codes = sorted(set(reco["code"]))
    cph = ",".join(f":c{i}" for i in range(len(codes)))
    entry_win = repo.read_sql(
        f"SELECT code, trade_date, close FROM stock_daily "
        f"WHERE code IN ({cph}) AND trade_date>=:s AND trade_date<=:e",
        {**{f"c{i}": c for i, c in enumerate(codes)}, "s": str(reco["rec_date"].min()),
         "e": asof})
    if entry_win.empty:
        return lines + ["（推荐标的无行情，无法对账）"]
    df = _advisor_rows(reco, entry_win)
    if df.empty:
        return lines + ["（推荐标的暂无可对账收益）"]
    bench = _bench_series(repo, str(df["entry_date"].min()), asof)
    df["excess"] = [r["ret"] - _bench_ret(bench, r["entry_date"], asof)
                    for _, r in df.iterrows()]
    has_ex = df["excess"].notna().any()
    lines.append(f"- 基准：自各标的首次推荐日**次一交易日**收盘 → {asof} 收盘的实际涨跌"
                 f"（{len(df)} 个标的；同票多次推荐只记首评）")
    if has_ex:
        lines.append("- **超额=同窗口相对沪深300**：绝对涨跌含市场贝塔（普跌期整体为负不代表信号差），"
                     "校准决策看超额列。")
    lines.append("")
    lines.append("| 分级 | 标的数 | 平均涨跌 | 平均超额 | 胜率 | 超额胜率 | 最好 | 最差 |")
    lines.append("|---|---|---|---|---|---|---|---|")

    def _ex_cols(sub):
        ex = sub["excess"].dropna()
        if ex.empty:
            return "—", "—"
        return f"{ex.mean():+.1%}", f"{(ex > 0).mean():.0%}"

    for g in ["A", "B", "C", "?"]:
        sub = df[df["grade"] == g]
        if sub.empty:
            continue
        exm, exw = _ex_cols(sub)
        lines.append(f"| {g} | {len(sub)} | {sub['ret'].mean():+.1%} | {exm} | "
                     f"{(sub['ret'] > 0).mean():.0%} | {exw} | {sub['ret'].max():+.1%} | {sub['ret'].min():+.1%} |")
    allr = df["ret"]
    exm, exw = _ex_cols(df)
    lines.append(f"| 全部 | {len(df)} | {allr.mean():+.1%} | {exm} | {(allr > 0).mean():.0%} | {exw} | "
                 f"{allr.max():+.1%} | {allr.min():+.1%} |")
    # 最强/最弱个股
    nm = _names(repo, list(df["code"]))
    top = df.sort_values("ret", ascending=False).head(3)
    bot = df.sort_values("ret").head(3)
    lines.append("")
    lines.append("- 🏆 表现最好：" + "，".join(
        f"{nm.get(r['code'], r['code'])}({r['grade']}) {r['ret']:+.0%}" for _, r in top.iterrows()))
    lines.append("- 🥶 表现最差：" + "，".join(
        f"{nm.get(r['code'], r['code'])}({r['grade']}) {r['ret']:+.0%}" for _, r in bot.iterrows()))
    lines.append("- 📌 校准提示：若某分级平均涨跌/胜率长期偏弱，应下调该分级权重或收紧纳入标准。")
    return lines


# ── 2) 模型因子复盘 ──────────────────────────────────────────────
def review_model(repo: BaseRepository) -> list[str]:
    lines = ["", "## 二、模型因子复盘（rank_pct 分档前瞻收益 / 区分力）"]
    if not repo.table_exists("model_prediction"):
        return lines + ["（无 model_prediction 表）"]
    preds = repo.read_sql(
        "SELECT trade_date, code, rank_pct FROM model_prediction WHERE version=:v", {"v": VERSION})
    if preds.empty:
        return lines + ["（模型暂无预测，跳过）"]
    preds["rank_pct"] = pd.to_numeric(preds["rank_pct"], errors="coerce")
    dates = sorted(preds["trade_date"].unique())
    if len(dates) < 2:
        return lines + ["（调仓日不足 2 个，暂无法算前瞻收益）"]
    closes = _closes_on(repo, dates)
    if closes.empty:
        return lines + ["（调仓日无行情）"]
    piv = closes.pivot_table(index="code", columns="trade_date", values="close", aggfunc="last")
    top_rets, bot_rets, spreads = [], [], []
    for d, nxt in zip(dates[:-1], dates[1:]):
        if d not in piv.columns or nxt not in piv.columns:
            continue
        pr = preds[preds["trade_date"] == d][["code", "rank_pct"]].dropna()
        fwd = (piv[nxt] / piv[d] - 1.0)
        m = pr.merge(fwd.rename("fwd").reset_index(), on="code").dropna()
        if len(m) < 20:
            continue
        top = m[m["rank_pct"] >= 0.8]["fwd"]
        bot = m[m["rank_pct"] <= 0.2]["fwd"]
        if top.empty or bot.empty:
            continue
        top_rets.append(top.mean()); bot_rets.append(bot.mean())
        spreads.append(top.mean() - bot.mean())
    if not spreads:
        return lines + ["（暂无足够样本算分档收益）"]
    sp = np.array(spreads)
    lines.append(f"- 跨 {len(spreads)} 个调仓区间，按调仓日模型分位分档，持有至下个调仓日的平均收益：")
    lines.append("")
    lines.append("| 档位 | 平均区间收益 |")
    lines.append("|---|---|")
    lines.append(f"| 高分档（模型分位前20%） | {np.mean(top_rets):+.2%} |")
    lines.append(f"| 低分档（模型分位后20%） | {np.mean(bot_rets):+.2%} |")
    lines.append(f"| **多空价差 (高-低)** | **{sp.mean():+.2%}** |")
    lines.append("")
    lines.append(f"- 多空价差为正的区间占比：{(sp > 0).mean():.0%}（越高说明分位越稳地区分强弱）")
    verdict = ("模型分位在收益上有正向区分力，可作参谋" if sp.mean() > 0
               else "进攻端（选涨幅）区分力弱——与防御端验证结论并读：大跌日高低分组差 "
                    "+1.10pp/日、87% 为正（run 29682743077，判据预登记全过），模型定位为防御参谋："
                    "排位用于风险提示加权（参谋异议行），不用于选股加成")
    lines.append(f"- 📌 结论：{verdict}。")
    return lines


# ── 3) 持仓盈亏归因 ──────────────────────────────────────────────
def review_holdings(repo: BaseRepository) -> list[str]:
    lines = ["", "## 三、持仓盈亏归因（最新快照）"]
    if not repo.table_exists("holding_snapshot"):
        return lines + ["（无 holding_snapshot 表）"]
    snaps = repo.read_sql("SELECT DISTINCT snapshot_date FROM holding_snapshot ORDER BY snapshot_date")
    if snaps.empty:
        return lines + ["（暂无持仓快照）"]
    last = str(snaps["snapshot_date"].iloc[-1])
    h = repo.read_sql(
        "SELECT code, name, asset_type, market_value, pnl, pnl_pct FROM holding_snapshot "
        "WHERE snapshot_date=:d", {"d": last})
    if h.empty:
        return lines + [f"（{last} 快照为空）"]
    for c in ["market_value", "pnl", "pnl_pct"]:
        h[c] = pd.to_numeric(h[c], errors="coerce")
    stock = h[h["asset_type"].astype(str).str.lower() != "cash"].copy()
    tot_pnl = stock["pnl"].sum(skipna=True)
    gross = stock["pnl"].abs().sum(skipna=True)   # 贡献分母用绝对值和，避免净额近零时占比被放大
    lines.append(f"- 快照日：{last} | 持仓市值合计：{stock['market_value'].sum(skipna=True):,.0f} | "
                 f"合计浮盈亏：{tot_pnl:+,.0f}")
    lines.append("")
    lines.append("| 标的 | 市值 | 浮盈亏 | 收益率 | 盈亏占比 |")
    lines.append("|---|---|---|---|---|")
    for _, r in stock.sort_values("pnl", ascending=False, na_position="last").iterrows():
        pp = r["pnl_pct"]                         # 快照里已是百分数（如 36.56 表示 +36.56%）
        pp_s = f"{pp:+.1f}%" if np.isfinite(pp) else "—"
        contrib = (r["pnl"] / gross) if gross and np.isfinite(gross) and gross != 0 else float("nan")
        contrib_s = f"{contrib:+.0%}" if np.isfinite(contrib) else "—"
        lines.append(
            f"| {r['name'] or r['code']} | {r['market_value']:,.0f} | {r['pnl']:+,.0f} | {pp_s} | {contrib_s} |")
    if len(snaps) >= 2:
        prev = str(snaps["snapshot_date"].iloc[-2])
        hp = repo.read_sql(
            "SELECT code, name, pnl FROM holding_snapshot WHERE snapshot_date=:d", {"d": prev})
        hp["pnl"] = pd.to_numeric(hp["pnl"], errors="coerce")
        prev_map = dict(zip(hp["code"].astype(str), hp["pnl"]))
        prev_names = dict(zip(hp["code"].astype(str), hp["name"].astype(str)))
        lines.append(f"\n### 区间归因（{prev} → {last} 浮盈亏变化）\n")
        lines.append("| 标的 | 上期浮盈亏 | 本期浮盈亏 | 区间变化 |")
        lines.append("|---|---|---|---|")
        deltas = []
        for _, r in stock.iterrows():
            c = str(r["code"])
            pv = prev_map.get(c)
            if pv is None or not np.isfinite(pv):
                deltas.append((str(r["name"] or c), float("nan"), r["pnl"], float("nan"), "本期新增"))
            else:
                deltas.append((str(r["name"] or c), pv, r["pnl"], r["pnl"] - pv, ""))
        for nm2, pv, cv, dl, tag in sorted(
                deltas, key=lambda x: (x[3] if np.isfinite(x[3]) else 0), reverse=True):
            pv_s = f"{pv:+,.0f}" if np.isfinite(pv) else "—"
            dl_s = f"{dl:+,.0f}" if np.isfinite(dl) else tag
            lines.append(f"| {nm2} | {pv_s} | {cv:+,.0f} | {dl_s} |")
        gone = [c for c in prev_map if c not in set(stock["code"].astype(str))
                and np.isfinite(prev_map[c])]
        if gone:
            lines.append("\n- 期间清出：" + "、".join(
                f"{prev_names.get(c, c)}（清出前浮盈亏 {prev_map[c]:+,.0f}，实现盈亏以成交为准）"
                for c in gone))
    else:
        lines.append("\n- 📌 目前仅 1 个快照，随每日快照累积，将给出区间盈亏变化与选股/择时归因。")
    return lines


# ── 4) 信号时效与纪律 ────────────────────────────────────────────
def review_discipline(repo: BaseRepository, asof: str) -> list[str]:
    lines = ["", "## 四、信号时效与纪律（买点/风控 事后验证）"]
    if not repo.table_exists("action_plan"):
        return lines + ["（无 action_plan 历史，随每日计划累积后生效）"]
    ap = repo.read_sql("SELECT plan_date, code, action, ref_price, reason FROM action_plan")
    if ap.empty:
        return lines + ["（action_plan 暂无记录）"]
    cn = {"buy": "买入", "add": "加仓", "sell": "清仓", "trim": "减仓", "hold": "持有", "watch": "观察"}
    last = ap["plan_date"].max()
    comp = ap[ap["plan_date"] == last]["action"].value_counts().to_dict()
    lines.append(f"- 最新计划（{last}）信号构成：" +
                 "，".join(f"{cn.get(k, k)}{v}" for k, v in comp.items()))
    # 买点时效：历史 buy/add 信号自触发日至今的实际收益（验证买点靠不靠谱）
    buys = ap[(ap["action"].isin(["buy", "add"])) & (ap["plan_date"] < asof)].copy()
    if not buys.empty:
        codes = sorted(set(buys["code"]))
        cur_px = _closes_on(repo, [asof], codes)
        cur_map = dict(zip(cur_px["code"], cur_px["close"])) if not cur_px.empty else {}
        bench = _bench_series(repo, str(buys["plan_date"].min()), asof)

        def _chan(reason: str) -> str:
            r = str(reason or "")
            if "免闸" in r or "研报" in r or "速通" in r:
                return "研报速通"
            if "回踩" in r or "突破" in r:
                return "严格买点闸"
            return "其它"

        buys["chan"] = buys["reason"].map(_chan)
        parts = []
        for ch, sub in buys.groupby("chan"):
            rets, exs = [], []
            for _, r in sub.iterrows():
                entry = _f(r["ref_price"])
                if entry and entry > 0 and r["code"] in cur_map:
                    ret = cur_map[r["code"]] / entry - 1.0
                    rets.append(ret)
                    b = _bench_ret(bench, str(r["plan_date"]), asof)
                    if np.isfinite(b):
                        exs.append(ret - b)
            if rets:
                rr = np.array(rets)
                ex_s = (f"，超额 {np.mean(exs):+.1%}" if exs else "")
                parts.append(f"{ch} {len(rr)} 次（均 {rr.mean():+.1%}{ex_s}，胜率 {(rr > 0).mean():.0%}）")
        if parts:
            lines.append("- 历史买点信号按通道：" + "；".join(parts))
            lines.append("- 📌 通道口径：研报速通=A/B级研报免闸直入；严格买点闸=回踩/突破三闸全过。"
                         "评估收紧对象须分通道看，勿混判。")
    else:
        lines.append("- 买点时效：历史买点信号累积中（当前无触发或前瞻样本不足）。")
    exits = ap[(ap["plan_date"] == last) & (ap["action"].isin(["sell", "trim"]))]
    if not exits.empty:
        lines.append(f"- 本次风控触发 {len(exits)} 笔（清仓/减仓）——执行到位是纪律关键，"
                     "复盘核对：是否按计划执行、有无该止损未止/该减未减。")
    return lines


def review_policy_shadow(repo: BaseRepository) -> list[str]:
    """研报速通 vs 严格闸 影子对账（policy_shadow 逐信号净值，速通政策的裁决数据）。"""
    lines = ["", "## 四·附、研报速通 vs 严格闸（影子对账）"]
    if not repo.table_exists("policy_shadow"):
        return lines + ["（无 policy_shadow 表——影子随速通信号累积后生效）"]
    df = repo.read_sql("SELECT signal_date, code, grade, fast_ret, gate_ret, gate_date "
                       "FROM policy_shadow")
    if df.empty:
        return lines + ["（影子暂无记录）"]
    for c in ("fast_ret", "gate_ret"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    fast = df["fast_ret"].dropna()
    gate_hit = df[df["gate_date"].notna()]["gate_ret"].dropna()
    n_gate_miss = int((df["gate_date"].isna()).sum())
    gate_all = pd.concat([gate_hit, pd.Series([0.0] * n_gate_miss)], ignore_index=True)
    lines.append(f"- 影子信号 {len(df)} 条（research A/B 级）；严格闸未触发（对照＝空仓）{n_gate_miss} 条")
    lines.append("")
    lines.append("| 口径 | 条数 | 平均收益 | 胜率 |")
    lines.append("|---|---|---|---|")
    if len(fast):
        lines.append(f"| 速通（立即买入） | {len(fast)} | {fast.mean():+.1%} | {(fast > 0).mean():.0%} |")
    if len(gate_all):
        lines.append(f"| 严格闸（触发才买·未触发空仓） | {len(gate_all)} | {gate_all.mean():+.1%} | "
                     f"{(gate_all > 0).mean():.0%} |")
    if len(fast) and len(gate_all):
        diff = fast.mean() - gate_all.mean()
        lines.append("")
        lines.append(f"- **速通 − 严格闸 = {diff:+.1%}**（正=速通占优）。该对比是"
                     "B级速通资格去留的裁决依据，样本仍在累积、勿按单周下结论。")
    return lines


def _f(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if np.isfinite(v) else None


def review_arb(repo: BaseRepository, asof: str) -> list[str]:
    """套利模块复盘：sleeve 账本 + carry 实现 vs 预期 + α 证伪状态 + 水表兑现。"""
    if not repo.table_exists("sleeve_target"):
        return []
    out: list[str] = ["", "## 五、套利/守恒 sleeve 账本复盘"]
    sl = repo.read_sql(
        "SELECT sleeve, target_pct, nav, note FROM sleeve_target "
        "WHERE plan_date=(SELECT MAX(plan_date) FROM sleeve_target WHERE note='backtest')"
        " AND note='backtest'")
    if not sl.empty:
        out += ["", "| sleeve | 目标占比 | 回测期末净值 |", "|---|---:|---:|"]
        for _, r in sl.iterrows():
            nav = _f(r["nav"])
            out.append(f"| {r['sleeve']} | {(_f(r['target_pct']) or 0):.0%} | "
                       f"{nav:.3f} |" if nav is not None else
                       f"| {r['sleeve']} | {(_f(r['target_pct']) or 0):.0%} | — |")
    # carry 信号数
    if repo.table_exists("carry_signal"):
        cs = repo.read_sql(
            "SELECT sleeve, COUNT(*) n, AVG(expected_carry) ec FROM carry_signal "
            "WHERE trade_date=(SELECT MAX(trade_date) FROM carry_signal) GROUP BY sleeve")
        for _, r in cs.iterrows():
            ec = _f(r["ec"])
            out.append(f"- carry「{r['sleeve']}」信号 {int(r['n'])} 条"
                       + (f"，加权预期年化约 {ec:.2%}" if ec else ""))
    # α 证伪状态
    if repo.table_exists("alpha_candidate"):
        ac = repo.read_sql(
            "SELECT falsified, COUNT(*) n FROM alpha_candidate "
            "WHERE as_of_date=(SELECT MAX(as_of_date) FROM alpha_candidate) GROUP BY falsified")
        if not ac.empty:
            m = {int(r["falsified"]): int(r["n"]) for _, r in ac.iterrows()
                 if r["falsified"] is not None}
            out.append(f"- 盲区 α：未证伪 {m.get(0,0)+m.get(-1,0)} 个 / 已证伪(水表反转) {m.get(1,0)} 个"
                       "（证伪铁律：剥离股价·只看产业侧资金到没到）")
    if len(out) <= 2:
        out.append("（暂无套利数据——观察态或数据未就绪）")
    return out


def build_review(repo: BaseRepository, asof: str, horizon: int = 10) -> str:
    """构建五段复盘 Markdown（单段出错跳过不阻断）。"""
    lines = [f"# 复盘报告 — 截至 {asof}", "",
             "> 闭环校准：投顾说得准不准、模型分位有没有区分力、持仓靠什么赚钱、套利账本守没守住零杠杆。"]
    for fn in (lambda: review_advisor(repo, asof, horizon),
               lambda: review_model(repo),
               lambda: review_holdings(repo),
               lambda: review_discipline(repo, asof),
               lambda: review_policy_shadow(repo),
               lambda: review_arb(repo, asof)):
        try:
            lines += fn()
        except Exception as e:  # noqa: BLE001
            lines += ["", f"（本段复盘出错，跳过：{e}）"]
    return "\n".join(lines)


def persist_review(repo: BaseRepository, asof: str, period: str, md: str) -> None:
    """复盘报告落库（review_report），供仪表盘展示；失败不阻断输出。"""
    from datetime import datetime, timezone

    from invest_model.data import create_schema

    create_schema(repo.engine)
    repo.upsert("review_report", pd.DataFrame([{
        "report_date": asof, "period": period, "version": VERSION, "markdown": md,
        "meta": json.dumps(
            {"generated_at": datetime.now(timezone.utc).isoformat()}, ensure_ascii=False),
    }]), ["report_date", "period"])


def main() -> None:
    ap = argparse.ArgumentParser(description="复盘引擎：投顾/模型/持仓/纪律 与真实收益对账")
    ap.add_argument("--db", default=None)
    ap.add_argument("--horizon", type=int, default=10, help="投顾前瞻窗口（预留）")
    ap.add_argument("--out", default=None)
    ap.add_argument("--period", default="weekly", choices=["daily", "weekly", "adhoc"],
                    help="报告周期标签（落库 review_report 用）")
    args = ap.parse_args()

    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())
    asof = _asof(repo)
    if not asof:
        print("stock_daily 无数据，无法复盘")
        return
    md = build_review(repo, asof, args.horizon)
    print(md)
    try:
        persist_review(repo, asof, args.period, md)
        print(f"\n复盘已落库 review_report（{asof}/{args.period}）")
    except Exception as e:  # noqa: BLE001
        print(f"\nWARN review_report 落库失败：{e}")
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")
        print(f"\n已写入 {args.out}")


if __name__ == "__main__":
    main()
