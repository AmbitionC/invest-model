"""E13：负 Gamma × 临近到期 挤压风险探针（P17 预登记验证）——
   "临近月度 OpEx + VIX 骤升 + SPY/QQQ 破 MA10" 这个**代理窗口**，是否真的让卖 CSP 系统性更差？

> 仅美股模块。若证不出显著恶化 → 代理无效 → 不上线（US-O5 保持 off，卖 put 行为零改动）。

H0（零假设）：在上述代理窗口内卖现金担保 put（CSP），其被行权后的 20 日浮亏与回本率，
  相对非窗口卖同规格 CSP **没有系统性恶化** → 该代理无法识别负 Gamma 踩踏 → 不值得收紧。

出处：2026-07-13 投顾收盘笔记（做市商负 Gamma 越跌越卖死亡螺旋 + 临近到期 Gamma 放大 +
  杠杆 ETF 尾盘顺周期再平衡）。机制为通用期权做市学。

口径（诚实声明 · 代理法局限）：
  - **无真实 dealer Gamma 敞口（GEX）数据**——yfinance 拿不到稳定的全 strike 期权 OI。
    用「临近月度 OpEx + VIX 骤升 + 破 MA10」**近似**负 Gamma 环境；E13 正是检验这个代理是否有判别力。
  - CSP 被行权/浮亏用标的（SPY/QQQ）**远期路径**近似：在信号日"卖 ATM CSP"的坏处 ≈ 标的未来
    HOLD_DAYS 的最大浮亏与回本率（接货后越跌越深 = 越坏）。非真实期权链回测（期权历史不可回溯）。
  - 到期日历按规则推算（月度=第三个周五；假日顺延到当月最近交易日），不依赖 OI 数据。

**预登记口径细化（跑数前 · 未见任何结果 · 非看结果改判据）**：原登记写"月度或周度到期"，
  实现前收窄为**仅标准月度 OpEx（第三个周五）**——周度到期使"临近"近乎恒真、失去判别力；
  且负 Gamma/做市商 Gamma 敞口集中于月度 OpEx。此为跑数前的诚实口径修订，记录在案。

窗口定义（预登记 · 写死勿改）：距月度 OpEx ≤ WINDOW_DAYS_TO_EXPIRY 交易日
  且（VIX 单日涨幅 ≥ VIX_SPIKE_PCT 或 VIX ≥ VIX_ABS_LEVEL）且 标的 < MA(MA_WINDOW)。

过关判据（预登记 · 写死勿改 · 不看结果改判据）：
  ① 窗口样本数 ≥ MIN_WINDOW_SAMPLES；
  ② 窗口内被行权后 HOLD_DAYS 平均最大浮亏，较非窗口 **恶化 ≥ WORSEN_DRAWDOWN_PP** 个百分点；
  ③ 窗口内"HOLD_DAYS 内回本（远期收益 ≥0）"占比，较非窗口 **低 ≥ WORSEN_RECOVER_PP** 个百分点。
  三条同时满足 → 代理有效、值得收紧（US-O5 达标，可议高置信直升）。
  样本 < MIN_WINDOW_SAMPLES → **不判定、不动**。

用法（在 GitHub Actions 美国 runner 上，yfinance 可用）：
  python scripts/validation/e13_gamma_squeeze.py [--period 15y] [--out results/e13.md] [--post-issue]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.us import signals as S  # noqa: E402

# ── 预登记常量（判据先写死；晋升前逐字保持，不许看结果回调）──────────────
WINDOW_DAYS_TO_EXPIRY = 2      # 距月度 OpEx ≤2 交易日
VIX_SPIKE_PCT = 0.15          # VIX 单日涨幅 ≥ +15%
VIX_ABS_LEVEL = 35.0          # 或 VIX 绝对值 ≥ 35
MA_WINDOW = 10                # 标的破 MA10
HOLD_DAYS = 20                # 被行权后持有观察期（交易日）
MIN_WINDOW_SAMPLES = 20       # 窗口样本 <20 不判定
WORSEN_DRAWDOWN_PP = 5.0      # 窗口内 20 日平均最大浮亏较非窗口恶化 ≥5pp
WORSEN_RECOVER_PP = 10.0      # 窗口内 20 日回本率较非窗口低 ≥10pp
UNIVERSE = ("SPY", "QQQ")     # 代理标的
VIX_CODE = "^VIX"


def _fetch(period: str) -> dict[str, pd.Series]:
    """yfinance 拉 UNIVERSE + VIX 收盘序列（index=YYYYMMDD 字符串）。"""
    import yfinance as yf
    codes = list(UNIVERSE) + [VIX_CODE]
    data = yf.download(codes, period=period, interval="1d",
                       auto_adjust=True, group_by="ticker", progress=False, threads=True)
    out: dict[str, pd.Series] = {}
    for code in codes:
        df = data[code] if len(codes) > 1 else data
        df = df.dropna(subset=["Close"])
        if df.empty:
            continue
        s = pd.Series(df["Close"].values,
                      index=pd.to_datetime(df.index).strftime("%Y%m%d"))
        out[code] = s
    return out


def _forward_metrics(close: pd.Series, hold: int) -> pd.DataFrame:
    """每日的远期 hold 日：最大浮亏（min 累计收益）与终点收益（回本判定）。"""
    c = pd.to_numeric(close, errors="coerce").astype(float)
    n = len(c)
    vals = c.values
    max_dd = np.full(n, np.nan)
    end_ret = np.full(n, np.nan)
    for i in range(n):
        j = min(i + hold, n - 1)
        if j <= i:
            continue
        fwd = vals[i + 1:j + 1] / vals[i] - 1.0   # 未来 1..hold 日相对收益
        if len(fwd) == 0:
            continue
        max_dd[i] = float(fwd.min())              # 最深浮亏（接货后越跌越深）
        end_ret[i] = float(vals[j] / vals[i] - 1.0)
    return pd.DataFrame({"max_dd": max_dd, "end_ret": end_ret}, index=c.index)


def run(period: str = "15y") -> dict:
    raw = _fetch(period)
    if VIX_CODE not in raw or not any(u in raw for u in UNIVERSE):
        raise RuntimeError("E13：yfinance 未取到 SPY/QQQ/VIX——无法验证（疑限流/网络）")
    vix = raw[VIX_CODE]

    win_dd, win_rec, non_dd, non_rec = [], [], [], []
    per_underlying = {}
    for u in UNIVERSE:
        if u not in raw:
            continue
        bench = raw[u]
        window = S.label_squeeze_windows(
            bench, vix,
            days_to_expiry_max=WINDOW_DAYS_TO_EXPIRY, vix_spike_pct=VIX_SPIKE_PCT,
            vix_abs=VIX_ABS_LEVEL, ma_window=MA_WINDOW)
        fm = _forward_metrics(bench, HOLD_DAYS)
        idx = window.index.intersection(fm.index)
        window = window.reindex(idx)
        fm = fm.reindex(idx).dropna()
        idx = fm.index
        window = window.reindex(idx).fillna(False)
        w = window.values.astype(bool)
        dd = fm["max_dd"].values
        rec = (fm["end_ret"].values >= 0).astype(float)
        per_underlying[u] = {
            "win_n": int(w.sum()),
            "win_dd": float(np.mean(dd[w])) if w.sum() else float("nan"),
            "win_rec": float(np.mean(rec[w])) if w.sum() else float("nan"),
            "non_dd": float(np.mean(dd[~w])) if (~w).sum() else float("nan"),
            "non_rec": float(np.mean(rec[~w])) if (~w).sum() else float("nan"),
        }
        win_dd += list(dd[w]); win_rec += list(rec[w])
        non_dd += list(dd[~w]); non_rec += list(rec[~w])

    win_n = len(win_dd)
    m_win_dd = float(np.mean(win_dd)) if win_dd else float("nan")
    m_non_dd = float(np.mean(non_dd)) if non_dd else float("nan")
    m_win_rec = float(np.mean(win_rec)) if win_rec else float("nan")
    m_non_rec = float(np.mean(non_rec)) if non_rec else float("nan")

    dd_worsen_pp = (m_non_dd - m_win_dd) * 100.0    # 窗口更负 → 正值=恶化幅度
    rec_drop_pp = (m_non_rec - m_win_rec) * 100.0   # 窗口回本率更低 → 正值=下降幅度

    if win_n < MIN_WINDOW_SAMPLES:
        verdict = "INSUFFICIENT"
    elif dd_worsen_pp >= WORSEN_DRAWDOWN_PP and rec_drop_pp >= WORSEN_RECOVER_PP:
        verdict = "PASS"
    else:
        verdict = "FAIL"

    return {
        "period": period, "pooled_universe": [u for u in UNIVERSE if u in raw],
        "win_n": win_n, "non_n": len(non_dd),
        "win_max_dd": m_win_dd, "non_max_dd": m_non_dd, "dd_worsen_pp": dd_worsen_pp,
        "win_recover": m_win_rec, "non_recover": m_non_rec, "rec_drop_pp": rec_drop_pp,
        "per_underlying": per_underlying, "verdict": verdict,
    }


def _report_md(r: dict) -> str:
    v = r["verdict"]
    badge = {"PASS": "✅ PASS（代理有效·可议高置信直升 US-O5）",
             "FAIL": "❌ FAIL（代理无判别力·维持 off 不上线）",
             "INSUFFICIENT": "⚠️ INSUFFICIENT（窗口样本不足·不判定不动）"}[v]
    L = [
        f"## E13 · 负 Gamma×到期 挤压探针验证 — {badge}",
        "",
        f"- 代理标的（合并）：{', '.join(r['pooled_universe'])}｜历史窗口：{r['period']}",
        f"- 窗口定义（写死）：距月度 OpEx ≤{WINDOW_DAYS_TO_EXPIRY} 交易日 且（VIX 单日≥+{VIX_SPIKE_PCT:.0%} "
        f"或 ≥{VIX_ABS_LEVEL:.0f}）且 标的<MA{MA_WINDOW}；被行权后观察 {HOLD_DAYS} 交易日",
        "",
        "| 指标 | 窗口内 | 非窗口 | 差值 |",
        "|---|---|---|---|",
        f"| 样本数 | {r['win_n']} | {r['non_n']} | — |",
        f"| 20日平均最大浮亏 | {r['win_max_dd']:.2%} | {r['non_max_dd']:.2%} | "
        f"恶化 {r['dd_worsen_pp']:+.1f}pp |",
        f"| 20日回本率(远期≥0) | {r['win_recover']:.1%} | {r['non_recover']:.1%} | "
        f"下降 {r['rec_drop_pp']:+.1f}pp |",
        "",
        f"**过关判据（预登记写死）**：样本≥{MIN_WINDOW_SAMPLES}、浮亏恶化≥{WORSEN_DRAWDOWN_PP:.0f}pp、"
        f"回本率下降≥{WORSEN_RECOVER_PP:.0f}pp —— 三条同时满足才算代理有效。",
        "",
        "分标的：" + "；".join(
            f"{u}(窗口{d['win_n']}·浮亏{d['win_dd']:.1%}vs{d['non_dd']:.1%}·回本{d['win_rec']:.0%}vs{d['non_rec']:.0%})"
            for u, d in r["per_underlying"].items()),
        "",
        "> 代理法局限：无真实 dealer Gamma/GEX 数据，用「临近月度 OpEx+VIX 骤升+破 MA10」近似；"
        "CSP 结局用标的远期路径近似。**FAIL 即代理无效、诚实不上线**。",
    ]
    if v == "PASS":
        L.append("\n**结论**：代理窗口内卖 put 确实系统性更差 → US-O5 值得接线（若同过「高置信直升」四条则直升 strict）。")
    elif v == "FAIL":
        L.append("\n**结论**：代理窗口未显著更差 → 代理无判别力，US-O5 维持 off、不改卖 put 行为；机制记入知识库。")
    else:
        L.append("\n**结论**：窗口样本不足以判定 → 维持现状，后续样本累积或口径再议（须重新预登记）。")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="E13 负 Gamma×到期 挤压探针预登记验证")
    ap.add_argument("--period", default="15y")
    ap.add_argument("--out", default=None)
    ap.add_argument("--post-issue", action="store_true", help="回帖到「🧪 验证报告」issue")
    args = ap.parse_args()

    r = run(args.period)
    md = _report_md(r)
    print(md)
    print(f"\n[E13] verdict={r['verdict']} win_n={r['win_n']} "
          f"dd_worsen={r['dd_worsen_pp']:.1f}pp rec_drop={r['rec_drop_pp']:.1f}pp")
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")
    if args.post_issue:
        from datetime import datetime, timezone
        from faas import gh_notify
        ts = datetime.now(timezone.utc).strftime("%Y%m%d %H:%M UTC")
        gh_notify.post_issue_comment(
            "🧪 验证报告",
            seed_body="本 issue 汇总预登记验证（E 系列）结论。",
            comment_body=f"（{ts}）\n\n{md}",
            dedupe_prefix="## E13 · 负 Gamma")


if __name__ == "__main__":
    main()
