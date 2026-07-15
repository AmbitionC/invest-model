"""历史计划买入信号全量回放（用户命题 2026-07-15·只读不落库）。

系统自 2026-07-01 出计划以来，issue #9 全部「建议买入」信号共 4 只（唯一口径）：
均为 2026-07-06 计划的 A/B 级研报「3 日内免闸半仓直入」信号（0707 重挂为同一
窗口的延续，不重复计）；严格买点闸（回踩/突破）在全部历史计划中 0 触发。

  20260706  001309.SZ 德明利    次日尾盘 ≤935.0
  20260706  002463.SZ 沪电股份  次日尾盘 ≤128.83
  20260706  300308.SZ 中际旭创  次日尾盘 ≤1098.92
  20260706  688041.SH 海光信息  次日尾盘 ≤339.0

回放口径（尽量贴生产，简化处如实标注）：
  - 成交：信号后 3 个交易日内首个「收盘 ≤ 限价」日，按当日收盘成交（"次日尾盘"
    市价近似）；一次性全额成交（生产为半仓+回踩补半仓，简化为单笔，注明）。
  - 离场：逐日调用**生产 risk.py**——evaluate_holding（硬止损-8%/破MA20·P10缓冲）
    + profit_protect（浮盈15%后自峰值回撤 8%减半/12%清仓）+ armed_ladder（盈利后
    破MA5减半/MA10清仓），多信号取最严；卖出按当日收盘。time_stop 默认关（同生产）。
  - 对照：同一成交价「持有不动至今」的收益，及期间最大浮亏。

输出：每笔信号的动作日志 + 策略收益 vs 持有收益；汇总胜率/均值。
用法（Actions）：python scripts/analysis/plan_signal_replay.py [--out ...]
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
)

SIGNALS = [
    ("20260706", "001309.SZ", "德明利", 935.0),
    ("20260706", "002463.SZ", "沪电股份", 128.83),
    ("20260706", "300308.SZ", "中际旭创", 1098.92),
    ("20260706", "688041.SH", "海光信息", 339.0),
]
FILL_DAYS = 3


def _closes(repo: BaseRepository, code: str) -> pd.Series:
    df = repo.read_sql(
        "SELECT trade_date, close FROM stock_daily WHERE code=:c AND trade_date>='20260401' "
        "ORDER BY trade_date", {"c": code})
    s = pd.Series(pd.to_numeric(df["close"], errors="coerce").values,
                  index=df["trade_date"].astype(str))
    try:  # 前复权 fail-open（窗口内如无除权则原样）
        adj = repo.read_sql(
            "SELECT trade_date, adj_factor FROM stock_adj WHERE code=:c AND trade_date>='20260401'",
            {"c": code})
        if not adj.empty:
            f = pd.Series(pd.to_numeric(adj["adj_factor"], errors="coerce").values,
                          index=adj["trade_date"].astype(str)).reindex(s.index).ffill()
            if f.notna().any():
                s = s * (f / float(f.dropna().iloc[-1]))
    except Exception:  # noqa: BLE001
        pass
    return s.dropna()


def _replay(closes: pd.Series, sig_date: str, limit: float,
            cfg: RiskConfig) -> dict:
    dates = list(closes.index)
    after = [d for d in dates if d > sig_date]
    fill_date = fill_px = None
    for d in after[:FILL_DAYS]:
        if float(closes[d]) <= limit:
            fill_date, fill_px = d, float(closes[d])
            break
    if fill_date is None:
        return {"filled": False}

    pos, realized, tier_ma, tier_pp = 1.0, 0.0, 0, 0
    log: list[str] = [f"{fill_date} 成交@{fill_px:.2f}"]
    exit_done = False
    for d in dates[dates.index(fill_date) + 1:]:
        hist = closes.loc[:d]
        hold_hist = closes.loc[fill_date:d]
        px = float(closes[d])
        dec = evaluate_holding(hist, fill_px, cfg, prev_tier=tier_ma)
        cands = [dec]
        ppd = profit_protect(hold_hist, fill_px, cfg, prev_tier=tier_pp)
        if ppd is not None:
            cands.append(ppd)
        try:
            lad = armed_ladder(hist, fill_date, fill_px, cfg)
            if lad is not None:
                cands.append(lad)
        except Exception:  # noqa: BLE001 — 梯子回放异常不阻断（如样本不足）
            pass
        tier_ma = max(tier_ma, dec.new_tier)
        if ppd is not None:
            tier_pp = max(tier_pp, ppd.new_tier)
        exits = [x for x in cands if x.action == "exit"]
        trims = [x for x in cands if x.action == "trim"]
        if exits:
            realized += pos * px
            log.append(f"{d} 清仓@{px:.2f}（{exits[0].reason}）")
            pos = 0.0
            exit_done = True
            break
        if trims:
            keep = min(x.keep_frac for x in trims)
            sold = pos * (1 - keep)
            realized += sold * px
            pos *= keep
            log.append(f"{d} 减仓至{pos:.0%}@{px:.2f}（{trims[0].reason}）")
    last_d, last_px = dates[-1], float(closes.iloc[-1])
    value = realized + pos * last_px
    strat_ret = value / fill_px - 1
    hold_path = closes.loc[fill_date:]
    hold_ret = last_px / fill_px - 1
    mdd = float(hold_path.min() / fill_px - 1)
    if not exit_done and pos > 0:
        log.append(f"{last_d} 持有中@{last_px:.2f}（仓位{pos:.0%}）")
    return {"filled": True, "fill_date": fill_date, "fill_px": fill_px,
            "strat_ret": strat_ret, "hold_ret": hold_ret, "mdd": mdd,
            "last_d": last_d, "last_px": last_px, "log": log}


def main() -> None:
    ap = argparse.ArgumentParser(description="历史计划买入信号回放")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db)
    repo = BaseRepository(engine)
    cfg = RiskConfig()   # 生产默认口径

    L = ["## 历史计划「建议买入」信号全量回放（生产 risk.py 口径）", "",
         "| 信号日 | 标的 | 限价 | 成交 | 策略收益 | 持有至今 | 期间最大浮亏 | 最新价(日期) |",
         "|---|---|---:|---|---:|---:|---:|---|"]
    details, srets, hrets = [], [], []
    for sig_date, code, name, limit in SIGNALS:
        closes = _closes(repo, code)
        r = _replay(closes, sig_date, limit, cfg)
        if not r.get("filled"):
            L.append(f"| {sig_date} | {name} | {limit} | ✗ 3日内未触及限价，未成交 | — | — | — | — |")
            details.append(f"**{name}**：未成交（3 日窗口收盘均高于限价）")
            continue
        srets.append(r["strat_ret"]); hrets.append(r["hold_ret"])
        L.append(f"| {sig_date} | {name} | {limit} | {r['fill_date']}@{r['fill_px']:.2f} "
                 f"| {r['strat_ret']:+.2%} | {r['hold_ret']:+.2%} | {r['mdd']:+.2%} "
                 f"| {r['last_px']:.2f}({r['last_d']}) |")
        details.append(f"**{name}** 动作日志：" + " → ".join(r["log"]))
    if srets:
        L += ["", f"**汇总（成交 {len(srets)} 笔）**：跟随策略 胜率 {sum(1 for x in srets if x > 0)}/{len(srets)}"
              f" · 平均 {np.mean(srets):+.2%}｜持有不动 胜率 {sum(1 for x in hrets if x > 0)}/{len(hrets)}"
              f" · 平均 {np.mean(hrets):+.2%}"]
    L += ["", "### 动作明细", *[f"- {d}" for d in details],
          "", "> 口径：信号后3交易日内收盘≤限价按收盘成交（单笔全额，生产为半仓+补半仓的简化）；"
          "离场=生产 risk.py（硬止损-8%/P10破MA20缓冲/盈利保护15-8-12/盈利后梯子）逐日回放，收盘价成交；"
          "0707 重挂为 0706 同一信号窗口的延续不重复计；严格买点闸（回踩/突破）历史 0 触发，"
          "故全部信号均来自「A/B级研报免闸半仓直入」规则。"]
    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
