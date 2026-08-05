# -*- coding: utf-8 -*-
"""把四腿宽基策略的全窗回测导出成前端可直接吃的 JSON（invest-journey「宽基指数」板块）。

**为什么是静态 JSON 而不是接口**：历史回测要用仓内 2005 起的静态基底 CSV（库里 index_daily
只有 2015 起），FC（Node）跑不了这套 Python 引擎；而历史是**不变量**——同一份闸位口径下，
昨天的买卖点不会变。所以历史走静态产物，**当日状态走 `/invest/broad`**（库表 broad_leg_state，
与每日计划 hints 同源）。两者在页面上分区标注，不混。

口径与生产完全同源：闸位取 `invest_model/broad_gates.py`，引擎复用 `long_window_backtest.run`。
只读 results/*.csv，不落库、不联网。

用法：
  python scripts/analysis/broad_export_web.py --out ../invest-journey/public/data/broadIndex.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))
from invest_model.broad_gates import BUY_MUL, SELL_MUL  # noqa: E402
from long_window_backtest import LEGS, first_tradable, prep, run  # noqa: E402
from bias_low_rank import low_episodes  # noqa: E402

ETF = {"沪深300": "510300", "创业板": "159915", "科创50": "588000", "红利": "515080"}
STRIDE = 5          # 序列抽稀步长（交易日）——19.5 年日频 ≈4700 点，图上看不出差别，体积 /5


def _thin(n: int, keep: set[int]) -> list[int]:
    """等距抽稀，但强制保留 keep 里的下标（买卖点所在日必须在轴上）与首尾。"""
    idx = set(range(0, n, STRIDE)) | keep | {0, n - 1}
    return sorted(idx)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    ap.add_argument("--out", default="results/broad_web.json")
    a = ap.parse_args()
    root = Path(a.data)

    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))
    data = {nm: prep(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}
    mode = {nm: m for nm, _, _, _, _, m in LEGS}

    legs = []
    for nm, (df, ret) in data.items():
        st = first_tradable(df, mode[nm], None)
        r = run(df, ret, fmap, nm, st, None, mode[nm])
        d = df.trade_date.tolist()
        pos_i = int(np.searchsorted(np.array(d), r["dates"][0]))        # 仓位/净值序列的起点
        tset = {int(np.searchsorted(np.array(d), t["date"])) for t in r["trades"]}
        keep = _thin(len(d), tset)

        # 买入持有净值（与策略同起点、同长度）——图上作虚线对照
        _s = (ret if ret is not None else df.c).ffill()
        _i0 = int(np.searchsorted(df.trade_date.values, r["dates"][0]))
        _bh = _s.iloc[_i0:_i0 + len(r["dates"])].to_numpy(dtype=float)
        bh_c = _bh / _bh[0]

        # 乖离率（P39/E37）——**只作波动刻度展示，不参与任何闸判定**。
        # E37 2026-08-02 首跑已判死它作方向信号（高尾进前 5% 后未来 20 日反而
        # +0.53~+5.70pp），唯一残留是破极值后 60 日最大回撤明显高于常态。
        bias_s = (df.c / df.c.rolling(60).mean() - 1.0)
        bv = bias_s.dropna()

        # 乖离率的**极值排名谱**（owner 2026-08-05：「我要看的是历史极值排第几」）。
        # 两个排名并列，因为它们回答的不是同一个问题：
        #   · 逐日排名 = 因果、可交易（截至当日见过的第几低）
        #   · episode 排名 = 事后、只可描述（把同一轮连续深跌折叠成一个代表点；
        #     "谁是第 1 名"要看完全部历史才知道，不可写进规则）
        _bv = bias_s.dropna()
        _i0e = int(np.searchsorted(df.trade_date.values, r["dates"][0]))
        _eps = low_episodes(bias_s.to_numpy(dtype=float), df.trade_date.values, _i0e)
        _cur = float(_bv.iloc[-1])
        bias_rank_day = int((_bv < _cur).sum()) + 1
        bias_rank_ep = sum(1 for e in _eps if e["bias"] < _cur) + 1
        bias_spectrum = [{"rank": k, "date": e["date"], "bias": round(e["bias"], 4)}
                         for k, e in enumerate(_eps[:8], 1)]

        cum_s = float(r["curve"][-1] / r["curve"][0])
        cum_b = float((1 + r["bh"]) ** r["yrs"])
        legs.append({
            "name": nm, "etf": ETF[nm], "mode": mode[nm],
            "buy_mul": BUY_MUL[nm], "sell_mul": SELL_MUL[nm],
            "start": r["dates"][0], "end": r["dates"][-1], "years": round(float(r["yrs"]), 2),
            "ann": round(float(r["ann"]), 6), "bh_ann": round(float(r["bh"]), 6),
            "cum": round(cum_s, 4), "bh_cum": round(cum_b, 4),
            "sharpe": round(float(r["sharpe"]), 3),
            "mdd": round(float(r["mdd"]), 4), "bh_mdd": round(float(r["bhmdd"]), 4),
            "n_buy": int(r["nb"]), "n_sell": int(r["ns"]), "pos_avg": round(float(r["posavg"]), 4),
            # 价格轴（抽稀）：收盘 / 中位线锚 / 买入线 / 卖出线
            "dates": [d[i] for i in keep],
            "close": [round(float(df.c.iloc[i]), 2) for i in keep],
            "anchor": [None if pd.isna(df.exp.iloc[i]) else round(float(df.exp.iloc[i]), 2)
                       for i in keep],
            "buy_line": ([None if pd.isna(df.exp.iloc[i]) else
                          round(float(df.exp.iloc[i]) * BUY_MUL[nm], 2) for i in keep]
                         if mode[nm] != "ladder" else
                         [round(float(df.peak.iloc[i]) * 0.50, 2) for i in keep]),
            "sell_line": [None if pd.isna(df.exp.iloc[i]) else
                          round(float(df.exp.iloc[i]) * SELL_MUL[nm], 2) for i in keep],
            # 仓位与净值（回测口径，起点＝该腿首个可交易日）
            "pos_dates": [r["dates"][i] for i in range(0, len(r["dates"]), STRIDE)],
            "pos": [round(float(r["pos_series"][i]), 4)
                    for i in range(0, len(r["dates"]), STRIDE)],
            "nav": [round(float(r["curve"][i] / r["curve"][0]), 4)
                    for i in range(0, len(r["dates"]), STRIDE)],
            "bh_nav": [round(float(bh_c[i]), 4)
                       for i in range(0, len(r["dates"]), STRIDE)],
            "bias": [None if pd.isna(bias_s.iloc[i]) else round(float(bias_s.iloc[i]), 4)
                     for i in keep],
            "bias_p05": round(float(bv.quantile(0.05)), 4),
            "bias_p95": round(float(bv.quantile(0.95)), 4),
            "bias_min": round(float(bv.min()), 4),
            "bias_max": round(float(bv.max()), 4),
            "bias_last": round(float(bv.iloc[-1]), 4),
            "bias_last_pct": round(float((bv <= bv.iloc[-1]).mean()), 4),
            "bias_rank_day": bias_rank_day,
            "bias_n_day": int(len(bv)),
            "bias_rank_ep": bias_rank_ep,
            "bias_spectrum": bias_spectrum,
            "trades": [{"date": t["date"], "side": t["side"], "why": t["why"],
                        "price": round(float(t["price"]), 2),
                        "amount": round(float(t["amount"]), 3),
                        "frac": round(float(t["frac"]), 4)} for t in r["trades"]],
        })
        _ = pos_i

    fd = fear.sort_values("trade_date")
    fear_out = fd.iloc[::STRIDE]
    payload = {
        "generated_at": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S%z"),
        "gates": {"buy_mul": BUY_MUL, "sell_mul": SELL_MUL},
        "legs": legs,
        "fear": {"dates": fear_out.trade_date.tolist(),
                 "score": [round(float(v), 1) for v in pd.to_numeric(fear_out.score)]},
        # 页面上必须常驻的三条免责，跟数字一起走，避免前端文案漂移
        # 乖离率的裁决随数据一起走——网站上凡出现这个数的地方都必须带着它，
        # 否则一个已被证伪的信号会因为"被画在图上"而重新显得权威。
        "bias_verdict": {
            "tested": "高尾 P39/E37（2026-08-02）· 低尾 P65/E56（2026-08-05）——两个尾部都已判死",
            "high_tail": "❌ 已判死作方向信号：乖离率进入全历史前 5% 分位后，未来 20 日收益"
                         "不是更差而是更好——沪深300 +5.70pp、创业板 +3.94pp、红利 +4.97pp、"
                         "科创50 +0.53pp（判据要求 ≤−2.0pp，四腿 0/4 达标）；破历史极值后"
                         "60 日内价格回到 MA60 下方的比例只有 42%/39%/0%/28%（判据 ≥80%）。"
                         "这是顶部机械信号第四次失败（P16/P19/E24/E37）。",
            "residual": "✅ 唯一未被否定的残留：破历史极值后 60 日的最大回撤为 −11.5%~−21.2%，"
                        "明显高于常态 ⟹ 可作波动/回撤刻度，不能作方向信号。",
            "low_tail": "❌ 低尾（跌得太深）也已判死：E56（2026-08-05 首跑）四条判据全不过。"
                        "失败机制不是「低尾不灵」，而是低尾与 B2 腿的既有价格闸几乎不相交——"
                        "乖离率低尾是相对 60 日均线的短期偏离（牛市回调也点亮），"
                        "而 B2 要求收盘低于近 5 年中位数（长期估值位置）；X=5% 时四腿的 "
                        "10~145 个低尾日里，同时满足价格闸的只有 0~13 天。"
                        "强行拆掉价格闸后年化 −0.7~−3.1pp、回撤最多恶化 −20.5pp。",
            "rank": "owner 问的「历史极值排第几」已按博主原口径（排名，不是分位）复测，"
                    "换排名口径实测仍不改判：因果排名前 3/5/10 的触发日里，"
                    "与 B2 既有价格闸「收盘<近 5 年中位数」的共现四腿全为 0，"
                    "接进 B2 后四腿 Δ年化全是 +0.00。且创业板的因果排名低尾有 77% 与"
                    "恐慌≥75 是同一批日子＝判据②说的「恐慌的影子」。"
                    "⚠️ 事后 episode 谱上的前瞻收益（后 20 日 +10.0%、18/19 为正）看着很好，"
                    "但那是事后选点——当天你并不知道自己排第几；可交易的因果版弱得多"
                    "（沪深300 −0.1%、红利 +3.9%），且前五 episode 跨腿高度重叠"
                    "（2015-08 三腿、2016-01 三腿），有效独立事件远少于 20。",
            "wiring": "本页只显示，不接任何买卖闸。四腿的买卖判定完全由中位线锚决定。",
        },
        "caveats": [
            "可得性偏差：515080（中证红利ETF）2019 年才有，19.5 年窗口里约 66% 的时间这条腿"
            "无法按回测执行；科创50 的起点 20191231 是回溯基日（指数 2020-07 才发布）。"
            "因此这些超额数字没有可对外的可执行组合口径。",
            "预热参数是针尖：中位线锚的预热 WARM=500 个交易日，改成 650 会让红利与创业板"
            "两条腿的超额直接变号。过拟合风险不在买卖闸上，在这个参数上。",
            "统计不显著：四腿 bootstrap p=0.35~0.69，有效独立周期只有 2.8~3.9 个。"
            "这套规则可以当纪律用（约束高位不买、崩盘敢买），不能当收益预测用。",
        ],
    }
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                   encoding="utf-8")
    kb = out.stat().st_size / 1024
    print(f"saved {out}  ({kb:.0f} KB)")
    for lg in legs:
        print(f"  {lg['name']:>7s} {lg['start']}~{lg['end']} {lg['years']:>5.1f}y "
              f"累计 {lg['cum']:.2f}x vs {lg['bh_cum']:.2f}x  年化 {lg['ann']:.2%} vs "
              f"{lg['bh_ann']:.2%}  买{lg['n_buy']}/卖{lg['n_sell']}  均仓 {lg['pos_avg']:.0%}")


if __name__ == "__main__":
    main()
