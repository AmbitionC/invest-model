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
from bias_rank_extremes import extreme_episodes  # noqa: E402

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
        _bnp = bias_s.to_numpy(dtype=float)
        _cur = float(_bv.iloc[-1])
        # 🔴 2026-08-05 修正：此前用策略起点（＝expanding 锚的 WARM=500 预热日）当谱的起点，
        # 但那是**锚**的预热，乖离率只需要 60 个交易日 ⟹ 每条腿的极值谱都被砍掉了前 ~499 个
        # 可算日。沪深300 原谱首位 20070129 +35.74% 其实不是全历史最高（被砍段里 20070122
        # 有 +37.64%），红利同理。改为 bias 首个可算日，全腿同一口径。
        _i0e = int(np.argmax(~np.isnan(_bnp)))
        _epL = extreme_episodes(_bnp, df.trade_date.values, _i0e, "low")
        _epH = extreme_episodes(_bnp, df.trade_date.values, _i0e, "high")
        bias_rank_day = int((_bv < _cur).sum()) + 1                    # 低尾：第几低
        bias_rank_day_high = int((_bv > _cur).sum()) + 1               # 高尾：第几高
        bias_rank_ep = sum(1 for e in _epL if e["bias"] < _cur) + 1
        bias_rank_ep_high = sum(1 for e in _epH if e["bias"] > _cur) + 1
        bias_spectrum = [{"rank": k, "date": e["date"], "bias": round(e["bias"], 4)}
                         for k, e in enumerate(_epL[:8], 1)]
        bias_spectrum_high = [{"rank": k, "date": e["date"], "bias": round(e["bias"], 4)}
                              for k, e in enumerate(_epH[:8], 1)]

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
            "bias_rank_day_high": bias_rank_day_high,
            "bias_rank_ep_high": bias_rank_ep_high,
            "bias_spectrum_high": bias_spectrum_high,
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
            "tested": "高尾 P39/E37（2026-08-02）· 低尾 P65/E56 · 排名口径两尾补齐 · 双尾前三腿 "
                      "P67/E57 · 止盈止损 sleeve P68/E58 · 近十年滚动窗口高卖低买 P69/E59"
                      "（均 2026-08-05）——五个口径的 E 验证全部 FAIL",
            "high_tail": "❌ 作方向信号已判死（E37）：乖离率进入全历史前 5% 分位后，未来 20 日"
                         "收益不是更差而是更好——沪深300 +5.70pp、创业板 +3.94pp、红利 +4.97pp、"
                         "科创50 +0.53pp（判据要求 ≤−2.0pp，四腿 0/4）；「超过此前历史最大值」后"
                         "60 日内回到 MA60 下方的比例只有 42%/39%/0%/28%（判据 ≥80%）。",
            "residual": "✅ 唯一未被否定的残留：破历史极值后 60 日的最大回撤为 −11.5%~−21.2%，"
                        "明显高于常态 ⟹ 可作波动/回撤刻度，不能作方向信号。",
            "low_tail": "❌ 低尾（跌得太深）也已判死：E56 四条判据全不过。失败机制不是「低尾不灵」，"
                        "而是低尾与 B2 腿的既有价格闸几乎不相交——乖离率低尾是相对 60 日均线的"
                        "短期偏离（牛市回调也点亮），而 B2 要求收盘低于近 5 年中位数（长期估值位置）；"
                        "X=5% 时四腿的 10~145 个低尾日里，同时满足价格闸的只有 0~13 天。",
            "rank": "owner 追问「历史极值排第几、极值包不包括极大和极小」后，两个尾部都按博主原口径"
                    "（排名，不是分位）补测。结论：事后谱与可交易口径给出相反的答案，差别全在事后视角。"
                    "① 高尾·事后 episode 谱前五（n=20）：后 20 日 −6.6%、仅 5/20 为正，"
                    "60 日内回到 MA60 下方 16/20＝80% —— 博主的命题在这一档基本成立。"
                    "② 高尾·因果排名（当日可知的口径）：创业板 K=5 有 48 个触发日，后 20 日反而 +5.6%、"
                    "35/48 为正、回落 MA60 下方只有 38%；沪深300 与红利在 K≤5 几乎无触发日"
                    "（它们的历史最高偏离出现在 2007 年、预热期内）。"
                    "③ 低尾·因果排名 K=3/5/10：与 B2 价格闸的共现四腿全为 0，接进 B2 后 Δ年化全 +0.00。"
                    "⟹ 两个尾部的裁决都不因换排名口径而改变；高尾那个 80% 只在事后成立，"
                    "而当天你并不知道自己是不是第 1 名。",
            "top3": "owner 命题「所有指数偏离度进前三＝强信号、值得中短线操作」已按完整腿"
                    "（进场+持有+退出）在 7 个指数上验完（P67/E57，2026-08-05，"
                    "含上证50/中证500/中证1000 三条从未参与调参的样本外对照）——双腿均 FAIL。"
                    "✅ 有利的一半：跌到前三后 20 日确有反弹，效应量 5/7 达标"
                    "（创业板 +3.6pp、科创50 +11.4pp、红利 +4.1pp、中证500 +3.7pp、中证1000 +3.4pp）。"
                    "🔴 但 60 日完全反转——沪深300 −13.1pp、上证50 −13.1pp、中证500 −11.0pp、"
                    "红利 −10.5pp，7 个里 5 个把 20 日赚的全吐回去还倒亏 ⟹ 不带退出腿就是负的。"
                    "🔴 而且它十年不响一次：104 个触发日按自然月去重只剩 14 个事件，几乎全在 "
                    "2008 与 2015 两场危机，2015 年后 7 个指数总共只触发过 1 次（2022-04 科创50）"
                    "⟹ 合并置换检验 p=0.138 不显著。做成净值后夏普 7/7 下降（−0.04~−0.60）。",
            "top3_high": "涨到前三那一头方向就是错的，而且比首轮报告的更彻底。首轮我说「只有中证500"
                         "与中证1000 方向对」——那两个在主口径下分别只有 6 个和 2 个触发日，"
                         "把排名预热从 500 放宽到 120/250 后双双翻号（−8.85%→+5.34%、"
                         "−12.33%→+15.84%）⟹ 那两个负号是极小样本噪声，已撤回。"
                         "放宽预热后 7 个指数全部为正（+3.1% ~ +15.8%）：涨到前三之后是继续涨，"
                         "清仓是错的。「价格类极值在 A 股是动量不是均值回归」第五次独立复现。",
            "tp_sl": "owner 追问「带好止盈止损，胜率是不是挺好」已验完（P68/E58，2026-08-05）——"
                     "胜率确实好，但那个好是用砍掉右尾换来的。18 格止盈/止损网格全为正、"
                     "含成本每笔中位 +3.74%、胜率中位 60%、盈亏比 1.41，三条从未调参的对照指数"
                     "同样 18/18 全为正 —— 这是乖离率第一次在整片网格 + 样本外同时站住。"
                     "🔴 但相对「固定持有 20 日」反而更差，只有 1/7 达标（沪深300 +4.3% vs +5.0%、"
                     "创业板 +3.9% vs +7.0%、科创50 +10.1% vs +14.4%）。看构成就清楚：TP=5% 的三格是"
                     "30 次止盈 / 19 次止损 / 0 次到期、平均只持有 4~5 天——止盈线比信号本身的"
                     "时间尺度还短，把一个 20 日级别的中期反弹压成了几天的短打。"
                     "落地否决项：做成 10% 仓位的 sleeve，十九年出手约 8 次、年化贡献中位仅 +0.209%，"
                     "比闲钱放货基还少一个数量级。⟹ 不接入生产、不配资金。",
            "rolling10y": "owner 指定的最后一个口径——「近十年滚动窗口前四，高的卖、低的买，"
                          "单纯算这个策略本身」（P69/E59，2026-08-05）——同样 FAIL，七腿超额 "
                          "−5.35~−7.83pp、判据 ①0/7 ②0/3 ③0/7 ⑤3/7，满仓起手对照臂也是 0/7。"
                          "立项理由是我此前的口径错误：博主原文「近十年历史上排名第五」是滚动窗口，"
                          "而我一直用的是全历史排名（会在 2008/2015 后被永久锁死）。"
                          "🔴 失败机制是新的：低尾阈值由「过去十年的平静」定，所以它在崩盘的第一条腿上"
                          "就点亮；高尾阈值被上一轮泡沫锁死，要等下一轮泡沫才解锁。 于是每笔都是"
                          "「崩盘开头买入、多年后反弹里卖出」——上证50 2008-01-23 买 → 2015-01-06 卖 "
                          "−31.3%（持有 6.8 年）、创业板 2015-07-07 → 2024-10-09 −3.1%（持有 9.0 年）。"
                          "沪深300 更极端：2007-11-22 首次点亮时乖离率才 −11.4%，买进去后再没出现过一次"
                          "卖出信号（高尾阈被 2007-01 的 +35.7% 锁到 2017 年），一路持有至今。"
                          "🔴 附带发现：这条规则度量的不是「便宜」——同一条「近十年前四低」，沪深300 "
                          "2007 年 −11.4% 就算、2015 年要 −28.8% 才算，中证红利 2026-06-30 的 −10.7% "
                          "又算了。阈值随最近十年的波动幅度漂移，与本系统的价值锚（expanding 中位线）"
                          "是两种不兼容的尺度。",
            "wiring": "乖离率至此已在五个不同口径上全部判死（分位高尾 E37 · 分位低尾 E56 · "
                      "全历史排名双尾 E57 · 排名+止盈止损 E58 · 近十年滚动排名双尾状态机 E59），"
                      "五次的失败机制各不相同。本页只显示，不接任何买卖闸——"
                      "四腿的买卖判定完全由中位线锚决定。",
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
