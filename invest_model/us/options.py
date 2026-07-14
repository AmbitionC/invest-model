"""期权造血：现金担保 put（CSP）/ 备兑 call（CC）候选打分（纯函数，可离线测试）。

结构思想：可转债"下有债底、上有正股看涨期权"的镜像——以愿意持有的折扣价
承接优质资产、期间收取权利金现金流（资产自身造血）。分析性迁移，出处与
声明见 life-teachers insights/us-stock-investing-methodology.md 第一节。

铁律（US-O1~O4）：绝不裸卖——CSP 必须全额现金担保、CC 必须持有正股；
担保占用不超过造血 sleeve 预算；VIX 恐慌/破线时暂停新卖（signals.selling_puts_allowed）。
"""

from __future__ import annotations

import pandas as pd

from invest_model.us import config as C


def _mid(bid: float, ask: float) -> float:
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return max(bid, 0.0)


def score_csp(chain: pd.DataFrame, close: float, budget: float,
              quality_ok: bool = True, anchor: float | None = None,
              mode: str = "normal") -> pd.DataFrame:
    """从期权链快照筛现金担保 put 候选（V2：行权价锚定"心甘情愿接盘价"）。

    过滤（每条都可解释）：
      仅 put ｜ 安全边际 = 1-strike/close ≥ OPT_MIN_SAFETY
      **行权价 ≤ 估值锚 anchor**（全哥 Sell Put 底层逻辑：不是赌涨、不是贪权利金，
      是基于估值找出心甘情愿接盘的价格 K——锚现价折扣会随涨追高，锚估值不会）
      strict 模式（恐慌/破线）：行权价 ≤ anchor×0.9 ——情绪化最重=IV最厚=期权
      最好时机（全哥），但接货价必须便宜到极致（守本金收紧）
      担保 strike*100 ≤ budget（全额现金担保，绝不裸卖）
      OI ≥ OPT_MIN_OI ｜ bid>0 ｜ 年化 ∈ [下限, 上限]（高到离谱=市场在定价崩塌）
    排序：安全边际优先、年化次之（确定性>收益率）。
    """
    cols = ["code", "strategy", "expiry", "strike", "premium", "dte",
            "annualized_yield", "safety_margin", "collateral", "iv",
            "open_interest", "reason"]
    if chain is None or chain.empty or close <= 0 or budget <= 0 or not quality_ok:
        return pd.DataFrame(columns=cols)
    puts = chain[chain["strategy_side"] == "put"].copy()
    if puts.empty:
        return pd.DataFrame(columns=cols)
    # 行权价上限：现价折扣 与 估值锚 双重取低（strict 再打九折）
    strike_cap = close * (1 - C.OPT_MIN_SAFETY)
    if anchor is not None and anchor > 0:
        cap2 = anchor * (0.9 if mode == "strict" else 1.0)
        strike_cap = min(strike_cap, cap2)
    elif mode == "strict":
        return pd.DataFrame(columns=cols)   # 无估值锚的标的恐慌期不卖（不装数据）
    puts["premium"] = [_mid(b, a) for b, a in zip(puts["bid"], puts["ask"])]
    puts["safety_margin"] = 1 - puts["strike"] / close
    puts["collateral"] = puts["strike"] * 100
    puts["annualized_yield"] = (puts["premium"] / puts["strike"]) * (365 / puts["dte"])
    ok = puts[
        (puts["premium"] > 0)
        & (puts["strike"] <= strike_cap)
        & (puts["collateral"] <= budget)
        & (puts["open_interest"] >= C.OPT_MIN_OI)
        & (puts["annualized_yield"] >= C.OPT_MIN_ANNUAL_YIELD)
        & (puts["annualized_yield"] <= C.OPT_MAX_ANNUAL_YIELD)
    ].copy()
    if ok.empty:
        return pd.DataFrame(columns=cols)
    ok["strategy"] = "csp"
    anchor_note = (f"≤接盘锚${anchor:.0f}" if anchor else "无锚")
    mode_note = "·恐慌strict九折" if mode == "strict" else ""
    ok["reason"] = ok.apply(
        lambda r: (f"心甘情愿接盘价内（{anchor_note}{mode_note}）：现价${close:.2f}"
                   f"打{(1 - r['safety_margin']) * 100:.0f}折接货，权利金年化 "
                   f"{r['annualized_yield']:.0%}，担保 ${r['collateral']:,.0f}"
                   f"〔规则US-O1/V2〕"), axis=1)
    ok = ok.sort_values(["safety_margin", "annualized_yield"],
                        ascending=[False, False])
    return ok[cols].reset_index(drop=True)


def score_cc(chain: pd.DataFrame, close: float, cost_price: float,
             shares: float) -> pd.DataFrame:
    """备兑 call 候选（仅对 ≥100 股的持仓）：行权价 ≥ max(现价*1.03, 成本价)
    ——绝不让备兑锁死亏损卖出（回本纪律）。"""
    cols = ["code", "strategy", "expiry", "strike", "premium", "dte",
            "annualized_yield", "safety_margin", "collateral", "iv",
            "open_interest", "reason"]
    if chain is None or chain.empty or close <= 0 or shares < 100:
        return pd.DataFrame(columns=cols)
    calls = chain[chain["strategy_side"] == "call"].copy()
    if calls.empty:
        return pd.DataFrame(columns=cols)
    floor = max(close * 1.03, cost_price)
    calls["premium"] = [_mid(b, a) for b, a in zip(calls["bid"], calls["ask"])]
    calls["safety_margin"] = calls["strike"] / close - 1
    calls["collateral"] = 0.0            # 备兑：正股即担保
    calls["annualized_yield"] = (calls["premium"] / close) * (365 / calls["dte"])
    ok = calls[
        (calls["premium"] > 0)
        & (calls["strike"] >= floor)
        & (calls["open_interest"] >= C.OPT_MIN_OI)
        & (calls["annualized_yield"] >= C.OPT_MIN_ANNUAL_YIELD / 2)
    ].copy()
    if ok.empty:
        return pd.DataFrame(columns=cols)
    ok["strategy"] = "cc"
    ok["reason"] = ok.apply(
        lambda r: (f"持股备兑：行权价 ${r['strike']:.2f}≥成本${cost_price:.2f}，"
                   f"权利金年化 {r['annualized_yield']:.0%}（被行权=止盈非割肉）"
                   f"〔规则US-O2〕"), axis=1)
    ok = ok.sort_values("annualized_yield", ascending=False)
    return ok[cols].reset_index(drop=True)


def spans_earnings(expiry: str, next_earnings: str | None,
                   today: str | None = None) -> bool:
    """期权到期日是否跨越下一次财报日（纯函数，供 CSP 候选标注跳空风险）。

    跨财报卖 put：财报跳空使被行权概率/深度陡增而接盘价锚不变——不是禁止，
    是必须知情（DDOG/RKLB 研报把财报日当一级风险事件管理的移植）。
    """
    if not next_earnings or not expiry:
        return False
    e, x = str(next_earnings), str(expiry)
    lo = str(today) if today else ""
    return (e <= x) and (not lo or e >= lo)
