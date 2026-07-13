"""E13：负 Gamma × 临近到期 挤压风险探针（P17 预登记验证）——
   "临近到期 + VIX 骤升 + SPY 破 MA10" 这个**代理窗口**，是否真的让卖 CSP 系统性更差？

> 仅美股模块。若证不出显著恶化 → 代理无效 → 不上线（US-O5 保持只观察不生效）。

H0（零假设）：在上述代理窗口内卖现金担保 put（CSP），其被行权后的 20 日浮亏与回本率，
  相对非窗口卖同规格 CSP **没有系统性恶化** → 该代理无法识别负 Gamma 踩踏 → 不值得收紧。

出处：2026-07-13 投顾收盘笔记（做市商负 Gamma 越跌越卖死亡螺旋 + 临近到期 Gamma 放大 +
  杠杆 ETF 尾盘顺周期再平衡）。机制为通用期权做市学。

口径（诚实声明 · 代理法局限）：
  - **无真实 dealer Gamma 敞口（GEX）数据**——yfinance 拿不到稳定的全 strike 期权 OI。
    本验证用「临近到期 + VIX 骤升 + 破 MA10」**近似**负 Gamma 环境，代理与真 Gamma 的
    相关性本身未经证实，E13 正是检验这个代理是否有判别力。
  - CSP 被行权/浮亏用标的（SPY/QQQ）日线近似（行权价取接盘价锚或 ATM），非真实期权链回测。
  - 到期日历按规则推算（标准月度=第三个周五；周度=每周五），不依赖 OI 数据。

窗口定义（预登记 · 写死勿改）：距月度或周度到期 ≤ WINDOW_DAYS_TO_EXPIRY 交易日
  且（VIX 单日涨幅 ≥ VIX_SPIKE_PCT 或 VIX ≥ VIX_ABS_LEVEL）且 SPY < MA10。

过关判据（预登记 · 写死勿改 · 不看结果改判据）：
  ① 窗口样本数 ≥ MIN_WINDOW_SAMPLES；
  ② 窗口内被行权后 20 日平均浮亏，较非窗口 **恶化 ≥ WORSEN_DRAWDOWN_PP** 个百分点；
  ③ 窗口内"20 日转正/回本"占比，较非窗口 **低 ≥ WORSEN_RECOVER_PP** 个百分点。
  三条同时满足 → 代理有效、值得收紧（US-O5 达标）。样本 < MIN_WINDOW_SAMPLES → 不判定、不动。

用法：python scripts/validation/e13_gamma_squeeze.py [--db ...] [--out results/e13.md]
"""

from __future__ import annotations

# ── 预登记常量（判据先写死；晋升前逐字保持，不许看结果回调）──────────────
WINDOW_DAYS_TO_EXPIRY = 2      # 距到期 ≤2 交易日
VIX_SPIKE_PCT = 0.15          # VIX 单日涨幅 ≥ +15%
VIX_ABS_LEVEL = 35.0          # 或 VIX 绝对值 ≥ 35
HOLD_DAYS = 20                # 被行权后持有观察期（交易日）
MIN_WINDOW_SAMPLES = 20       # 窗口样本 <20 不判定
WORSEN_DRAWDOWN_PP = 5.0      # 窗口内 20 日平均浮亏较非窗口恶化 ≥5pp
WORSEN_RECOVER_PP = 10.0      # 窗口内 20 日回本率较非窗口低 ≥10pp
UNIVERSE = ("SPY", "QQQ")     # 代理标的


def main() -> None:
    raise NotImplementedError(
        "E13 预登记占位：判据（上方常量）已写死。实现前须先 bump ops/index-backfill "
        "或美股 us_daily 备好 SPY/QQQ/VIX 日线，再落地窗口标注 + CSP 近似回测。"
        "实现后达标且过「高置信直升」四条才进生产（US-O5 置 strict/pause）；否则维持现状。"
    )


if __name__ == "__main__":
    main()
