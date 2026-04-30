"""模型层入口 — 逐票信号顾问。

旧的 TopK 换仓回测已移除，改为逐票置信度信号顾问模式。

使用方式：
    from invest_model.advisor import StockAdvisor, AdvisorSignal

    advisor = StockAdvisor(engine)
    signals = advisor.advise_batch(codes, trade_date)
    for s in signals:
        print(s.action_cn, s.confidence, s.attribution)
"""

from invest_model.advisor import AdvisorSignal, StockAdvisor

__all__ = [
    "StockAdvisor",
    "AdvisorSignal",
]
