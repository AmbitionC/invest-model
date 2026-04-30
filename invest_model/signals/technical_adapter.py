"""把 technical 包里的 TechnicalSignalGenerator 登记到 signals.registry。

做成独立文件有两个好处：
1. 避免 `technical/` 包反向依赖 `signals/`，保持依赖方向干净；
2. 让 `signals.registry.ensure_default_generators_loaded()` 统一控制加载时机。
"""

from __future__ import annotations

from invest_model.signals.registry import REGISTRY
from invest_model.technical.signal_interpreter import TechnicalSignalGenerator


# 技术信号类已在 signal_interpreter.py 中具备 category/scope/required_tables 属性，
# 这里仅完成注册。不是类装饰器，避免循环引用。
REGISTRY["technical"] = TechnicalSignalGenerator
