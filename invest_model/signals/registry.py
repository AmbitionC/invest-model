"""信号生成器注册中心。

所有 `CategorizedSignalGenerator` 子类都应通过 `@register("name")` 装饰器
注册到全局 `REGISTRY`，`CompositeScorer` 只依赖本 registry 驱动，
从而在不改动 scorer 的前提下扩展/禁用某类信号。
"""

from __future__ import annotations

from typing import Callable, Type

from invest_model.signals.base import CategorizedSignalGenerator

__all__ = [
    "REGISTRY",
    "register",
    "get_all_generators",
    "get_by_category",
    "get_generator",
    "ensure_default_generators_loaded",
]


REGISTRY: dict[str, Type[CategorizedSignalGenerator]] = {}


def register(name: str) -> Callable[[Type[CategorizedSignalGenerator]], Type[CategorizedSignalGenerator]]:
    """类装饰器：把一个 Generator 登记到 REGISTRY。"""

    def deco(cls: Type[CategorizedSignalGenerator]) -> Type[CategorizedSignalGenerator]:
        if not issubclass(cls, CategorizedSignalGenerator):
            raise TypeError(
                f"register 只能装饰 CategorizedSignalGenerator 的子类, 实际: {cls}"
            )
        REGISTRY[name] = cls
        return cls

    return deco


def get_all_generators() -> list[CategorizedSignalGenerator]:
    """实例化并返回全部已注册的 generator。"""
    ensure_default_generators_loaded()
    return [cls() for cls in REGISTRY.values()]


def get_by_category(category: str) -> list[CategorizedSignalGenerator]:
    """按类别筛选 generator 实例。"""
    ensure_default_generators_loaded()
    return [cls() for cls in REGISTRY.values() if cls.category == category]


def get_generator(name: str) -> CategorizedSignalGenerator | None:
    """按注册名拿单个实例。"""
    ensure_default_generators_loaded()
    cls = REGISTRY.get(name)
    return cls() if cls else None


_DEFAULTS_LOADED = False


def ensure_default_generators_loaded() -> None:
    """懒加载默认 generator 模块，触发它们的 @register 副作用。

    使用懒加载避免循环依赖，并允许调用方根据需要只加载部分类别。
    """
    global _DEFAULTS_LOADED
    if _DEFAULTS_LOADED:
        return

    # 导入顺序：先 technical 的适配器，再基本面/资金流/情绪
    from invest_model.signals import technical_adapter  # noqa: F401
    from invest_model.signals import fundamental  # noqa: F401
    from invest_model.signals import money_flow  # noqa: F401
    from invest_model.signals import sentiment  # noqa: F401

    _DEFAULTS_LOADED = True
