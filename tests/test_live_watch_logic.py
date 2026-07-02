"""盯盘推送策略纯函数测试：摘要缓冲 flush 决策 + 自适应轮询间隔。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.live_check import _next_interval, _should_flush


def test_flush_crit_immediate():
    """卖出类风控（crit）出现 → 无视窗口立即推送。"""
    assert _should_flush(True, 0, 0.0, 1200, 600)
    assert _should_flush(True, 3, 10.0, 1200, 600)


def test_flush_batch_waits_for_window():
    """买点类进缓冲：未到窗口不推，到窗口才合并推。"""
    assert not _should_flush(False, 2, 300.0, 1200, 600)
    assert _should_flush(False, 2, 1200.0, 1200, 600)


def test_flush_empty_pending_never():
    assert not _should_flush(False, 0, 9999.0, 1200, 600)


def test_flush_forced_near_close():
    """14:45（hm=885）后强制清空缓冲，当日机会不过夜。"""
    assert _should_flush(False, 1, 0.0, 1200, 885)
    assert not _should_flush(False, 1, 0.0, 1200, 884)


def test_flush_zero_window_means_immediate():
    """--digest-window 0 → 一有缓冲即推（等价旧的全部立即推行为）。"""
    assert _should_flush(False, 1, 0.0, 0, 600)


def test_adaptive_interval_ladder():
    """距触发线 <1% 提频、<3% 常态、更远降频。"""
    assert _next_interval(0.005, 60, 20, 180) == 20
    assert _next_interval(0.02, 60, 20, 180) == 60
    assert _next_interval(0.10, 60, 20, 180) == 180
    assert _next_interval(float("inf"), 60, 20, 180) == 180  # 无任何可盯目标 → 最低频


def test_once_flush_batch_wall_clock_boundary():
    """once 模式（FaaS）：batch 只在每 digest 分钟边界后的首个 tick 推送。"""
    from scripts.live_check import _once_flush_batch
    assert _once_flush_batch(600, 20, 3)        # 10:00，600%20=0 → 边界 tick，推
    assert _once_flush_batch(602, 20, 3)        # 10:02，602%20=2<3 → 仍在边界窗内
    assert not _once_flush_batch(610, 20, 3)    # 10:10，10>=3 → 不推（等下个边界）
    assert _once_flush_batch(886, 20, 3)        # 14:46 尾盘 → 强制推
    assert _once_flush_batch(610, 0, 3)         # 窗口 0 → 每次都推
