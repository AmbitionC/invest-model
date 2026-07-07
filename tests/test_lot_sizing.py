"""可执行手数回归：高价股/科创板的买入指令必须物理可执行（2026-07-07 用户实盘发现）。

实例：账户 55.4 万、目标增量 2.8%≈15,510 元——
  德明利 001309(851元/股)  一手 85,100 元(占15.9%) → 不可执行，须判 0；
  中际旭创 300308(1121.9)  一手 112,190 元 → 不可执行；
  海光信息 688041(342.6)   科创板最低 200 股=68,520 元 → 不可执行；
  沪电股份 002463(128.83)  120 股→向下 100 股 或补足一手(12,883≤1.2×增量) → 100。
"""
from invest_model.portfolio.sizing import buy_shares, min_lot

EQ = 553934.0
DELTA = 0.028 * EQ          # ≈15,510


def test_min_lot_by_board():
    assert min_lot("001309.SZ") == 100      # 深主板
    assert min_lot("300308.SZ") == 100      # 创业板
    assert min_lot("688041.SH") == 200      # 科创板
    assert min_lot("689009.SH") == 200      # 科创板 CDR


def test_high_price_mainboard_infeasible():
    assert buy_shares("001309.SZ", DELTA, 851.0) == 0.0       # 一手 85,100 ≫ 15,510
    assert buy_shares("300308.SZ", DELTA, 1121.9) == 0.0      # 一手 112,190


def test_star_market_min_200_infeasible():
    assert buy_shares("688041.SH", DELTA, 342.6) == 0.0       # 200股=68,520 ≫ 15,510×1.2


def test_star_market_feasible_one_share_increment():
    # 预算 10 万：100000/342.6=291.9 → 291 股（科创板 ≥200 后可 1 股递增）
    assert buy_shares("688041.SH", 100000.0, 342.6) == 291.0


def test_mainboard_floor_to_lot():
    assert buy_shares("002463.SZ", DELTA, 128.83) == 100.0    # 120.4 → 100
    assert buy_shares("000001.SZ", 8000.0, 10.0) == 800.0


def test_round_up_to_one_lot_within_tolerance():
    # 95 股不足一手，一手 1000 ≤ 950×1.2 → 补足 100
    assert buy_shares("000001.SZ", 950.0, 10.0) == 100.0
    # 一手 1000 > 700×1.2=840 → 不可执行
    assert buy_shares("000001.SZ", 700.0, 10.0) == 0.0


def test_degenerate_inputs():
    assert buy_shares("000001.SZ", 0.0, 10.0) == 0.0
    assert buy_shares("000001.SZ", 10000.0, 0.0) == 0.0
    assert buy_shares("000001.SZ", -1.0, 10.0) == 0.0
