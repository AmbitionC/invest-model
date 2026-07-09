"""S1/S10 回归：review_advisor 的入场价口径与同票去重（纯函数 _advisor_rows）。

- 入场价=首推日**之后**首个收盘（原 >= 用当日收盘：盘中/收盘后录入的信号不可成交，
  系统性高估战绩；与 build_signal_scorecard 的严格次日口径对齐）；
- 同票多次推荐（哪怕分级不同）只记首评（原 GROUP BY code,grade 会重复进多个分级桶）；
- 刚推荐、尚无次日行情的票不计入。
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.review import _advisor_rows  # noqa: E402


def _entry_win() -> pd.DataFrame:
    # AAA：0701 收盘 10.0、0702 收盘 11.0、0703 收盘 12.0
    return pd.DataFrame([
        {"code": "AAA.SZ", "trade_date": "20260701", "close": 10.0},
        {"code": "AAA.SZ", "trade_date": "20260702", "close": 11.0},
        {"code": "AAA.SZ", "trade_date": "20260703", "close": 12.0},
    ])


def test_entry_is_next_close_not_same_day():
    reco = pd.DataFrame([{"code": "AAA.SZ", "grade": "A", "rec_date": "20260701"}])
    df = _advisor_rows(reco, _entry_win())
    assert len(df) == 1
    # 入场 = 0702 收盘 11.0（非当日 10.0），现价 12.0 → +9.09%；旧口径会给 +20%
    assert abs(df["ret"].iloc[0] - (12.0 / 11.0 - 1.0)) < 1e-9


def test_same_code_multi_grade_counted_once():
    reco = pd.DataFrame([
        {"code": "AAA.SZ", "grade": "B", "rec_date": "20260701"},   # 首评 B
        {"code": "AAA.SZ", "grade": "A", "rec_date": "20260702"},   # 升级重复推荐
    ])
    df = _advisor_rows(reco, _entry_win())
    assert len(df) == 1
    assert df["grade"].iloc[0] == "B"          # 记首评
    assert df["first"].iloc[0] == "20260701"


def test_fresh_reco_without_next_close_skipped():
    # 推荐日=行情最后一日 → 无次日收盘，不计入（原口径会用当日收盘记 0% 干扰胜率）
    reco = pd.DataFrame([{"code": "AAA.SZ", "grade": "A", "rec_date": "20260703"}])
    df = _advisor_rows(reco, _entry_win())
    assert df.empty
