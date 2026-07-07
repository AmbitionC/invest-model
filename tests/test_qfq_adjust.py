"""P11 回归：股票收盘序列运行时前复权（除权除息缺口抹平，fail-open 保护）。"""
import pandas as pd

from invest_model.data.adjust import apply_qfq, qfq_close_hist


class _Repo:
    """桩：close_rows / adj_rows 为 (trade_date, value) 列表。"""

    def __init__(self, close_rows, adj_rows, has_adj=True):
        self.close_rows, self.adj_rows, self.has_adj = close_rows, adj_rows, has_adj

    def table_exists(self, t):
        return t != "stock_adj" or self.has_adj

    def read_sql(self, sql, params=None):
        if "stock_adj" in sql:
            return pd.DataFrame(self.adj_rows, columns=["trade_date", "adj_factor"])
        return pd.DataFrame(self.close_rows, columns=["trade_date", "close"])


def test_split_gap_flattened():
    # 10转10：价格 20→10 减半，因子 1→2。前复权后应为恒定 10，无假跳空。
    repo = _Repo([("D1", 20.0), ("D2", 20.0), ("D3", 10.0), ("D4", 10.0)],
                 [("D1", 1.0), ("D2", 1.0), ("D3", 2.0), ("D4", 2.0)])
    s = qfq_close_hist(repo, "000001.SZ", "D1", "D4")
    assert list(s.round(6)) == [10.0, 10.0, 10.0, 10.0]
    assert float(s.iloc[-1]) == 10.0          # 最新价 == 原始价


def test_dividend_gap_flattened():
    # 分红 2%：10→9.8，因子 1→1.0204。复权后近似恒定 9.8。
    repo = _Repo([("D1", 10.0), ("D2", 10.0), ("D3", 9.8)],
                 [("D1", 1.0), ("D2", 1.0), ("D3", 1.020408)])
    s = qfq_close_hist(repo, "000001.SZ", "D1", "D3")
    assert abs(float(s.iloc[0]) - 9.8) < 1e-3
    assert abs(float(s.iloc[1]) - 9.8) < 1e-3
    assert float(s.iloc[2]) == 9.8


def test_fail_open_no_table():
    repo = _Repo([("D1", 20.0), ("D2", 10.0)], [], has_adj=False)
    s = qfq_close_hist(repo, "000001.SZ", "D1", "D2")
    assert list(s) == [20.0, 10.0]            # 无因子表 → 原样


def test_fail_open_no_factor_rows():
    # ETF（行情已复权、不在 adj_factor 覆盖内）→ 无因子行 → 原样，不二次复权
    repo = _Repo([("D1", 1.32), ("D2", 1.347)], [])
    s = qfq_close_hist(repo, "588010.SH", "D1", "D2")
    assert list(s) == [1.32, 1.347]


def test_backfill_before_first_factor():
    # 窗口早于首个因子日：bfill 用首因子 → 等价假设窗口开头无除权，不引入新跳空
    repo = _Repo([("D1", 10.0), ("D2", 10.0), ("D3", 10.0)],
                 [("D2", 2.0), ("D3", 2.0)])
    s = qfq_close_hist(repo, "000001.SZ", "D1", "D3")
    assert list(s.round(6)) == [10.0, 10.0, 10.0]


def test_apply_qfq_on_prebuilt_series():
    s0 = pd.Series([20.0, 10.0], index=["D1", "D2"])
    repo = _Repo([], [("D1", 1.0), ("D2", 2.0)])
    s = apply_qfq(repo, "000001.SZ", s0, "D1", "D2")
    assert list(s.round(6)) == [10.0, 10.0]
