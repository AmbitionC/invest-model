"""盘中恐慌 fear_intraday 的回归测试。

覆盖：
  - 盘中值与"把同样的价格当收盘"的日频值口径一致（同一套分量公式）
  - 拉取不足（有效现价 < min_stocks）→ 返回 None（降级不写脏数据）
  - persist_fear_intraday 落库 fear_intraday 且不触碰 fear_daily
  - get_realtime_market 批次解析（注入腾讯响应文本，不走网络）
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.fear import fear_gauge, fear_intraday  # noqa: E402


def _seed(tmp_path, n_codes=60, n_days=28):
    eng = make_engine(f"sqlite:///{tmp_path}/t.db")
    create_schema(eng)
    repo = BaseRepository(eng)
    dates = [f"202605{d:02d}" for d in range(1, n_days + 1)]
    rng = np.random.default_rng(5)
    srows = []
    for code in [f"{i:06d}.SZ" for i in range(n_codes)]:
        px = 10.0
        for dt in dates:
            chg = float(rng.normal(0, 2))
            px = max(1.0, px * (1 + chg / 100))
            srows.append({"code": code, "trade_date": dt, "open": px, "high": px,
                          "low": px, "close": round(px, 3), "pct_chg": round(chg, 4),
                          "volume": 1e5, "amount": 1e6})
    repo.upsert("stock_daily", pd.DataFrame(srows), ["code", "trade_date"])
    ipx, irows = 4000.0, []
    for dt in dates:
        ipx *= 1 + float(rng.normal(0, 0.5)) / 100
        irows.append({"code": "000300.SH", "trade_date": dt, "open": ipx, "high": ipx,
                      "low": ipx, "close": round(ipx, 3), "pct_chg": 0.1,
                      "volume": 1e8, "amount": 1e9})
    repo.upsert("index_daily", pd.DataFrame(irows), ["code", "trade_date"])
    return eng, repo, dates


def test_intraday_matches_daily_when_prices_equal_close(tmp_path):
    """把「最后一天的收盘价」当作盘中现价喂进去，盘中值必须 == 该日日频值。"""
    eng, repo, dates = _seed(tmp_path)
    last = dates[-1]
    # 去掉最后一天，模拟盘中（stock_daily 只到昨日）；price_map 用被删那天的收盘价
    last_rows = repo.read_sql(
        "SELECT code, close, pct_chg FROM stock_daily WHERE trade_date=:d", {"d": last})
    idx_last = repo.read_sql(
        "SELECT close FROM index_daily WHERE code='000300.SH' AND trade_date=:d",
        {"d": last})["close"].iloc[0]
    daily = fear_gauge(eng, last)                          # 含 last 的日频官方值

    repo.execute_sql("DELETE FROM stock_daily WHERE trade_date=:d", {"d": last})
    repo.execute_sql("DELETE FROM index_daily WHERE trade_date=:d", {"d": last})
    price_map = {r["code"]: {"price": float(r["close"]),
                             "pre_close": float(r["close"]) / (1 + r["pct_chg"] / 100)}
                 for _, r in last_rows.iterrows()}
    intra = fear_intraday(eng, price_map, float(idx_last), last, min_stocks=10)
    assert intra is not None and intra.get("intraday") is True
    assert abs(intra["score"] - daily["score"]) < 1e-6
    assert intra["components"] == daily["components"]


def test_degrade_when_too_few_prices(tmp_path):
    eng, _, dates = _seed(tmp_path)
    price_map = {"000000.SZ": {"price": 10.0, "pre_close": 9.9}}   # 只有 1 只
    assert fear_intraday(eng, price_map, 4000.0, dates[-1], min_stocks=2000) is None


def test_persist_intraday_does_not_touch_daily(tmp_path):
    eng, repo, dates = _seed(tmp_path)
    from scripts.fear_gauge import persist_fear_intraday
    g = {"date": dates[-1], "score": 62.5, "level": "偏恐慌",
         "components": {"动量": 60}, "raw": {"n": 100}}
    persist_fear_intraday(eng, g, "2026-07-13 14:00:00")
    intra = repo.read_sql("SELECT * FROM fear_intraday")
    assert len(intra) == 1 and float(intra["score"].iloc[0]) == 62.5
    assert repo.read_sql("SELECT COUNT(*) c FROM fear_daily")["c"].iloc[0] == 0


def test_get_realtime_market_parses_batches(monkeypatch):
    import requests
    from invest_model.signals import realtime

    class _Resp:
        def __init__(self, text):
            self.text = text
            self.encoding = "gbk"

    def fake_get(url, timeout=15):
        # 腾讯格式：v_sz000001="1~名~000001~现价~昨收~今开~量~…~最高~最低~…";
        body = 'v_sz000001="1~甲~000001~11.50~11.00~11.1~1e5~~~~~~~~~~~~~~~~~~~~~~~~~~~~~12.0~10.8";\n'
        return _Resp(body)

    monkeypatch.setattr(requests, "get", fake_get)
    out = realtime.get_realtime_market(["000001.SZ"], chunk=60)
    assert out["000001.SZ"]["price"] == 11.5 and out["000001.SZ"]["pre_close"] == 11.0
