"""三水表自动构建（市场资金流代理）单测：北向/两融按行业 → flow_score。"""

import pandas as pd

from invest_model.arb.watermeter import aggregate_flow
from invest_model.arb.watermeter_auto import build_watermeter_auto
from invest_model.data import create_schema, make_engine
from invest_model.repositories.base import BaseRepository


def _seed(engine):
    repo = BaseRepository(engine)
    # 3 行业各 2 只票
    info = pd.DataFrame([
        {"ts_code": "600001.SH", "industry": "半导体"},
        {"ts_code": "600002.SH", "industry": "半导体"},
        {"ts_code": "600003.SH", "industry": "地产"},
        {"ts_code": "600004.SH", "industry": "地产"},
        {"ts_code": "600005.SH", "industry": "银行"},
        {"ts_code": "600006.SH", "industry": "银行"},
    ])
    repo.upsert("stock_info", info, ["ts_code"])
    # 北向持股占比：半导体大增、地产大减、银行持平
    hk = []
    for d, semi, prop, bank in [("20260601", 1.0, 5.0, 3.0), ("20260630", 4.0, 2.0, 3.1)]:
        hk += [
            {"code": "600001.SH", "trade_date": d, "vol": 1, "ratio": semi},
            {"code": "600002.SH", "trade_date": d, "vol": 1, "ratio": semi},
            {"code": "600003.SH", "trade_date": d, "vol": 1, "ratio": prop},
            {"code": "600004.SH", "trade_date": d, "vol": 1, "ratio": prop},
            {"code": "600005.SH", "trade_date": d, "vol": 1, "ratio": bank},
            {"code": "600006.SH", "trade_date": d, "vol": 1, "ratio": bank},
        ]
    repo.upsert("stock_hk_hold", pd.DataFrame(hk), ["code", "trade_date"])
    # 两融融资余额：半导体加杠杆、其它平
    md = []
    for d, semi, other in [("20260601", 100.0, 100.0), ("20260630", 180.0, 101.0)]:
        md += [
            {"code": "600001.SH", "trade_date": d, "rzye": semi},
            {"code": "600002.SH", "trade_date": d, "rzye": semi},
            {"code": "600003.SH", "trade_date": d, "rzye": other},
            {"code": "600004.SH", "trade_date": d, "rzye": other},
            {"code": "600005.SH", "trade_date": d, "rzye": other},
            {"code": "600006.SH", "trade_date": d, "rzye": other},
        ]
    repo.upsert("stock_margin_detail", pd.DataFrame(md), ["code", "trade_date"])


def test_build_watermeter_auto_semis_in_property_out(tmp_path):
    db = f"sqlite:///{tmp_path}/wm.db"
    engine = make_engine(db)
    create_schema(engine)
    _seed(engine)
    n = build_watermeter_auto(engine, "20260630")
    assert n > 0
    repo = BaseRepository(engine)
    sig = repo.read_sql(
        "SELECT meter, dimension, key, flow_score, direction, source FROM watermeter_signal")
    assert set(sig["source"]) == {"auto"}
    # 政策资本（北向）：半导体流入 > 地产
    pol = sig[sig["meter"] == "policy_capital"].set_index("key")["flow_score"]
    assert pol["半导体"] > pol["地产"]
    assert sig[(sig["meter"] == "policy_capital") & (sig["key"] == "半导体")]["direction"].iloc[0] == "in"
    # 信贷（两融）：半导体加杠杆最高
    cred = sig[sig["meter"] == "credit"].set_index("key")["flow_score"]
    assert cred["半导体"] == cred.max()
    # 下游聚合：半导体 composite 最高（资金共振）
    flow = aggregate_flow(sig.rename(columns={}))
    top = flow.sort_values("composite", ascending=False).iloc[0]
    assert top["key"] == "半导体"


def test_build_watermeter_auto_missing_tables_graceful(tmp_path):
    db = f"sqlite:///{tmp_path}/wm2.db"
    engine = make_engine(db)
    create_schema(engine)
    # 无 stock_info 行业映射 → 返回 0，不抛
    assert build_watermeter_auto(engine, "20260630") == 0
