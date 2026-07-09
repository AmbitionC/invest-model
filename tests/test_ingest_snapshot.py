"""R8 回归：快照录入的 entry_date 三级回退 + current_holding 资产口径。

券商快照对 ETF/转债常不带 entry_date，此前每次上传把缺失值重置为快照日 →
时间止损/盈利保护的持有期窗口坍缩为 1 天、对 ETF 永不生效。修复后：
CSV 显式值 > 旧 current_holding 值 > 快照日。
同时守住既有口径：bond/cash 不入 current_holding（转债无日线、不做 MA 风控，
盯盘 rt 接口对转债返价会误发硬止损——见 docs/remediation_plan_202607.md）。
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


def _run_ingest(monkeypatch, csv_path: str, db_url: str) -> None:
    import scripts.ingest_holding_snapshot as ing
    monkeypatch.setattr(sys, "argv",
                        ["ingest_holding_snapshot.py", "--db", db_url, "--csv", csv_path])
    ing.main()


def _csv(tmp_path: Path, name: str, snap_date: str, entry_a: str = "") -> str:
    rows = [
        "snapshot_date,code,name,asset_type,shares,available,cost_price,last_price,market_value,entry_date",
        f"{snap_date},000001.SZ,平安银行,stock,100,100,10.0,11.0,1100.0,{entry_a}",
        f"{snap_date},516120.SH,化工ETF,etf,1000,1000,0.95,0.96,960.0,",
        f"{snap_date},113001,宜化发债,bond,10,0,100.0,100.0,1000.0,",
        f"{snap_date},CASH,现金,cash,,,,,61.0,",
    ]
    p = tmp_path / name
    p.write_text("\n".join(rows), encoding="utf-8")
    return str(p)


@pytest.fixture()
def db_url(tmp_path_factory) -> str:
    return f"sqlite:///{tmp_path_factory.mktemp('snapdb') / 'snap.db'}"


def test_entry_date_preserved_across_uploads(tmp_path, monkeypatch, db_url):
    # 第一次录入：无 entry_date → 用快照日 0701
    _run_ingest(monkeypatch, _csv(tmp_path, "a.csv", "20260701"), db_url)
    repo = BaseRepository(make_engine(db_url))
    ch = repo.read_sql("SELECT code, entry_date FROM current_holding")
    got = dict(zip(ch["code"], ch["entry_date"]))
    assert got == {"000001.SZ": "20260701", "516120.SH": "20260701"}

    # 第二次录入（0703，仍无 entry_date）→ 保留首次日期，不被重置
    _run_ingest(monkeypatch, _csv(tmp_path, "b.csv", "20260703"), db_url)
    ch = repo.read_sql("SELECT code, entry_date FROM current_holding")
    got = dict(zip(ch["code"], ch["entry_date"]))
    assert got == {"000001.SZ": "20260701", "516120.SH": "20260701"}


def test_csv_explicit_entry_date_wins(tmp_path, monkeypatch, db_url):
    _run_ingest(monkeypatch, _csv(tmp_path, "a.csv", "20260701"), db_url)
    # CSV 显式给出 entry_date → 以 CSV 为准（覆盖旧值）
    _run_ingest(monkeypatch, _csv(tmp_path, "b.csv", "20260703", entry_a="20260520"), db_url)
    repo = BaseRepository(make_engine(db_url))
    ch = repo.read_sql("SELECT code, entry_date FROM current_holding")
    got = dict(zip(ch["code"], ch["entry_date"]))
    assert got["000001.SZ"] == "20260520"
    assert got["516120.SH"] == "20260701"


def test_new_code_gets_snapshot_date(tmp_path, monkeypatch, db_url):
    _run_ingest(monkeypatch, _csv(tmp_path, "a.csv", "20260701"), db_url)
    rows = [
        "snapshot_date,code,name,asset_type,shares,available,cost_price,last_price,entry_date",
        "20260703,000001.SZ,平安银行,stock,100,100,10.0,11.0,",
        "20260703,600000.SH,浦发银行,stock,200,200,8.0,8.1,",
    ]
    p = tmp_path / "c.csv"
    p.write_text("\n".join(rows), encoding="utf-8")
    _run_ingest(monkeypatch, str(p), db_url)
    repo = BaseRepository(make_engine(db_url))
    ch = repo.read_sql("SELECT code, entry_date FROM current_holding")
    got = dict(zip(ch["code"], ch["entry_date"]))
    assert got == {"000001.SZ": "20260701", "600000.SH": "20260703"}


def test_bond_and_cash_stay_out_of_current_holding(tmp_path, monkeypatch, db_url):
    _run_ingest(monkeypatch, _csv(tmp_path, "a.csv", "20260701"), db_url)
    repo = BaseRepository(make_engine(db_url))
    codes = set(repo.read_sql("SELECT code FROM current_holding")["code"])
    assert "113001" not in codes and "CASH" not in codes
    # 但 holding_snapshot 里 bond 在（口径：快照含全部资产）
    hs = repo.read_sql("SELECT code, asset_type FROM holding_snapshot")
    assert "113001" in set(hs["code"])
