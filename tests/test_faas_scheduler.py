"""FaaS 统一调度器（faas/scheduler_handler + jobs + gh_notify）单元测试。

不打网、不连 MySQL：GitHub API 用 monkeypatch 假 _req，DB 用 sqlite 临时库。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from faas import gh_notify, jobs  # noqa: E402
from faas.scheduler_handler import handler, resolve_job  # noqa: E402


# ── resolve_job：FC timer 事件各形态 ─────────────────────────────────

def test_resolve_job_timer_payload_bytes():
    event = json.dumps({"triggerTime": "2026-07-02T09:00:00Z",
                        "triggerName": "daily-update-plan",
                        "payload": "daily_update_plan"}).encode()
    assert resolve_job(event) == "daily_update_plan"


def test_resolve_job_payload_json_job():
    event = json.dumps({"payload": json.dumps({"job": "ingest_etf"})})
    assert resolve_job(event) == "ingest_etf"


def test_resolve_job_plain_string_and_dict():
    assert resolve_job("live_watch") == "live_watch"
    assert resolve_job(b'"snapshot_remind"') == "snapshot_remind"
    assert resolve_job({"job": "weekly_rebuild_review"}) == "weekly_rebuild_review"


def test_resolve_job_trigger_name_fallback():
    assert resolve_job({"triggerName": "live_watch", "payload": ""}) == "live_watch"


def test_resolve_job_unknown():
    assert resolve_job({"triggerName": "no-such"}) == ""
    assert resolve_job("") == ""


def test_handler_unknown_job_raises():
    with pytest.raises(ValueError, match="无法从事件解析 job"):
        handler(b"{}", None)


def test_handler_dispatch_and_json_result(monkeypatch):
    monkeypatch.setitem(jobs.JOBS, "live_watch", lambda: {"checked": 3, "pushed": 0})
    out = json.loads(handler(json.dumps({"payload": "live_watch"}), None))
    assert out == {"checked": 3, "pushed": 0}


def test_handler_failure_alerts_and_reraises(monkeypatch):
    alerts = []
    monkeypatch.setattr(gh_notify, "alert", lambda job, err: alerts.append((job, str(err))))

    def boom():
        raise RuntimeError("db down")

    monkeypatch.setitem(jobs.JOBS, "ingest_etf", boom)
    with pytest.raises(RuntimeError, match="db down"):
        handler('{"payload": "ingest_etf"}', None)
    assert alerts == [("ingest_etf", "db down")]


# ── gh_notify：找/建 issue、追评、防重 ────────────────────────────────

class _FakeGH:
    """记录 _req 调用并模拟 search/create/comments 三个端点。"""

    def __init__(self, existing_issue: bool, existing_comments: list[str] | None = None):
        self.existing_issue = existing_issue
        self.existing_comments = existing_comments or []
        self.posted: list[tuple[str, dict | None]] = []

    def __call__(self, method, url, token, payload=None):
        self.posted.append((f"{method} {url.split('api.github.com')[1]}", payload))
        if "/search/issues" in url:
            items = [{"title": "📈 每日操作计划", "number": 7}] if self.existing_issue else []
            return {"items": items}
        if url.endswith("/issues") and method == "POST":
            return {"number": 42}
        if "/comments" in url and method == "GET":
            return [{"body": b} for b in self.existing_comments]
        return {}


def test_post_comment_no_credentials(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    res = gh_notify.post_issue_comment("t", "seed", "body")
    assert res == {"posted": False, "issue": None, "reason": "no-credentials"}
    assert "body" in capsys.readouterr().out


def test_post_comment_creates_issue_and_comments(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPOSITORY", "AmbitionC/invest-model")
    fake = _FakeGH(existing_issue=False)
    monkeypatch.setattr(gh_notify, "_req", fake)
    res = gh_notify.post_issue_comment("📈 每日操作计划", "seed", "## 2026-07-02 盘后操作计划\n\nX")
    assert res == {"posted": True, "issue": 42, "reason": "ok"}
    # 所有推送评论末尾带 @提及（走 mentions 通知通道，不依赖 Watch 级别）
    posted_bodies = [pl["body"] for c, pl in fake.posted if "/issues/42/comments" in c and pl]
    assert posted_bodies and posted_bodies[0].startswith("## 2026-07-02 盘后操作计划")
    assert posted_bodies[0].rstrip().endswith("cc @AmbitionC")
    assert any("POST /repos/AmbitionC/invest-model/issues" == c and p["title"] == "📈 每日操作计划"
               for c, p in fake.posted)
    assert any("/issues/42/comments" in c for c, _ in fake.posted)


def test_post_comment_dedupe_skips(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPOSITORY", "AmbitionC/invest-model")
    fake = _FakeGH(existing_issue=True,
                   existing_comments=["## 2026-07-02 盘后操作计划\n\n旧的"])
    monkeypatch.setattr(gh_notify, "_req", fake)
    res = gh_notify.post_issue_comment("📈 每日操作计划", "seed", "## 2026-07-02 盘后操作计划\n\n新的",
                                       dedupe_prefix="## 2026-07-02 盘后操作计划")
    assert res == {"posted": False, "issue": 7, "reason": "duplicate"}
    assert not any("POST" in c and "/comments" in c for c, _ in fake.posted)


# ── 交易日守卫 + 快照提醒 job（sqlite，不出网）─────────────────────────

@pytest.fixture()
def sqlite_db(tmp_path, monkeypatch):
    db = f"sqlite:///{tmp_path}/t.db"
    eng = create_engine(db)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trade_calendar (cal_date TEXT PRIMARY KEY, is_open INTEGER)"))
        conn.execute(text("INSERT INTO trade_calendar VALUES ('20260702', 0), ('20260703', 1)"))
    monkeypatch.setenv("INVEST_DB_URL", db)
    return db


def test_is_trade_day(sqlite_db):
    assert jobs._is_trade_day("20260702") is False
    assert jobs._is_trade_day("20260703") is True
    assert jobs._is_trade_day("20991231") is True  # 无记录按交易日处理


def test_snapshot_remind_skips_non_trading_day(sqlite_db, monkeypatch):
    monkeypatch.setattr(gh_notify, "bj_now",
                        lambda: __import__("datetime").datetime(2026, 7, 2, 15, 20))
    called = []
    monkeypatch.setattr(gh_notify, "post_issue_comment",
                        lambda *a, **k: called.append(a) or {"posted": True})
    res = jobs.job_snapshot_remind()
    assert res["skipped"] == "non-trading-day"
    assert not called


def test_snapshot_remind_posts_on_trading_day(sqlite_db, monkeypatch):
    monkeypatch.setattr(gh_notify, "bj_now",
                        lambda: __import__("datetime").datetime(2026, 7, 3, 15, 20))
    posted = {}

    def fake_post(title, seed_body, comment_body, dedupe_prefix=None):
        posted.update(title=title, body=comment_body, dedupe=dedupe_prefix)
        return {"posted": True, "issue": 1, "reason": "ok"}

    monkeypatch.setattr(gh_notify, "post_issue_comment", fake_post)
    res = jobs.job_snapshot_remind()
    assert res["posted"] is True
    assert posted["title"] == "📸 持仓快照提醒"
    assert posted["body"].startswith("## 20260703 收盘")
    assert posted["dedupe"] == "## 20260703 收盘"


# ── 计划 job 的周末守卫与参数拼装 ─────────────────────────────────────

def test_build_plan_skipped_on_weekend(monkeypatch):
    monkeypatch.setattr(gh_notify, "bj_now",
                        lambda: __import__("datetime").datetime(2026, 7, 4, 18, 0))  # 周六
    assert jobs._build_and_post_plan() == {"plan": "skipped-weekend"}


def test_daily_update_plan_persists_snapshot_when_update_fails(monkeypatch):
    """数据源失败（ip超限/Tushare 抖动）时，账户快照与恐慌仍按库内数据落库，
    仅跳过出计划——快照每日推进不被数据源阻塞。"""
    calls = []

    def boom_update(main_func, argv):
        calls.append(argv[argv.index("--mode") + 1] if "--mode" in argv else argv)
        raise SystemExit(1)   # run_pipeline --mode update 失败即 sys.exit(1)

    monkeypatch.setattr(jobs, "_run_cli", boom_update)
    monkeypatch.setattr(jobs, "_persist_fear_daily", lambda: "ok:20260709")
    monkeypatch.setattr(jobs, "_persist_account_snapshot_daily", lambda: "ok:20260709")
    # 计划链不应被触达
    monkeypatch.setattr(jobs, "_build_and_post_plan",
                        lambda: (_ for _ in ()).throw(AssertionError("不应出计划")))

    out = jobs.job_daily_update_plan()
    assert out["update"] == "failed"
    assert out["account"] == "ok:20260709"
    assert out["fear"] == "ok:20260709"
    assert out["plan"] == "skipped:update-failed"


def test_run_cli_restores_argv():
    seen = []
    jobs._run_cli(lambda: seen.append(list(sys.argv)), ["x.py", "--flag"])
    assert seen == [["x.py", "--flag"]]
    assert sys.argv != ["x.py", "--flag"]


# ── tushare keep-alive 会话补丁（FC 出口 IP 不固定 → ip超限 的修复）──────

def test_use_keepalive_session_patches_module():
    import requests as _requests
    from tushare.pro import client as pro_client
    from invest_model.sources.tushare_client import _use_keepalive_session

    original = pro_client.requests
    try:
        _use_keepalive_session()
        assert isinstance(pro_client.requests, _requests.Session)
        first = pro_client.requests
        _use_keepalive_session()  # 幂等：不重复创建
        assert pro_client.requests is first
    finally:
        pro_client.requests = original


# ── account_snapshot 日更重估：停牌回退 + 转债并入（R3）───────────────────

@pytest.fixture()
def reval_db(tmp_path, monkeypatch):
    db = f"sqlite:///{tmp_path}/reval.db"
    from invest_model.data import create_schema, make_engine
    create_schema(make_engine(db))
    eng = create_engine(db)
    with eng.begin() as conn:
        # 两只持仓：A 有当日(0709)收盘；B 停牌，最后收盘在 0703
        conn.execute(text(
            "INSERT INTO stock_daily (code, trade_date, close) VALUES "
            "('AAA.SZ','20260703',10.0), ('AAA.SZ','20260709',11.0), "
            "('BBB.SZ','20260703',20.0)"))
        conn.execute(text(
            "INSERT INTO current_holding (code, shares, cost_price, entry_date) VALUES "
            "('AAA.SZ',100,9.0,'20260601'), ('BBB.SZ',200,21.0,'20260601')"))
        # 旧快照：现金 61 + 一行转债 1000（当日无手动快照 → 触发重估）
        conn.execute(text(
            "INSERT INTO account_snapshot (snapshot_date, cash, market_value, total_asset) "
            "VALUES ('20260703', 61.0, 100000.0, 100061.0)"))
        conn.execute(text(
            "INSERT INTO holding_snapshot (snapshot_date, code, name, asset_type, shares, "
            "market_value, last_price) VALUES "
            "('20260703','113001','宜化发债','bond',10,1000.0,100.0)"))
    monkeypatch.setenv("INVEST_DB_URL", db)
    return db


def test_reval_suspended_stock_and_bond_included(reval_db):
    res = jobs._persist_account_snapshot_daily()
    assert res == "ok:20260709"
    eng = create_engine(reval_db)
    with eng.begin() as conn:
        row = conn.execute(text(
            "SELECT cash, market_value, total_asset FROM account_snapshot "
            "WHERE snapshot_date='20260709'")).fetchone()
    # AAA 100×11 + BBB(停牌,回退0703收盘) 200×20 + 转债 1000 = 6100；现金沿用 61
    assert row[0] == 61.0
    assert row[1] == 6100.0
    assert row[2] == 6161.0


def test_reval_skips_when_manual_exists(reval_db):
    eng = create_engine(reval_db)
    with eng.begin() as conn:
        conn.execute(text(
            "INSERT INTO account_snapshot (snapshot_date, cash, market_value, total_asset) "
            "VALUES ('20260709', 1.0, 2.0, 3.0)"))
    assert jobs._persist_account_snapshot_daily() == "skip:manual-exists:20260709"


# ── 计划现金源：ACCOUNT_CASH env 优先，未配置读最新快照（R5）──────────────

def _capture_plan_cash(monkeypatch):
    seen = {}

    def fake_plan_main():
        seen["argv"] = list(sys.argv)
        i = sys.argv.index("--out")
        Path(sys.argv[i + 1]).write_text("plan", encoding="utf-8")

    import scripts.build_action_plan as bap
    monkeypatch.setattr(bap, "main", fake_plan_main)
    monkeypatch.setattr(gh_notify, "post_issue_comment",
                        lambda *a, **k: {"posted": True})
    monkeypatch.setattr(gh_notify, "bj_now",
                        lambda: __import__("datetime").datetime(2026, 7, 9, 18, 0))  # 周四
    return seen


def test_plan_cash_from_snapshot_when_env_unset(reval_db, monkeypatch):
    monkeypatch.delenv("ACCOUNT_CASH", raising=False)
    seen = _capture_plan_cash(monkeypatch)
    jobs._build_and_post_plan()
    i = seen["argv"].index("--cash")
    assert seen["argv"][i + 1] == "61.0"      # 最新 account_snapshot(0703).cash


def test_plan_cash_env_overrides(reval_db, monkeypatch):
    monkeypatch.setenv("ACCOUNT_CASH", "1061")
    seen = _capture_plan_cash(monkeypatch)
    jobs._build_and_post_plan()
    i = seen["argv"].index("--cash")
    assert seen["argv"][i + 1] == "1061"
