"""FaaS 定时任务实现——原 GitHub Actions 定时 workflow 的等价迁移。

每个 job 都是无状态单次执行：读写生产 MySQL，结果以 GitHub Issue 评论推送（→邮件），
与原 workflow 行为一一对应：

  job_live_watch            ← live-watch.yml     盘中盯盘（每 3 分钟无状态扫描）
  job_snapshot_remind       ← snapshot-remind.yml 持仓快照提醒（交易日 15:20）
  job_ingest_etf            ← ingest-etf.yml     ETF 前复权日线入库（交易日 16:50）
  job_daily_update_plan     ← data-update.yml(update档) + plan-notify.yml
                              增量更新后链式出计划（17:00 起，天然保证先数据后计划）
  job_weekly_rebuild_review ← data-update.yml(周六all档) + P4影子回测 + review.yml
                              周六全量重建 → 影子回测（不阻断）→ 复盘推送

可写目录只有 /tmp（FC 代码目录只读），所有 --out 都落 /tmp。
环境变量见 faas/scheduler_handler.py 模块注释。
"""

from __future__ import annotations

import os
import shlex
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from faas import gh_notify  # noqa: E402

# 与原 workflow 相同的默认参数（可用环境变量覆盖）
_PIPELINE_START = os.getenv("PIPELINE_START", "20250101")
_ETF_START = os.getenv("ETF_START", "20230101")
_DEFAULT_PLAN_ARGS = ("--advisor-led --risk --trend-filter "
                      "--concentration medium --time-stop-days 8")


def _run_cli(main_func, argv: list[str]) -> None:
    """以指定 argv 调用脚本的 main()（脚本都是 argparse CLI，不改脚本本身）。"""
    old_argv = sys.argv
    sys.argv = argv
    try:
        main_func()
    finally:
        sys.argv = old_argv


def _is_trade_day(day: str) -> bool:
    """trade_calendar 守卫（同 snapshot-remind.yml 内联 python）：查询失败按交易日处理。"""
    try:
        from invest_model.data import make_engine
        from invest_model.repositories.base import BaseRepository
        repo = BaseRepository(make_engine())
        if repo.table_exists("trade_calendar"):
            df = repo.read_sql(
                "SELECT is_open FROM trade_calendar WHERE cal_date=:d", {"d": day})
            if not df.empty:
                return int(df["is_open"].iloc[0]) == 1
    except Exception as e:  # noqa: BLE001
        print(f"trade_calendar 守卫查询失败，按交易日处理：{e}")
    return True


# ── 盯盘（live-watch）────────────────────────────────────────────────

def job_live_watch() -> dict:
    """盘中盯盘单次无状态扫描；时段/节假日守卫在脚本内，毫秒级退出。"""
    from scripts.live_check import run_once
    return run_once()


# ── 持仓快照提醒（snapshot-remind）───────────────────────────────────

def job_snapshot_remind() -> dict:
    day = gh_notify.bj_now().strftime("%Y%m%d")
    if not _is_trade_day(day):
        return {"job": "snapshot_remind", "skipped": "non-trading-day", "day": day}
    res = gh_notify.post_issue_comment(
        "📸 持仓快照提醒",
        seed_body="本 issue 每交易日收盘后提醒上传当天券商持仓截图（快照入库用）。",
        comment_body=(f"## {day} 收盘\n\n"
                      "📸 **请把今天的券商「持仓+现金」截图发到会话**，我转成快照入库。\n"
                      "赶在 **17:00 的盘后操作计划**前完成，计划的风控段就用上你今天的真实持仓。"),
        dedupe_prefix=f"## {day} 收盘",
    )
    return {"job": "snapshot_remind", "day": day, **res}


# ── ETF 日线入库（ingest-etf）────────────────────────────────────────

def job_ingest_etf() -> dict:
    from scripts.ingest_etf_daily import main as etf_main
    _run_cli(etf_main, ["ingest_etf_daily.py", "--start", _ETF_START])
    return {"job": "ingest_etf", "ok": True}


# ── 盘后增量更新 + 操作计划（data-update + plan-notify 链）──────────────

def _build_and_post_plan() -> dict:
    """生成三段式操作计划并推送（等价 plan-notify.yml 的两个步骤）。"""
    bj = gh_notify.bj_now()
    if bj.weekday() >= 5:  # 周末不发计划（同原 github-script 守卫）
        return {"plan": "skipped-weekend"}
    plan_args = shlex.split(os.getenv("PLAN_ARGS") or _DEFAULT_PLAN_ARGS)
    cash = os.getenv("ACCOUNT_CASH", "0")
    out = "/tmp/action_plan.md"
    from scripts.build_action_plan import main as plan_main
    _run_cli(plan_main,
             ["build_action_plan.py", *plan_args, "--cash", cash, "--out", out])
    with open(out, encoding="utf-8") as f:
        body = f.read()
    today = bj.strftime("%Y-%m-%d")
    res = gh_notify.post_issue_comment(
        "📈 每日操作计划",
        seed_body="本 issue 由 FC 定时函数每个交易日盘后追加操作计划评论。",
        comment_body=f"## {today} 盘后操作计划\n\n{body}",
        dedupe_prefix=f"## {today} 盘后操作计划",
    )
    return {"plan": res}


def _persist_fear_daily() -> str:
    """恐慌指数按日落库（仪表盘历史曲线）；失败不阻断出计划。"""
    try:
        from invest_model.data import make_engine
        from invest_model.signals.fear import fear_gauge
        from scripts.fear_gauge import persist_fear
        engine = make_engine()
        persist_fear(engine, fear_gauge(engine))
        return "ok"
    except Exception as e:  # noqa: BLE001
        print(f"WARN fear_daily 落库失败：{e}")
        return f"WARN: {e}"


def job_daily_update_plan() -> dict:
    """增量数据更新成功后链式出计划；更新失败不出计划（避免旧数据误导）。"""
    from scripts.run_pipeline import main as pipe_main
    _run_cli(pipe_main, ["run_pipeline.py", "--mode", "update",
                         "--start", _PIPELINE_START])
    fear = _persist_fear_daily()
    return {"job": "daily_update_plan", "update": "ok", "fear": fear,
            **_build_and_post_plan()}


# ── 周六全量重建 + P4 影子回测 + 复盘（data-update all档 + review 链）────

def job_weekly_rebuild_review() -> dict:
    from scripts.run_pipeline import main as pipe_main
    out: dict = {"job": "weekly_rebuild_review"}

    # 1) 全量刷新 universe→因子→IC→预测→回测（失败告警但不阻断复盘，
    #    对齐原架构里 review.yml 独立于 data-update 运行的行为）
    try:
        _run_cli(pipe_main, ["run_pipeline.py", "--mode", "all",
                             "--start", _PIPELINE_START])
        out["rebuild"] = "ok"
    except BaseException as e:  # noqa: BLE001 — 含 SystemExit
        out["rebuild"] = f"FAIL: {e}"
        gh_notify.alert("weekly_rebuild(all)", e)

    # 2) P4 影子对照回测（原 workflow 即「失败不阻断」）
    try:
        _run_cli(pipe_main, ["run_pipeline.py", "--mode", "backtest",
                             "--start", _PIPELINE_START, "--version", "pf_v2",
                             "--scheme", "inv_vol", "--hold-buffer", "1.5"])
        out["p4_shadow"] = "ok"
    except BaseException as e:  # noqa: BLE001
        out["p4_shadow"] = f"WARN: {e}"
        print(f"WARN P4 影子回测失败（不阻断）：{e}")

    # 3) 复盘（纯读 DB）+ 推送
    review_out = "/tmp/review.md"
    from scripts.review import main as review_main
    _run_cli(review_main, ["review.py", "--out", review_out])
    with open(review_out, encoding="utf-8") as f:
        body = f.read()
    today = gh_notify.bj_now().strftime("%Y-%m-%d")
    out["review"] = gh_notify.post_issue_comment(
        "🔍 复盘报告",
        seed_body="本 issue 由 FC 定时函数每周追加复盘报告（投顾/模型/持仓/纪律 与真实收益对账）。",
        comment_body=f"## {today} 复盘\n\n{body}",
        dedupe_prefix=f"## {today} 复盘",
    )
    return out


JOBS = {
    "live_watch": job_live_watch,
    "snapshot_remind": job_snapshot_remind,
    "ingest_etf": job_ingest_etf,
    "daily_update_plan": job_daily_update_plan,
    "weekly_rebuild_review": job_weekly_rebuild_review,
}
