"""阿里云函数计算（FC）统一定时调度入口——盯盘/复盘等全部定时任务的 FaaS 化。

一份代码包、两个 FC 函数（资源配置在 fe-journey-faas 仓库 s.yaml，互相独立）：
  invest-live-watch  盯盘专用：timer 每 3 分钟（payload=live_watch），512MB/120s
  invest-scheduler   其余任务：4 个 timer 按 payload 分发，4GB/3h（容纳全量重建）

分发规则：FC timer 事件 {"triggerTime","triggerName","payload"}，payload 即 job 名
（也兼容 {"job":"..."} JSON、控制台直接传 job 名字符串、triggerName 同名兜底）。

job 一览（详见 faas/jobs.py）：
  live_watch / snapshot_remind / ingest_etf / daily_update_plan / weekly_rebuild_review

函数环境变量（必需）：
  INVEST_DB_URL 或 MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE
  TUSHARE_TOKEN、TUSHARE_HTTP_URL          （盯盘/ETF/数据更新/计划需要）
  GITHUB_TOKEN（fine-grained PAT，仅本仓库 Issues RW）、GITHUB_REPOSITORY
可选：
  INVEST_LOG_DIR=/tmp/logs  INVEST_RESULTS_DIR=/tmp/invest-results （FC 只读文件系统）
  PLAN_ARGS  ACCOUNT_CASH  PIPELINE_START=20250101  ETF_START=20230101
  LIVE_HARD_STOP  LIVE_PULLBACK  LIVE_BUY_WEIGHT  DIGEST_WINDOW  ONCE_STEP  LIVE_WATCH_MENTION

任务失败会追加评论到「⚠️ FaaS 定时任务告警」issue（→邮件），替代 Actions 失败邮件。
"""

from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def resolve_job(event) -> str:
    """从 FC 事件中解析 job 名。支持 timer payload / {"job":...} / 纯字符串。"""
    from faas.jobs import JOBS

    if isinstance(event, (bytes, bytearray)):
        event = event.decode("utf-8", errors="replace")
    if isinstance(event, str):
        s = event.strip()
        if not s:
            return ""
        try:
            event = json.loads(s)
        except ValueError:
            return s.strip('"')
        if isinstance(event, str):  # 控制台传 JSON 字符串字面量，如 "live_watch"
            return event
    if isinstance(event, dict):
        payload = event.get("payload")
        if isinstance(payload, str) and payload.strip():
            p = payload.strip()
            try:
                pj = json.loads(p)
            except ValueError:
                return p
            if isinstance(pj, dict) and pj.get("job"):
                return str(pj["job"])
            if isinstance(pj, str):
                return pj
            return p
        if isinstance(payload, dict) and payload.get("job"):
            return str(payload["job"])
        if event.get("job"):
            return str(event["job"])
        trigger = event.get("triggerName", "")
        if trigger in JOBS:
            return trigger
    return ""


def handler(event, context):  # noqa: ARG001 — FC 标准签名
    from faas import gh_notify
    from faas.jobs import JOBS

    job = resolve_job(event)
    if job not in JOBS:
        raise ValueError(
            f"无法从事件解析 job（got={job!r}）。请在 timer 触发器的 payload 里配置 "
            f"job 名，可选：{sorted(JOBS)}")

    print(f"[invest-scheduler] job={job} 开始")
    t0 = time.time()
    try:
        result = JOBS[job]()
    except BaseException as e:  # noqa: BLE001 — 含 SystemExit(脚本 sys.exit)
        print(f"[invest-scheduler] job={job} 失败：{type(e).__name__}: {e}")
        gh_notify.alert(job, e)
        raise
    print(f"[invest-scheduler] job={job} 完成，耗时 {time.time() - t0:.1f}s: {result}")
    return json.dumps(result, ensure_ascii=False, default=str)


if __name__ == "__main__":  # 本地验证：python faas/scheduler_handler.py <job>
    print(handler(json.dumps({"job": sys.argv[1] if len(sys.argv) > 1 else "live_watch"}),
                  None))
