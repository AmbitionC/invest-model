"""GitHub Issue 评论推送（FaaS 端）——复刻原 workflow 里 actions/github-script 的逻辑。

原来 plan-notify / review / snapshot-remind 的「找到(或新建)跟踪 issue → 追加评论
→ 触发 GitHub 邮件提醒」在 workflow 的 github-script 步骤里；迁到 FaaS 后由本模块
用标准库 urllib 直连 GitHub REST API 完成（与 scripts/live_check.py 的 _gh_req 同款，
不引额外依赖）。

凭证：环境变量 GITHUB_TOKEN（或 GH_TOKEN，fine-grained PAT 仅需本仓库 Issues RW）
     + GITHUB_REPOSITORY（AmbitionC/invest-model）。缺任一则打日志跳过，不抛错。
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

_GH_API = "https://api.github.com"


def bj_now() -> datetime:
    """北京时间（FC/Actions 容器时区都是 UTC）"""
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))


def _cred() -> tuple[str | None, str | None]:
    return (os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN"),
            os.getenv("GITHUB_REPOSITORY"))


def _req(method: str, url: str, token: str, payload: dict | None = None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "invest-faas-scheduler",
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body) if body else {}


def _mention(repo: str) -> str:
    """要 @ 的人：NOTIFY_MENTION / LIVE_WATCH_MENTION 环境变量，缺省 @仓库主。

    评论由机器人小号发出时，@提及走「Participating, @mentions」通知通道触发邮件，
    不依赖收件人对仓库的 Watch 级别（快照提醒/计划/复盘/告警此前无提及，
    Watch 未设 All Activity 就静默，故所有推送统一带提及）。
    """
    return (os.getenv("NOTIFY_MENTION") or os.getenv("LIVE_WATCH_MENTION")
            or f"@{repo.split('/')[0]}")


def _find_or_create_issue(token: str, repo: str, title: str, seed_body: str) -> int:
    q = urllib.parse.quote(f'repo:{repo} is:issue is:open in:title "{title}"')
    found = _req("GET", f"{_GH_API}/search/issues?q={q}", token)
    for item in found.get("items", []):
        if item.get("title") == title:
            return item["number"]
    created = _req("POST", f"{_GH_API}/repos/{repo}/issues", token,
                   {"title": title, "body": seed_body})
    return created["number"]


def post_issue_comment(title: str, seed_body: str, comment_body: str,
                       dedupe_prefix: str | None = None) -> dict:
    """把 comment_body 追加到标题为 title 的跟踪 issue（不存在则创建）。

    dedupe_prefix：若最近 30 条评论中已有以该前缀开头的评论则跳过
    （对应原 plan-notify 的「链式/cron 双触发防重」，FaaS 下防 async 重试重发）。
    返回 {"posted": bool, "issue": int|None, "reason": str}。
    """
    token, repo = _cred()
    if not token or not repo:
        print(f"  (未配置 GITHUB_TOKEN/GITHUB_REPOSITORY，跳过推送「{title}」，仅打日志)")
        print(comment_body)
        return {"posted": False, "issue": None, "reason": "no-credentials"}

    number = _find_or_create_issue(token, repo, title, seed_body)
    if dedupe_prefix:
        comments = _req(
            "GET",
            f"{_GH_API}/repos/{repo}/issues/{number}/comments"
            "?per_page=30&sort=created&direction=desc",
            token)
        if any((c.get("body") or "").startswith(dedupe_prefix) for c in comments):
            print(f"  已存在以「{dedupe_prefix}」开头的评论，跳过（防重）。")
            return {"posted": False, "issue": number, "reason": "duplicate"}

    # 提及放末尾：开头保留「## 日期 …」防重前缀（dedupe_prefix 用 startswith 判断）
    _req("POST", f"{_GH_API}/repos/{repo}/issues/{number}/comments", token,
         {"body": f"{comment_body}\n\ncc {_mention(repo)}"})
    print(f"  → 已推送到 issue #{number}「{title}」")
    return {"posted": True, "issue": number, "reason": "ok"}


_JOB_CN = {
    "live_watch": "盘中盯盘", "snapshot_remind": "持仓快照提醒",
    "ingest_etf": "ETF行情入库", "daily_update_plan": "盘后计划链",
    "plan_watchdog": "计划哨兵", "weekly_rebuild_review": "周末重建复盘",
    "watermeter_remind": "水表提醒", "ingest_advisor": "投顾信号入库",
    "ingest_snapshot": "持仓快照入库", "fear_intraday": "盘中恐慌",
    "fear_daily": "恐慌指数落库",
}


def alert(job: str, err: BaseException) -> None:
    """任务失败告警：追加到「⚠️ FaaS 定时任务告警」issue → GitHub 邮件。

    对应 GitHub Actions 时代的「workflow 失败邮件」。尽力而为，自身失败只打日志。
    """
    try:
        now = bj_now().strftime("%Y-%m-%d %H:%M")
        post_issue_comment(
            "⚠️ FaaS 定时任务告警",
            seed_body="本 issue 由 FC 定时函数在任务失败时追加告警评论（原 Actions 失败邮件的替代）。",
            comment_body=(f"## {now} {_JOB_CN.get(job, job)}（`{job}`）告警\n\n"
                          f"```\n{type(err).__name__}: {err}\n```\n"
                          "详情见阿里云 FC 函数日志（invest-scheduler / invest-live-watch）。"),
        )
    except Exception as e:  # noqa: BLE001 — 告警本身失败不再级联
        print(f"  (告警推送失败：{e})")
