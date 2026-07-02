#!/usr/bin/env bash
# 外部准点触发 GitHub 工作流（绕开 GitHub schedule 的小时级延迟/整档跳过）。
#
# 实测本仓库 schedule 延迟 2.8~5.8 小时且有整档跳过——盘中盯盘这类分钟级准点
# 需求必须由外部触发器保证。把本脚本挂到任意一台准点可靠的机器上即可：
#   - 自己的电脑/服务器 crontab（见 docs/scheduled_update.md「外部准点触发」）
#   - 阿里云函数计算定时触发器（用户已有阿里云 RDS，同账号即可）
#   - cron-job.org 等免费在线 cron（直接填 curl 的 URL+Header 也行）
#
# 用法:
#   GH_PAT=<token> ./scripts/dispatch_workflow.sh live-watch.yml
#   GH_PAT=<token> ./scripts/dispatch_workflow.sh data-update.yml master
# GH_PAT 需要 actions:write 权限（fine-grained PAT 只勾本仓库 Actions: Read and write）。
set -euo pipefail

WF="${1:?用法: dispatch_workflow.sh <workflow文件名> [ref]}"
REF="${2:-master}"
REPO="${GH_REPO:-AmbitionC/invest-model}"
: "${GH_PAT:?需要环境变量 GH_PAT（有 actions:write 权限的 GitHub PAT）}"

curl -fsS -X POST \
  -H "Authorization: Bearer $GH_PAT" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/$REPO/actions/workflows/$WF/dispatches" \
  -d "{\"ref\":\"$REF\"}"
echo "✓ 已触发 $REPO/$WF@$REF"
