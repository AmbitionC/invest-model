#!/bin/bash
# SessionStart hook: 准备 invest-model 运行环境（安装依赖）。
# 默认仅在 Claude Code on the web（远程环境）执行；本地开发不受影响。
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-.}"

# 安装包及全部可选依赖（dev=pytest，ml=xgboost+scikit-learn）。
# 可重复执行（幂等）；容器状态在 hook 完成后会被缓存。
pip install -e ".[dev,ml]"

# 若无 .env，则由示例生成一份。仅当环境未提供 INVEST_DB_URL 时才回落本地 SQLite，
# 环境（Claude on the web 的环境变量）已配 INVEST_DB_URL 则尊重环境值，不覆盖。
if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
  if [ -z "${INVEST_DB_URL:-}" ]; then
    printf '\n# 由 session-start hook 自动追加：环境未配 INVEST_DB_URL，回落本地 SQLite\nINVEST_DB_URL=sqlite:///./data/local.db\n' >> .env
  fi
fi
