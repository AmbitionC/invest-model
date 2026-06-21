#!/usr/bin/env bash
# 本地/服务器 crontab 用的数据更新脚本（适合你自己有持久机器的场景）。
#
# 用法：
#   1) 改下面 PROJECT_DIR / DB_URL（或导出 INVEST_DB_URL 环境变量）
#   2) chmod +x scripts/cron_update.sh
#   3) crontab -e 加一行（工作日 17:00 增量更新）：
#        0 17 * * 1-5 /path/to/invest-model/scripts/cron_update.sh >> /path/to/invest-model/logs/update.log 2>&1
#      可选：周六 18:00 全量刷新因子/预测：
#        0 18 * * 6   /path/to/invest-model/scripts/cron_update.sh all >> ... 2>&1
#
# 需保证环境变量已就绪：TUSHARE_TOKEN、TUSHARE_HTTP_URL，以及 DB（INVEST_DB_URL 或 .env 的 MySQL）。
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
MODE="${1:-update}"                        # update | all
DB_ARG=""
[ -n "${DB_URL:-}" ] && DB_ARG="--db ${DB_URL}"   # 不设则走 INVEST_DB_URL / .env

cd "$PROJECT_DIR"
START="$(date -d '40 days ago' +%Y%m%d 2>/dev/null || date -v-40d +%Y%m%d)"  # Linux / macOS 兼容
echo "[$(date '+%F %T')] data update mode=$MODE start=$START db=${DB_URL:-<env>}"
python scripts/run_pipeline.py --mode "$MODE" --start "$START" $DB_ARG
echo "[$(date '+%F %T')] done."
