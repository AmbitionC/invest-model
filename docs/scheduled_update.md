# 定时数据更新

数据更新走 Tushare 增量入库（`scripts/run_pipeline.py --mode update`），缺失交易日才拉取，幂等可重复跑。
`--mode all` 则在更新后继续刷新 universe→因子→IC→预测→回测。

## 为什么不能在云端会话容器里挂 cron
Claude Code on the web 的执行环境是**临时容器**，闲置/会话结束后会被回收，里面的 crontab 不会长期存活；
且本仓库用的 SQLite `data/real.db` 是本地临时文件（2.3G、已 gitignore），不适合作为定时任务的持久目标。
**定时任务应写入持久的生产 MySQL。**

## 方案一：GitHub Actions（推荐，云端持久）
工作流见 `.github/workflows/data-update.yml`：
- 工作日 17:00(北京) 增量更新日线/估值；周六 18:00 全量刷新因子与预测；也可手动触发（选 update/all）。
- 在 **仓库 Settings → Secrets and variables → Actions** 配置：
  | Secret | 含义 |
  |---|---|
  | `TUSHARE_TOKEN` | Tushare token |
  | `TUSHARE_HTTP_URL` | 自定义接口基地址，如 `https://minitick.top/` |
  | `INVEST_DB_URL` | `mysql+pymysql://user:pass@host:3306/dbname` |
- 生产 MySQL 需允许 GitHub 运行器访问（放行出口 IP，或改用自托管 runner）。

## 方案二：自己服务器的 crontab
用 `scripts/cron_update.sh`（Linux/macOS 兼容）：
```bash
chmod +x scripts/cron_update.sh
crontab -e
# 工作日 17:00 增量
0 17 * * 1-5 /path/to/invest-model/scripts/cron_update.sh        >> /path/to/invest-model/logs/update.log 2>&1
# 周六 18:00 全量（刷新因子/预测）
0 18 * * 6   /path/to/invest-model/scripts/cron_update.sh all    >> /path/to/invest-model/logs/update.log 2>&1
```
环境变量需就绪：`TUSHARE_TOKEN`、`TUSHARE_HTTP_URL`，DB 用 `INVEST_DB_URL` 或 `.env` 的 MySQL；
也可 `export DB_URL=sqlite:///./data/real.db` 更新本地 SQLite。
