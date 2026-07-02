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

## 外部准点触发（分钟级准点的唯一可靠路径）

**实测本仓库（私有库）的 GitHub schedule 延迟 2.8~5.8 小时，且发生过整档跳过**
（2026-07-01 live-check 07:20 UTC 档、2026-07-02 live-watch 01:25 UTC 档均未触发）。
GitHub 官方也明确 schedule 是"尽力而为"。因此：

- **盘后任务**（data-update / plan-notify）：延迟几小时尚可容忍，且 plan-notify 已改为
  data-update 完成后链式触发（`workflow_run`），只吃一层延迟。
- **盘中盯盘**（live-watch）：分钟级准点靠 schedule 不可能。仓库内已做三层缓解
  （07:10 早鸟档 + 09:25 直发档 + live-watch-guard 看门狗补拉），但要**保证**
  09:25 准点开盯，请再配一个外部触发器（任选其一）：

```bash
# ① 自己的电脑/服务器 crontab（准点可靠）
# 需要 fine-grained PAT：仅勾选本仓库 Actions: Read and write
20 9  * * 1-5  GH_PAT=<token> /path/to/invest-model/scripts/dispatch_workflow.sh live-watch.yml
5 17  * * 1-5  GH_PAT=<token> /path/to/invest-model/scripts/dispatch_workflow.sh data-update.yml
```

- ② **阿里云函数计算**定时触发器（已有阿里云账号）：新建定时函数，内容即
  `dispatch_workflow.sh` 里的那个 curl（PAT 放函数环境变量）。
- ③ **cron-job.org** 等免费在线 cron：POST
  `https://api.github.com/repos/AmbitionC/invest-model/actions/workflows/live-watch.yml/dispatches`，
  Header `Authorization: Bearer <PAT>`、`Accept: application/vnd.github+json`，
  Body `{"ref":"master"}`，北京时间 09:20 工作日执行。

live-watch 的并发策略是「排队不打断」：外部触发与仓库内档位重叠时只排队，
不会打断已在跑的健康实例，也不会重复报警（去重集从当日 issue 评论恢复）。

## 盯盘迁出 GitHub Actions（成本必读）

**常驻盯盘在 GitHub runner 上跑，空转等待也按分钟计费**：每交易日 ~320 分钟，
免费额度 2000 分钟/月约 6 个交易日耗尽，超额 $0.008/分钟（≈¥18/交易日）。
盘后任务（data-update ~15min/日、plan-notify ~10min/日、周六全量 ~90min）
合计每月仅 ~1000 分钟，留在 Actions 没问题。**只需把 live-watch 迁出去。**

### 方案 A：自己的电脑/服务器跑脚本（推荐，零成本，顺带解决准点问题）

脚本自包含，任何能出网的机器都能跑：

```bash
# 1) 环境变量（.env 或 export）：
#    INVEST_DB_URL=mysql+pymysql://...   TUSHARE_TOKEN=...   TUSHARE_HTTP_URL=...
#    GITHUB_TOKEN=<PAT: 本仓库 Issues: Read and write>   GITHUB_REPOSITORY=AmbitionC/invest-model
# 2) crontab（工作日 09:20 启动；脚本自带节假日守卫/收盘退出/去重，与云端行为一致）：
20 9 * * 1-5  cd /path/to/invest-model && python scripts/live_check.py --watch --notify \
    --min-interval 20 --max-interval 180 --digest-window 20 \
    --hard-stop 0.08 --pullback-pct 0.03 >> logs/live_watch.log 2>&1
# 3) 迁移完成后：GitHub 仓库 Settings → Variables 设 LIVE_WATCH_ON_ACTIONS=0
```

预警仍推到同一个 GitHub issue（邮件不变）；本地/云端切换随时可逆（删掉该 Variable 即回云端）。

### 方案 B：自托管 runner（零代码改动，分钟数免费）

在常开机器装 GitHub self-hosted runner，把 live-watch.yml 的 `runs-on` 改成
`[self-hosted]`——自托管 runner 不消耗 Actions 分钟额度，工作流逻辑零改动。

### 方案 C：留在云端付费

维持现状 ≈ 每月超额 6000 分钟 ≈ $48/月。不建议；若临时接受，请在
Settings → Billing 给 Actions 设一个小额预算（如 $10）而非 $0——$0 会在额度
耗尽后**冻结全部工作流**（盘后计划/数据更新一并停摆）。
