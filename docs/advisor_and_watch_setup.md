# 投顾录入 + 日常盯盘：前置配置清单

本文汇总「投顾观点录入→更新模型」与「日常盯盘」两块能力所需的全部前置配置，
一处对齐。两块共享同一套地基：**一个装了真实 A 股数据的持久库 + Tushare 访问凭证**。

## 0. 共同地基（两块都要）

| 前置项 | 用途 | 怎么配 |
|---|---|---|
| `TUSHARE_TOKEN` | 拉真实行情/估值、盘中实时价(`rt_k`) | GitHub Secrets，或本地 `.env` |
| `TUSHARE_HTTP_URL` | 数据服务基地址（含 VIP 接口） | 填当前镜像地址（如 `https://ts.gyzcloud.top/api`，随所购套餐给的地址为准，结尾按其文档要求带 `/api`） |
| 真实数据入库 | 投顾录入要校验代码命中 `stock_info`；盯盘 MA/买点位来自 `stock_daily` | 见下方「数据回填」 |
| 持久后端 | 信号/持仓/计划要留存 | 生产 MySQL（云端自动化）或本地 `data/real.db`（自己机器） |

> 注意：**Claude Code on the web 这个会话容器的出网策略屏蔽了 minitick/tushare**
> （CONNECT 403），且容器临时回收。所以真实拉数和实时盯盘**不在本会话里跑**，
> 而是跑在 ① 你自己已白名单 minitick 的机器，或 ② GitHub Actions（见下）。

### 数据回填（首次，二选一后端）

```bash
# 后端A：本地 SQLite（在你自己的机器上）
python scripts/fetch_local_sample.py --db sqlite:///./data/real.db --start 20190101
python scripts/run_pipeline.py --mode all --db sqlite:///./data/real.db

# 后端B：生产 MySQL（INVEST_DB_URL 已配）
python scripts/run_pipeline.py --mode update --start 20190101
python scripts/run_pipeline.py --mode all
```

## 1. 投顾观点 → 总结并更新模型

**交互式流程（代码已就绪）**：你发研报/早午盘 → 我按
`docs/advisor_extraction_prompt.md` 提炼成结构化 CSV（个股 `advisor_reco` /
主题 `advisor_theme`，含 A/B/C 分级、long/reduce/avoid/exit 方向、催化剂、失效日）
→ **A级/看空/重仓项请你确认** → 入库 → 操作计划/回测自动读当期有效信号。

```bash
python scripts/ingest_advisor.py --kind reco  --csv config/advisor_signal_template.csv --db <DB>
python scripts/ingest_advisor.py --kind theme --csv config/advisor_theme_template.csv --db <DB>
```

前置：除「共同地基」外无新增。模板见 `config/advisor_signal_template.csv`、
`config/advisor_theme_template.csv`。

## 2. 日常盯盘

**工具（代码已就绪）**：
- `scripts/live_check.py`：盘中实时（`rt_k` 现价 + 持仓硬止损/破MA20 + 观察池买点）。
- `scripts/build_action_plan.py`：盘后三段式计划（建议买入/持仓风控/观察池）。

**额外前置**：录入你的真实持仓（时间止损/移动止盈才准）：

```bash
# 编辑 config/holding_template.csv（code,shares,cost_price,entry_date）后：
python scripts/ingest_holding.py --csv config/holding_template.csv --db <DB>
```

ETF 观察清单：`config/watch_etf.txt`。（移动止盈白名单 `trailing_only.txt` 已于 2026-07-17 移除——硬止损对所有持仓一视同仁、无豁免。）

```bash
python scripts/live_check.py --db <DB> --alert          # 盘中轮询，只报触发项
python scripts/build_action_plan.py --advisor-led --risk --out results/action_plan.md --db <DB>
```

## 3. 云端自动化（GitHub Actions，可选但推荐）

两个工作流已就绪：
- `.github/workflows/data-update.yml`：工作日 17:00 增量、周六 18:00 全量刷新。
- `.github/workflows/plan-notify.yml`：工作日 17:30 生成计划，追加到 GitHub Issue
  「📈 每日操作计划」评论 → 你收到邮件提醒。

**一次性配置**（仓库 Settings → Secrets and variables → Actions）。

数据库 secret 两种配法，二选一（工作流两种都支持，`INVEST_DB_URL` 优先）：

- **配法① 复用 fe-journey-faas 的同名 secret（推荐，可共用）**：直接复用
  `DB_HOST` / `DB_USER` / `DB_PASS` 三个 secret（值与 faas 指向同一台 RDS）。
  把它们设为 **Organization secret** 即可两仓共享、改一处两边生效。库名由工作流固定为
  `invest`（可用 Variable `INVEST_DB_NAME` 覆盖）。代码侧会对用户名/密码做 URL 转义。
- **配法② 单个整串 URL（隔离账号时用）**：只配一个 `INVEST_DB_URL`，指向 `invest` 库。

| 类型 | 名称 | 值 / 说明 |
|---|---|---|
| Secret（配法①） | `DB_HOST` | RDS 外网地址，如 `rm-xxx.mysql.rds.aliyuncs.com` |
| Secret（配法①） | `DB_USER` | DB 用户名 |
| Secret（配法①） | `DB_PASS` | DB 密码（含特殊字符也行，代码会转义） |
| Secret（配法②，替代①） | `INVEST_DB_URL` | `mysql+pymysql://用户:密码@host:3306/invest?charset=utf8mb4` |
| Secret | `TUSHARE_TOKEN` | 你的 Tushare token |
| Secret | `TUSHARE_HTTP_URL` | 当前镜像地址，如 `https://ts.gyzcloud.top/api`（随套餐给的地址；生产唯一真源，代码不再硬编码） |
| Variable(可选) | `INVEST_DB_NAME` | 覆盖库名，默认 `invest` |
| Variable(可选) | `ACCOUNT_CASH` | 账户现金，如 `54089` |
| Variable(可选) | `PLAN_ARGS` | 覆盖默认 `--advisor-led --risk --trend-filter --concentration medium --time-stop-days 8` |

> 注意：GitHub secret 名不能以 `GITHUB_` 开头（故 faas 用 `GH_TOKEN`）。本仓的
> `plan-notify` 发 issue 用的是 Actions 内置的 `GITHUB_TOKEN`，无需另配。

注意：
- 生产 MySQL 需允许 GitHub 运行器访问（放行出口 IP，或自托管 runner）。
- `plan-notify` 依赖 `data-update` 已把当日数据/预测刷进 MySQL。
- 未配 Secrets 前定时任务只失败、无副作用。
- 配好后可在 Actions 页手动 `Run workflow` 跑一次 `data-update`(mode=all) 验证。

### 复用已有阿里云 RDS（方案 A）

若已有阿里云 RDS（如与 fe-journey-faas 共用实例），按下列方式复用，**不与其它库混表**：

1. **建独立库**（DMS/客户端执行）：
   ```sql
   CREATE DATABASE invest CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
   ```
   沿用现有 DB 账号即可；想隔离权限可另建 `invest_app` 用户只授权 `invest.*`。
2. **网络可达**：RDS 控制台「数据库连接」确认有**外网地址**；「白名单设置」需放行
   GitHub runner（其出口 IP 为大段动态范围，通常设 `0.0.0.0/0` + 强口令；想彻底不暴露
   则改用阿里云 FC 同 VPC 内网方案）。
3. **Secret 值**：`INVEST_DB_URL=mysql+pymysql://用户:密码@RDS外网地址:3306/invest?charset=utf8mb4`
4. **首次回填建议在本地机器跑**（已白名单 minitick），直接写 RDS，避免 Actions 单次时长限制：
   ```bash
   export INVEST_DB_URL='mysql+pymysql://用户:密码@RDS外网:3306/invest?charset=utf8mb4'
   export TUSHARE_TOKEN=... TUSHARE_HTTP_URL=https://ts.gyzcloud.top/api
   python scripts/run_pipeline.py --mode update --start 20190101
   python scripts/run_pipeline.py --mode all
   ```
   之后每日增量交给 GitHub Actions。`current_holding` 与投顾信号同样写入这个 `invest` 库。
