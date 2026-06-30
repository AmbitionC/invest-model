# 投顾录入 + 日常盯盘：前置配置清单

本文汇总「投顾观点录入→更新模型」与「日常盯盘」两块能力所需的全部前置配置，
一处对齐。两块共享同一套地基：**一个装了真实 A 股数据的持久库 + Tushare 访问凭证**。

## 0. 共同地基（两块都要）

| 前置项 | 用途 | 怎么配 |
|---|---|---|
| `TUSHARE_TOKEN` | 拉真实行情/估值、盘中实时价(`rt_k`) | GitHub Secrets，或本地 `.env` |
| `TUSHARE_HTTP_URL` | 数据服务基地址（含 VIP 接口） | 一般填 `https://minitick.top/` |
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

观察/白名单清单已就绪：`config/watch_etf.txt`、`config/trailing_only.txt`。

```bash
python scripts/live_check.py --db <DB> --alert          # 盘中轮询，只报触发项
python scripts/build_action_plan.py --advisor-led --risk --out results/action_plan.md --db <DB>
```

## 3. 云端自动化（GitHub Actions，可选但推荐）

两个工作流已就绪：
- `.github/workflows/data-update.yml`：工作日 17:00 增量、周六 18:00 全量刷新。
- `.github/workflows/plan-notify.yml`：工作日 17:30 生成计划，追加到 GitHub Issue
  「📈 每日操作计划」评论 → 你收到邮件提醒。

**一次性配置**（仓库 Settings → Secrets and variables → Actions）：

| 类型 | 名称 | 值 |
|---|---|---|
| Secret | `INVEST_DB_URL` | `mysql+pymysql://user:pass@host:3306/invest?charset=utf8mb4` |
| Secret | `TUSHARE_TOKEN` | 你的 Tushare token |
| Secret | `TUSHARE_HTTP_URL` | `https://minitick.top/` |
| Variable(可选) | `ACCOUNT_CASH` | 账户现金，如 `54089` |
| Variable(可选) | `PLAN_ARGS` | 覆盖默认 `--advisor-led --risk --trend-filter --concentration medium --time-stop-days 8` |

注意：
- 生产 MySQL 需允许 GitHub 运行器访问（放行出口 IP，或自托管 runner）。
- `plan-notify` 依赖 `data-update` 已把当日数据/预测刷进 MySQL。
- 未配 Secrets 前定时任务只失败、无副作用。
- 配好后可在 Actions 页手动 `Run workflow` 跑一次 `data-update`(mode=all) 验证。
