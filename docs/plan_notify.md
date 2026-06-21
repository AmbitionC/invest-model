# 每日操作计划 · 自动生成 + 通知

每个交易日盘后，定时任务在【生产 MySQL】上生成三段式操作计划（建议买入 / 持仓风控 / 观察池），
并以 **GitHub Issue 评论**的形式推送——你会收到 **GitHub 邮件**提醒。

- 扫描频率：**交易日每天 1 次**（买点基于日线收盘，盘后判定，手册即"盘后做计划"）。
- 产出时点：盘后数据更新后，约 **北京时间 17:30**（工作日）。
- 通知形式：GitHub Issue「📈 每日操作计划」按天追加评论 → 每天一封邮件。

## 一次性配置（在能连到 MySQL 的环境）

1. **配 Secrets**（仓库 Settings → Secrets and variables → Actions）：
   - `INVEST_DB_URL` = `mysql+pymysql://user:pass@host:3306/invest?charset=utf8mb4`
   - `TUSHARE_TOKEN`、`TUSHARE_HTTP_URL`（数据更新用，见 docs/scheduled_update.md）
2. **配 Variables**（可选）：
   - `ACCOUNT_CASH` = 你的账户现金（如 `54089`）；现金变动时改这里。
   - `PLAN_ARGS`（可选）覆盖默认 `--advisor-led --risk --trend-filter --concentration medium --time-stop-days 8`
3. **迁移你的数据到 MySQL**（持仓 + 投顾信号）：
   ```bash
   export INVEST_DB_URL='mysql+pymysql://user:pass@host:3306/invest?charset=utf8mb4'
   python scripts/migrate_user_data.py --src sqlite:///./data/real.db
   ```
   （或用 `scripts/ingest_advisor.py` / `scripts/ingest_holding.py` 配合 `config/*.csv` 重新录入）
4. **首次回填**：在 MySQL 上跑一次全量，生成 universe/因子/预测：
   ```bash
   python scripts/run_pipeline.py --mode all
   ```
5. 之后两条定时任务自动跑：
   - `.github/workflows/data-update.yml`：盘后更新数据（工作日 17:00）
   - `.github/workflows/plan-notify.yml`：盘后生成计划并发 Issue（工作日 17:30）

## 注意
- 生产 MySQL 需允许 GitHub Actions 运行器访问（放行出口 IP，或用自托管 runner）。
- 持仓变动（买卖/新建仓）后，记得更新 MySQL 的 `current_holding`（含 `entry_date`，时间止损/移动止盈才准）。
- 未配 Secrets 前，定时任务会失败（仅失败，无副作用）。
