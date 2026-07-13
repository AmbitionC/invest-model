# CLAUDE.md — invest-model（A股量化/投顾主导投资系统·核心仓）

Python 模型/回测/风控/FaaS 定时任务代码。投资系统三仓之一：
- **invest-model**（本仓）：因子模型、回测、风控规则、操作计划生成、FaaS 定时函数源码。
- **fe-journey-faas**（Node/Midway）：阿里云 FC，跑 API + invest 定时函数（打包本仓 faas/）。
- **invest-journey**（前端）：读 fe-journey-faas 的 `/invest/*` 接口。

## 系统定位（别改错方向）

**投顾主导 + 模型参谋**：标的由投顾定，ic_v1 模型只做质量分/时机/风控参谋，**不选股**。
风控规则是纯函数+单调状态机（`invest_model/portfolio/risk.py`），回测与实盘共用同一套判定。

## 投顾研报入库 → 计划重算（重要固定动作）

**用户每次更新投顾研报/早盘/午盘/收盘/周末投研笔记，流程固定：**
1. 按 `docs/advisor_extraction_prompt.md` 提炼为 CSV：
   - 主题 → `config/advisor_theme_YYYYMMDD_<slot>.csv`（列：rec_date,theme,source_type,direction,thesis,valid_until）
   - 个股 → `config/advisor_signals_YYYYMMDD_<slot>.csv`（列：rec_date,code,source_type,grade,direction,catalyst,valid_until,source）
   - slot：open/noon/close/weekend；source_type：intraday（盘中）/research（研报周末）
   - **thesis 长文本加双引号；文本内只用中文标点，严禁裸英文逗号**（防 CSV 解析错）；用 python csv 模块验证列数
   - 观点演化/分歧如实两方并记（如 0713 HBM long 与 0712 涨价见顶 reduce 并存），不替用户调和；direction 冲突信号在合成时相互抵消权重
2. commit + push master（`config/advisor_*.csv` push 自动触发 `ingest-advisor` workflow 增量入库）
3. **确认 ingest-advisor run success 后，立即 bump `ops/plan-notify.trigger` 触发计划重算——不等当天 17:00 例行 FC 计划。** plan-notify.yml 跑 `build_action_plan.py` 从库里重算三段式计划并追加到 issue #9（周末不发，有决策日去重）。
4. 计划出来后向用户汇报：新信号如何影响持仓与观察池、方向变化、以及对当前仓位的操作节奏建议。

## 触发机制（workflow_dispatch 常 403，一律用 trigger 文件 push master）

| 动作 | 触发 |
|---|---|
| 投顾信号入库 | push `config/advisor_*.csv`（自动） |
| 重出当日计划 | bump `ops/plan-notify.trigger` |
| 复盘 | `ops/review.trigger` ｜ 记分卡 `ops/signal-scorecard.trigger` |
| 模型重建 | `ops/build-model.trigger` ｜ 恐慌回填 `ops/fear-backfill.trigger` |
| 指数回填 | `ops/index-backfill.trigger` ｜ 验证 harness `ops/validation.trigger` |
| FC 手动调用（daily_update_plan 等） | fe-journey-faas `ops/invoke.trigger` |
| invest-model 改动生效到线上 FC | bump fe-journey-faas `.deploy-nudge`（PR merge master 触发 deploy.yml 按 ref=master 打包本仓 faas/） |

- 定时调度已迁阿里云 FC（`faas/`，fe-journey-faas s.yaml 的 invest-scheduler/invest-live-watch）；Actions schedule 实测延迟 2.8~5.8h 已弃用。
- Issue 归口：#8 复盘 · #9 每日计划 · #11 FaaS 告警 · #14 验证 · #3 盯盘预警。

## 治理（改模型/风控参数走这套）

提案（P1-P15，`docs/model_change_proposals.md`）→ 预登记验证（E1-E11，`scripts/validation/`，判据写死勿改）→ 影子 → 晋升。
三层归因/风控口径见 `docs/rulebook.md`。默认关的开关（P10 已开、P12/时间止损默认关等）晋升前逐字保持现状。
E10（红利低波底仓）/E11（因子分族）2026-07 首跑：均未过预登记判据，维持现状（详见 docs/strategy_research_202607.md）。
E9（P12 MA60 无条件破位）2026-07 未过关 → **E9 v2 条件化（趋势市中）** 已登记待实现（P12 段）。

**博主文章 → 系统迭代评估（固定动作）**：用户每收录一篇人生导师/博主文章（life-teachers 归档），
除归档外都要**评估其方法论对本投资系统有无可迭代的提案价值**：能形式化为规则/因子/择时的，
登记为新提案（P 序号）+ 预登记验证（E 序号，判据先写死），走治理流程；只是理念印证/已有规则的
实例补强则记入知识库不新增提案。如实区分"可验证的迭代"与"读后感"，绝不因一篇文章就改生产。

## 提交约定

- git user.name=Claude / user.email=noreply@anthropic.com
- commit trailer 两行：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` + `Claude-Session: ...`
- 面向用户文案简体中文；**不在推送物（commit/PR/代码注释）里写模型标识**。
