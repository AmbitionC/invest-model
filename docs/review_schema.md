# 复盘机器可读格式（review JSON schema v1.0.0）

给 Claude Code / Codex 类 Agent 消费的复盘产物。与 issue #8 的 markdown **同批同源**产出
（`scripts/review.py` 一次运行同时渲染两版，杜绝数字漂移）。

## 位置与分发

- **固定入口**：`results/review/latest.json`（每次复盘覆盖）
- 历史序列：`results/review/review_<YYYYMMDD>_<period>.json`（git 历史即时间序列）
- 由 review.yml 在 Actions 跑完后 commit 回 master（`[skip ci]`），Agent 开仓即读、无需 DB 凭据

## Agent 读取规则（从零上下文回答「本周该校准什么」）

```
待办 = calibration_queue[state != "closed"]
     ∪ conclusions[status ∈ ("pending_owner", "recurring", "recurring_*")]
```

每条自带：`based_on`（指回 facts 的路径——**引用数字必须走 facts，不得引用结论内嵌文本**）、
`confidence`、`suggested_action.requires_owner`（true = 必须 owner 拍板，Agent 只能起草不能执行；
对应治理红线：交易执行、主动偏离改判、治理晋升签核、投顾笔记原始录入永远留人）。

## 顶层结构

| 键 | 含义 |
|---|---|
| `schema_version` | semver；破坏性口径变更 bump minor 并在 calibers 留痕 |
| `engine` | 产出脚本 / git sha / 模型版本——复现实验用 |
| `calibers` | **口径注册表**：每个指标的 `caliber` 字段引用此处 id；`not_comparable_before` = 口径断代日，禁止跨断代直接对比（2026-07-25 前复权修复即断代事件） |
| `data_quality` | `snapshot_coverage`（近10交易日快照覆盖与缺口日列表——缺口=执行对账盲窗）、`known_biases`（做T盲区/修订覆盖/无成交流水）、`audit_status`（最近一次取数审计结论） |
| `facts` | **只有数字与样本量，无判断**。`advisor.by_grade[]`（含 10 日固定窗口列）、`model`（月度多空价差+recent3）、`holdings`、`execution`（见下） |
| `conclusions` | 判断层。`kind`: `conclusion`（推断）/ `discipline_fact`（执行纪律事实）；每条 `based_on` 指回 facts |
| `calibration_queue` | 校准事项状态机：`waiting_data / waiting_impl / pending_owner / closed`，`gate` 写明裁决条件（对应 E 验证判据） |

## facts.execution（执行对账，口径 exec_recon）

- `orders[]`：逐指令对账。`status` ∈ `executed / partial / not_executed / reversed /
  cond_untriggered（挂单价未触及，豁免不进分母）/ pre_executed / corporate_action（送转窗跳过）/
  no_baseline / no_snapshot（无法对账——一等状态，绝不硬算）/ too_recent`；
  `delay_td`（快照缺口时 `delay_uncertain=true` 表示上界）；`nonexec_cost`（次日收盘近似口径，卖类负=拖着继续亏）；
  `condition_still_valid`（强风控条款未执行时，触发条件事后是否仍成立——告警仅在此为 true 时升级）
- `metrics`：`risk_exit_exec_rate`（分母排除豁免/无法对账/主动偏离）、`buy_fill_rate`（分母=条件已触发的买类）、
  `median_delay_td`、`cum_nonexec_cost_sell/buy`
- 哲学：**未执行≠违纪**。owner 主动偏离经 execution_ack 通道（P1，未实现前一律"待确认"滚动提示）留档即静默。

## 版本纪律

- 新增字段=patch；字段语义变化=minor + calibers 记 `breaking_since`；删除字段=major。
- Agent 侧防御：先读 `schema_version`，major 不匹配即停止解析并报告。
