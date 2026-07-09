# 改造计划与逻辑谬误审计（2026-07）

> 输入：`results/model_account_review_20260709.md`（模型与账户整体复盘）+ 本次对
> 模型/回测、风控/计划、复盘/统计三条链路的逐类审计（前视偏差、rank_pct 语义、
> 幸存者偏差、复权口径、峰值持久化、选择性复盘、多重比较等约 20 个谬误类别）。
> 每条发现均给出 文件:行 证据，可直接复核。批次 A 已随本文档修复入库；
> 批次 B/C/D 为待办，模型层变更仍走 `docs/model_change_proposals.md` 的
> 影子→验证→晋升流程。

## 零、对上一轮复盘的解读修正（诚实条款）

审计发现 **R1（白名单硬止损豁免失效）** 部分改写复盘结论：卫星化学在
`config/trailing_only.txt` 白名单中（用户意图=只按 MA20 移动止盈管理），盘后计划
连发 7 天的"硬止损清仓"指令**违反了这一配置**，盘中 live_watch 同期显示"持有"。
因此上轮反事实中"执行卫星化学止损可少亏 8,576 元"一笔，性质是"违反配置但事后
正确"——纪律问题（32 条指令 0 执行）依然成立，但该票的矛盾指令责任在系统，
不在执行。修复后盘后计划不再对白名单票发硬止损；**想要 -8% 兜底的票请从白名单
移除**——保护性变弱是白名单语义的固有代价，控制权在用户。

## 一、谬误审计清单

严重度：🔴 高（影响结论/资金）｜🟡 中｜🟢 低。状态：✅ 本轮已修（批次 A）｜
⏳ 列入批次 B/C/D。

### 链路一：模型 / 因子 / 回测

| # | 严重度 | 发现 | 证据 | 状态 |
|---|---|---|---|---|
| F1 | 🔴 | **复权断层（P11 只修一半）**：前复权 `qfq_close_hist` 唯一生产调用点是实盘计划（`orchestration/action_plan.py:84`）；因子动量/波动（`factors/loader.py:101-131`）、前瞻标签（`model/dataset.py:59-75`）、回测收益与风控（`backtest/cs_engine.py:85-94,155-165,347`）、趋势/择时/买点（`signals/trend.py:33`、`portfolio/market_timing.py:74`、`signals/buypoint.py:94`）、复盘（`scripts/review.py`）全用未复权价。后果：回测在除权日记幽灵收益/假止损（打破"回测=实盘"不变式）、dividend_yield 候选因子 IC 被除息跳空系统性压负、mom_120 几乎必然跨除权日被污染。因子/标签/复盘线**超出 P11 已登记遗留范围，是新发现** | adjust.py:1-9 自述 | ⏳ 批次 C（移动全部历史基线，须 A/B 配对验证） |
| F2 | 🔴 | ICIR 权重小样本不稳：`min_history=3` 即脱离等权，IC std 极小时权重爆表（clip±3 兜底），因子符号可逐期翻转→换手噪声 | `model/combiner.py:22,29-52` | ⏳ 批次 C |
| F3 | 🔴 | 幸存者偏差防线依赖表存在性：缺 `stock_namechange` 时静默回退按**现名**过滤 ST → 追溯污染历史 universe | `universe/builder.py:73-86`、`filters.py:14-23` | ⏳ 批次 C（硬校验） |
| F4 | 🟡 | 退市清算默认 `delist_recovery=1.0` 全额回收，乐观 | `cs_engine.py:44,307-333` | ⏳ 批次 C |
| F5 | 🟡 | Sharpe/Sortino 未扣无风险利率；beta 的 cov(ddof=1)/var(ddof=0) 自由度不一致 | `backtest/metrics.py:67,73,118-120` | ⏳ 批次 C |
| F6 | 🟡 | review 模型分档复盘同样未复权（同 F1） | `scripts/review.py` | ⏳ 随 F1 |

**已核查无异常**：调仓 `exec_lag=1` 无当日前视；财务 PIT 用公告日 + 540 天时效；
IC 滚动窗口严格 `< as_of` 无泄漏；**rank_pct 全链路方向一致**（predict 高分=高分位 →
constructor 降序 top-N → 计划"前 x%"换算正确）；回测先卖后买无隐性杠杆；恐慌闸/
下跌二分法与 rulebook 逐条一致。

### 链路二：风控 / 操作计划 / 盯盘

| # | 严重度 | 发现 | 证据 | 状态 |
|---|---|---|---|---|
| R1 | 🔴 | **白名单硬止损豁免失效**：trailing_only 没传给 `evaluate_holding`，盘后对白名单票照发硬止损，盘中却豁免——对同一票两套指令（实例：卫星化学 -8.47%） | `action_plan.py:375,387-391`、`risk.py:191`、`live_check.py:410,414` | ✅ 已修 |
| R2 | 🔴 | 账户级风控只提示不动作：总仓位超标/risk_off 仅文字 hint，从不生成按比例减仓行；rulebook 第四节标"生产"但执行层未落地；risk_off 判据是成本浮亏而非峰值回撤（代码自认近似） | `action_plan.py:531-533,600-601` | ⏳ 批次 B |
| R3 | 🔴 | 总权益漏计多入口（07-01 ETF 错报 41.9 万同源）：日更重估对停牌股按 0 估值；转债在手动快照口径内、自动重估口径外→净值锯齿 | `faas/jobs.py:245-253`、`ingest_holding_snapshot.py:72` | ✅ 已修（停牌回退+转债并入重估；"bond 入 current_holding"因盯盘会对转债误发硬止损而**有意不做**） |
| R4 | 🔴 | 指令无生命周期：未执行只 hint，(plan_date,code) 主键每天重发刷 ref_price | `action_plan.py:497-504` | ⏳ 批次 B（执行确认闭环） |
| R5 | 🟡 | 计划现金取 env `ACCOUNT_CASH` 默认 0，不读快照真实现金 | `faas/jobs.py:104` | ✅ 已修（env 优先，未配置读最新 account_snapshot） |
| R6 | 🟡 | 盘中盈利保护峰值用不复权 `MAX(close)`、MA 却前复权→除权后提前止盈（P11 遗留②盘中版） | `live_check.py:88-110` | ⏳ 批次 B |
| R7 | 🟡 | 买点检测未复权→观察池假破位/假突破（F1 同源） | `signals/buypoint.py:94-96` | ⏳ 批次 B |
| R8 | 🟡 | 快照空 entry_date 每次上传被重置为快照日→时间止损/盈利保护对 ETF 永不生效 | `ingest_holding_snapshot.py:77-79` | ✅ 已修（CSV 值 > 旧值 > 快照日三级回退） |
| R9 | 🟡 | 零现金仍产出硬买单，与 R4 叠加=永不可执行指令 | `action_plan.py:429` | ⏳ 批次 B（随 R2 一并处理） |
| R10 | 🟡 | 盘中盘后风控不同源（env LIVE_* vs RiskConfig），无"以谁为准"仲裁 | `live_check.py:830-838` | ⏳ 批次 B |
| L1 | 🟢 | ETF 盘中价源失败静默不盯（只有自检计数行） | `live_check.py:493` | ⏳ 批次 D |
| L2 | 🟢 | 快照缺现金行静默按 0，混日期 CSV 的 DELETE 只删首行日期 | `ingest_holding_snapshot.py:45-61` | ⏳ 批次 D |
| L3 | 🟢 | 减半折股 `int(sh//200)*100` 奇数手少卖、100 股减半得 0 | `live_check.py:452,521` | ✅ 已修（`_half_lot` 最近整手） |
| L4 | 🟢 | 同票较新 long 会盖掉较旧 exit（drop_duplicates keep=last），可能吞掉仍有效的逻辑止损信号 | `advisor_repo.py:37,48-53` | ⏳ 批次 B |

**已核查无异常**：P11 实盘止损基线口径正确（缺 adj 因子 fail-open 已自认）；风控
优先级 逻辑证伪>硬止损>移动止盈 正确；峰值/档位跨日回放稳定（非每日重置）；
目标权重 Σ≤gross 无超配；快照同日重传幂等。

### 链路三：复盘 / 验证 / 统计方法

| # | 严重度 | 发现 | 证据 | 状态 |
|---|---|---|---|---|
| S1 | 🔴 | 复盘入场价前视：review 与 E1/E2/E3/E6/E8 用首推日**当日**收盘入场（盘中/盘后信号不可成交价），scorecard 却用严格次日——同一系统两套口径、review 更乐观；e6 文档自称"严格 PIT"与实现不符 | `review.py:91,95`、`validation/common.py:110-113`、`build_signal_scorecard.py:102` | ✅ review 已修；E-harness ⏳ 批次 C |
| S2 | 🔴 | 只考核 long：reduce/avoid/exit 信号准确率从未复盘（选择性记账，只给建仓记功） | `review.py`、scorecard、e1/e2/e3/e8 全部 `direction='long'` | ⏳ 批次 B（卖出信号复盘表） |
| S3 | 🔴 | 忽略 valid_until 与后续 exit：一律"持有到 asof"，投顾自己喊撤的票也按拿到今天计——牛市虚增战绩、择时无从检验（e7 明明在用 valid_until，字段可用） | `review.py:70-86`、`build_signal_scorecard.py:106-110` | ⏳ 批次 B |
| S4 | 🔴 | review 结论无显著性/题材调整：仅凭均值符号断言"分级能力/模型区分力"（spread 可能 1-2 个观测）；E2 的护栏（聚类 t、固定窗、题材相对）未移植到用户可见层 | `review.py:125,161,173-175` | ⏳ 批次 B |
| S5 | 🟡 | 多重比较无校正：多桶取其一 \|t\|>2 即"过关"；E1-E8 同一 25 天数据十几个假设无 FDR | `e2_advisor.py:182-188` | ⏳ 批次 C |
| S6 | 🟡 | scorecard 持有期不等长直接平均："推得早"混淆"分级好"（E2 固定窗口正确，未沿用） | `build_signal_scorecard.py:106-114` | ⏳ 批次 C |
| S7 | 🟡 | 验证 harness 止损臂按触发日收盘成交、无跳空缺口滑点——高估止损降尾能力（乐观上界） | `e1_risk.py:76-77`、`e8:91,119-124` | ⏳ 批次 C |
| S8 | 🟢 | 恐慌指数涨跌停阈值 ±9.8% 漏 20%/5% 板 | `signals/fear.py:73` | ⏳ 批次 D |
| S9 | 🟢 | 仪表盘 current() 前复权 close × 原始 cost 的浮盈率与券商快照口径岔开（展示困惑，非结算错误） | fe-journey-faas `holding.ts:52` | ⏳ 批次 D |
| S10 | 🟢 | review 按 (code,grade) 分组，同票跨级重复计数 | `review.py:70-72` | ✅ 已修（按 code 首评去重） |

**已核查无异常**：恐慌指数用固定绝对阈值归一、回填逐日 trailing 重算——**无前视**
（这是曾担心的点，实现正确）；scorecard 基准日对齐正确；仪表盘与 Python 盈亏公式
逐字一致；account_snapshot 无二次重估；E2/E6 聚类稳健 t/固定窗/功效声明扎实诚实。

**总体判断**：E1-E8 验证层方法论明显强于给用户看的 review/scorecard 战绩层——
自我美化恰好集中在护栏最少的展示层。批次 B/C 的主线是**把 E2 的口径反向移植**
（严格次日入场、固定窗口、聚类稳健 t、valid_until 出场）。

## 二、改造批次

### 批次 A：本轮已修（每项独立 commit，可单独 revert）

1. R1 白名单硬止损豁免（`evaluate_holding` 增 `exempt_hard_stop`，默认 False 行为
   逐字不变，回测不传参基线零变化）＋ `tests/test_trailing_whitelist.py`
2. R8 快照 entry_date 三级回退 ＋ `tests/test_ingest_snapshot.py`
3. R3 日更重估：停牌回退最近有效收盘/快照价、转债市值并入 ＋ faas 测试
4. R5 计划现金：env 优先、缺省读最新快照 ＋ faas 测试
5. L3 减半折股 `_half_lot` 最近整手 ＋ 测试
6. S1/S10 review 去美化：严格次日入场、同票首评去重（**战绩数字会下修，属去美化**）
   ＋ `tests/test_review_advisor.py`

已知遗留（有意不修，记录在案）：白名单票破 MA20 时盘中"确认清仓" vs 盘后走
P10 缓冲的语义差；`build_action_plan` 的 equity 仍不含转债市值（权重分母略偏）。

### 批次 B：短期（1-2 周，影子/可回退，单独 PR）

- R2+R9 账户级风控落地为动作：risk_off / 总仓位超标 → 按超出比例生成非白名单
  持仓的减仓行；零现金时新开仓降级为观察（或标注资金来源）
- R4 指令生命周期：下发→执行确认→未执行 N 日升级强提醒（roadmap"执行确认闭环"）
- S2/S3 卖出信号复盘表 + valid_until/后续 exit 出场口径
- S4 review 显著性护栏（复用 `validation/common.cluster_robust_tstat` + 短样本标注）
- R6/R7 盘中峰值与买点检测前复权（P11 遗留的盘中/观察池部分）
- R10 盘中盘后共用 RiskConfig，约定盘后 EOD 为权威；L4 exit 信号去重语义修复

### 批次 C：中期（移动回测基线，须 A/B 配对验证后切换）

- F1/F6 全链路前复权统一（因子/标签/回测/复盘；或回测日收益改用 pct_chg）——
  **本清单里对回测可信度影响最大的单点**
- F3 幸存者偏差硬校验（缺 namechange/退市覆盖 → 禁止标"可信回测"）
- F2 ICIR 稳定化（min_history≥6 + 收缩）；F4 退市回收保守化；F5 指标扣 rf/自由度
- S5 多重比较 BH-FDR 或预登记主桶；S6 scorecard 固定窗口化；S7 止损臂次日开盘成交
- E8 已过关的 P10 语义持续观察；cs_pf_v2 满 4 期后按流程晋升

### 批次 D：流程/运维

- 持仓快照自动化（OCR/券商导出）＋ 断更 N 日升级告警；L2 缺现金行告警
- 计划头部指标自检：与上一快照偏离 >X% 标"数据存疑"（07-01 类事故的兜底）
- 行业集中度从提示升级为计划动作（E7 拥挤预警连发两期无人理会）
- minitick 超时重试/备源；L1 ETF 价源失败的显式盲区提示
- S9 仪表盘与快照价源统一（fe-journey-faas）

## 三、纪律面（改造之外，复盘已证的行为问题）

代码修复解决不了执行率 0% 的问题。最低要求：每天收盘后按计划执行风控指令
（或明确记录"不执行原因"）；化工链降到行业 35% 上限内；保留 20-30% 现金 sleeve；
每日快照恢复上传（07-03 后断更，执行对账失去事实源）。

## 四、验证记录（批次 A）

- `pytest tests/ -q` 全绿（含新增 4 个测试文件/用例组）
- sqlite 端到端冒烟：合成数据建库 → 快照 CSV 连录两次（entry_date/口径）→
  `build_action_plan --risk --advisor-led`（白名单前后对比）→ 直调
  `_persist_account_snapshot_daily`（停牌回退+转债并入）
- faas GitHub 推送路径全部 monkeypatch，不打真实 API
