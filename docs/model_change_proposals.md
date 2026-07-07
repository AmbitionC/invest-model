# 模型层变更提案（待决策）

> 约定：数据层增量直接实施；**模型层大变动先列收益/风险/观测方案，经确认后再动**。
> 本文档即该清单。所有提案共用同一套试运行与回退机制（见文末「通用观测与回退」）。

---

## P1. 候选因子晋升：nb_ratio_chg_20（北向持股变化）

**改动**：把影子观察中的北向因子从 `CANDIDATE_DIRECTION` 移入 `FACTOR_DIRECTION`,
参与 IC 加权合成打分。一行代码，但改变所有后续打分与组合。

| | 说明 |
|---|---|
| 预期收益 | 外资流向在 A 股有较充分的有效性研究（信息优势+资金推动双重逻辑）；与现有价量/基本面因子相关性低，主要收益来自**分散化**而非单因子暴涨 |
| 风险 | ① 数据依赖港交所披露口径，历史上披露规则变过（2024 年北向实时额度信息停发），再变则因子断供；② 拥挤交易：外资重仓白马波动时因子回撤集中；③ 覆盖面只有陆股通标的（~2500 只），非覆盖票暴露为 0（中性），实际是隐性的「陆股通成分偏置」 |
| **晋升门槛（建议）** | 影子 IC 攒满 **≥12 期**且：\|rank-IC 均值\| ≥ 0.02、IC_IR ≥ 0.3、与现有各因子暴露截面相关性 < 0.7 |
| 观测指标 | `results/latest.json → health.candidate_factor_ic / candidate_ic_periods`（已上线，每次回测/report 自动输出）；周六复盘 issue 的模型段 |
| 回退 | 移回 CANDIDATE_DIRECTION 一行即回退；已落库暴露/IC 无需清理 |

**状态：影子观察已上线（本次提交），无需决策；晋升时再决策。**

## P2. 组合预测：IC 合成 ⊕ XGBoost ranker 取平均

**改动**：新增 `model_kind="ensemble"`，对两个已有引擎的截面分（各自 zscore 后）
取等权平均。不新增模型，只做组合。

| | 说明 |
|---|---|
| 预期收益 | 文献一致结论：组合预测几乎总优于单模型（降低单引擎的期间性失灵）。预期 IC_IR 提升 10~25%，单期 IC 未必更高但更稳 |
| 风险 | ① ranker 历史不足时（前 6 期）自动只剩 IC 合成，行为不变但需知晓；② 两引擎高度相关时（常见，相关性 0.7+）提升有限，白折腾；③ 可解释性下降：单票入选理由从「哪些因子好」变成「两个分的平均」 |
| 观测指标 | 用 `version=ens_v1` 影子跑全量 predict+backtest，对比 `model_registry` 里 ic_v1 / ranker_v1 / ens_v1 三行的 cv_ic_mean、cv_ic_ir、回测 alpha/Sharpe/MaxDD；实盘切换后看周度复盘「模型因子复盘」段的分档价差是否退化 |
| 回退 | LoopConfig.version 与 model_kind 切回 ic_v1 即回退，历史预测按 version 隔离互不污染 |
| 工作量 | 小（~50 行 + 测试） |

## P3. ranker 目标改分位回归 + 可选 LightGBM

**改动**：CSRanker 的标签从截面 zscore 收益改为截面**分位数**（0~1 rank）,
目标函数改 `reg:quantileerror` 或换 LightGBM `lambdarank`。

| | 说明 |
|---|---|
| 预期收益 | 选股本质是排序问题；rank 目标对 A 股肥尾/涨停板极值天然稳健，文献中 rank 目标较原值回归的截面 IC 常提升 5~15% |
| 风险 | ① 引入新依赖（LightGBM 路线）；② 超参重新调整期模型不稳定；③ 与 P2 叠加实施时归因困难——**建议 P2 观察 ≥8 期后再动 P3** |
| 观测指标 | 同 P2 的 version 隔离对比（`rankq_v1`）；重点看 cv_hit_rate（月度 IC>0 占比）是否 ≥ 现 ranker |
| 回退 | version 切回 |
| 工作量 | 中（改标签小，LightGBM 路线加依赖+调参） |

## P4. 组合层：波动率倒数加权 + 换手惩罚

**改动**：`build_targets` 新增 `scheme="inv_vol"`（rank 权重 × 20 日波动倒数）,
并加换手带宽：新目标与现持仓差 < 带宽的票不动（现引擎已有 min_trade=1%,
此处指**组合生成层**的持仓惯性，如「已持有且排名仍在 top 1.5×N 内则保留」）。

| | 说明 |
|---|---|
| 预期收益 | 月频调仓下**换手惩罚对净收益的改善通常最立竿见影**（现回测单边年换手约 6~8 倍，砍 1/3 换手 ≈ 直接加 0.5~1% 年化净收益）；波动倒数加权降组合波动、提 Sharpe |
| 风险 | ① 持仓惯性使组合对因子信号钝化，牛市换挡期可能少赚；② 波动倒数加权系统性超配低波蓝筹，与 small_size 因子对冲，需看合成效果；③ 回测与实盘计划的「持有惯性」口径趋同（这其实是收益：两套口径靠拢） |
| 观测指标 | 回测 metrics 的 turnover_total（预期降 30%+）、扣费后 annual_return、Sharpe；对照跑 rank_weight vs inv_vol 两个 version 的回测行 |
| 回退 | scheme 配置项切回 rank_weight |
| 工作量 | 中（~80 行 + 回测对照） |

**状态：已实施为影子版本（2026-07-02 批准）。** 生产默认行为不变
（`scheme=rank_weight, hold_buffer=0`）；build-model 工作流在主建模后自动跑
`pf_v2`（`--scheme inv_vol --hold-buffer 1.5`）对照回测。

**2026-07-04 实测（E5 晋升检查器读 backtest_run，3 次周度重建一致）**：
基线 `cs_ic_v1` annual +11.75% / sharpe +0.66 / MaxDD 16.0% / turnover 20.9;
影子 `cs_pf_v2` annual **+14.38%** / sharpe **+0.82** / MaxDD 15.9% / turnover 20.5。
→ **实际净收益不来自换手（仅 −2%），而来自 inv_vol 的 Sharpe 提升（Δ+0.16 / 年化 Δ+2.6pp）,
且 MaxDD 不恶化、3/3 期一致**。晋升基准据此从「换手↓」修正为「Sharpe 路径」。

**✅ 已晋升为生产默认（2026-07-04，用户确认「验证 ok 直接生效」）**：
把默认 `PortfolioConfig` 切 `scheme=inv_vol, hold_buffer=1.5`（`portfolio/constructor.py`）,
同步 `run_pipeline.py` argparse 默认与 `config/config.yaml`。实盘 `build_action_plan` 无 --scheme
参数、走 `loop._build_targets`（scheme=inv_vol 时自动算 vol_map），故此默认即**实盘权重口径**。
- **晋升依据**：`cs_pf_v2` 3 次周度重建一致优于 `cs_ic_v1`（Sharpe Δ+0.17、年化 Δ+2.6pp、
  MaxDD 不恶化、换手持平；E5 裁决见 issue #14）。
- **上线前验证**：77 单测全过（含端到端 pipeline smoke）× 新默认；管线复现无异常。
- **回退**：改回 `scheme=rank_weight, hold_buffer=0.0` 一处即回退（version 隔离，随时可退）。
- **持续监控红线**：inv_vol 系统性超配低波/大盘，优势可能部分是近 18 月低波 regime;
  E5 继续每周对比，若后续 Sharpe 转劣于旧口径或 MaxDD 恶化 >5pp → 触红线回退。

## P5. 分域/regime 建模（低优先，暂不建议）

按市值/行业分域训练，或加 regime 特征。**样本量不足**（月频×每期约 2000 票,
分域后单域样本过薄），当前阶段过拟合风险大于预期收益，建议永远排在 P2~P4 验证完之后。

---

## P6. 套利模块（引擎 A 防守 carry + 三水表 overlay + 盲区 α + 统一资金账本）

**改动**：新增子包 `invest_model/arb/`，把文档《套利方案 v2》落地为与现有截面多因子
选股系统（引擎 B / 交易）同属一个资金池的一体两面：引擎 A 防守 carry（国债逆回购 /
红利 / 可转债双低 / 恐慌弹药）、三水表（信贷/财政/政策资本）倾斜引擎 B 并生成盲区 α,
统一资金账本按 A(50-60%)/B(30-40%)/α(5-15%) 分配、**机器校验零杠杆**（`ledger_invariant`,
Σ≤100%）。数据层：逆回购/红利/可转债走 Tushare 自动化，三水表走人工 curated CSV
（`scripts/ingest_watermeter.py`，同投顾信号约定）。

| 项 | 说明 |
|---|---|
| 版本族 | `arb_repo_v1 / arb_div_v1 / arb_cb_v1 / arb_alpha_v1 / arb_flow_v1 / arb_ledger_v1`，与 `cs_ic_v1` 版本隔离并行 |
| 影子并行 | 水表倾斜乘子默认 1.0、盲区 α 权重默认 0（同 `CANDIDATE_FACTORS`），攒 **≥12 期** flow_score 影子 IC（health 段 `arb.watermeter_ic_periods`）方可晋升；carry 走 `--mode arb` 影子净值；`build_action_plan` 在 `ARB_ENABLED=0` 时把套利段渲染为「观察态·未动用资金」 |
| 晋升门槛（建议） | 水表影子 IC ≥12 期且 \|rank-IC\| ≥ 0.02、IC_IR ≥ 0.3；carry 回测实现 ≈ 预期（`review_arb` 段对账）；盲区 α 命中率为正 |
| 一键回退 | 主开关 `ARB_ENABLED=0`（默认）→ 计划等同今天的纯引擎 B（逐字一致）；水表倾斜 `ARB_WATERTILT=0`；逐 sleeve 开关在 `ArbConfig`。回退无需清理数据（按 version/sleeve 隔离） |
| 数据降级 | `cb_*`/逆回购 缺权限即 `logger.warning` 跳过，`build_action_plan` 用 `table_exists` 检查，缺表则该 sleeve 预算划入**现金（绝不加杠杆）**并加 risk_hint。任何 sleeve 缺失都不破坏零杠杆不变式（最坏=今天行为） |
| 观测指标 | health 段 `arb`（watermeter_ic_periods / alpha_falsified / ledger_ok）；周六复盘第五段 `review_arb`（sleeve 净值 / carry 实现vs预期 / α 证伪 / 水表兑现）；`arb_scorecard` 记分卡；盯盘 `watch_alert` 新增 CARRY/WATER/ALPHA 三类 |
| 红线（沿用文末） | 全程自有资金零杠杆（机器校验）；跟水不跟价——逻辑止损（`carry_logic_stop`，水表反转即离场）；盲区 α 必过证伪（剥离股价·只看产业侧资金）；连续 3 期实盘跑输 >3% / MaxDD 恶化 >5pp → 回退 |

**状态：影子/观察态已上线（本次提交），`ARB_ENABLED=0` 默认不动用资金、不改变生产计划；晋升时再决策。**

---

## P7. 财务排雷负面清单：影子 → universe 硬过滤

**改动**：`invest_model/universe/quality_screen.py` 的 7 规则红旗打分器（应收/商誉/
毛利率超行业/三费骤降/归母异常/利润-现金背离/免税放大器）从影子记录晋升为
universe 负面过滤（红旗 ≥3 剔除）+ 投顾信号自动降级提示。

方法论出处：重远投资观 财报#3/#4/#7；life-teachers 验证
`profit-vs-cash-and-fraud-screen`（干净 0 旗/问题 8 旗清晰分开）。

| | 说明 |
|---|---|
| 预期收益 | 尾部风险防御（暴雷排除），非 alpha 增强——"宁错杀不放过"只对负面清单成立；A 股财务暴雷（乐视/獐子岛/康得新型）单票 -50%+ 的左尾被系统性剪除 |
| 风险 | ① 误杀：红旗是"触发深挖"非"确认造假"，行业阈值校准不足时会错杀合法高应收行业（建筑/军工）；② universe 缩小影响因子截面与回测可比性；③ 快报/年报披露节奏导致红旗滞后 |
| **晋升门槛（建议）** | 影子攒 ≥8 期后：红旗 ≥3 组（等权）显著跑输 universe 均值（月均 < -1%）且组合内命中率 >0，方可晋升；晋升时先只剔红旗 ≥3、观察 4 期再讨论收紧到 ≥2 |
| 观测指标 | `results/latest.json → health.quality_screen`（红旗分布/组合命中数，已上线）；操作计划 risk_hint（持仓 ≥2 旗提示深挖）；建议补一个"红旗组 vs 干净组"分组收益脚本再裁决 |
| 回退 | 影子态本来就不动仓；晋升后回退=过滤开关一处 |

**状态：影子已上线（本次提交），每调仓日全 universe 打分落 `quality_flag`，不剔除任何票。**

## P8. 财报方法论候选因子组：growth_accel / bp_ex_goodwill / dividend_yield / insider_conviction

**改动**：四个候选因子已进 `CANDIDATE_DIRECTION` 影子观察（同 P1 机制：算暴露、
落库、记 IC，不参与打分）。晋升=逐个移入 `FACTOR_DIRECTION`。

| 因子 | 方法论出处（life-teachers 验证） | 数据依赖 |
|---|---|---|
| growth_accel 增速二阶导 | 财报#1"看增速二阶导"；`growth-deceleration-davis-killer`（失速→PE 重估跌 78%） | fina_indicator q_netprofit_yoy（VIP） |
| bp_ex_goodwill 扣商誉 BP | 财报#7"扣商誉重算"；`goodwill-strip-revaluation`（PB 1.2→3.0） | balancesheet_vip（新增采集） |
| dividend_yield 股息率 | 投资篇#2 分红=稳健者核心回报；`bank-stock-yield-and-value-trap` | daily_basic dv_ttm（已有） |
| insider_conviction 高管增持 | 跟庄篇；`insider-buying-signal-strength`（可信度=押注÷身家，高管≈20×） | stk_holdertrade（新增采集） |

| | 说明 |
|---|---|
| 预期收益 | growth_accel 与动量/成长相关但含前瞻信息（盈利动量文献扎实）；bp_ex_goodwill 修正 bp 的虚资产失真；dividend_yield 补红利风格暴露（现因子库无）；insider_conviction 信息面独立、与价量正交 |
| 风险 | ① growth_accel 对财报披露节奏敏感（季度更新，月频调仓下 2/3 时间是陈值）；② insider_conviction 覆盖稀疏（多数票无增减持记录，NaN=中性，同 adv_stance 的覆盖面偏置）；③ bp_ex_goodwill 与 bp 相关性预计 >0.9，晋升需先看增量（若相关 ≥0.95 建议直接**替换** bp 而非并列）；④ dividend_yield 与 ep 高相关，红利风格拥挤期回撤集中 |
| **晋升门槛** | 同 P1：≥12 期、\|rank-IC\|≥0.02、IC_IR≥0.3、与现有因子相关 <0.7（bp_ex_goodwill 例外：按"替换 bp 后合成 IC 不降"裁决） |
| 观测指标 | health.candidate_factor_ic / candidate_ic_periods（已自动输出）；周六复盘模型段 |
| 回退 | 候选态零影响（已实证：删候选暴露重算打分逐字一致）；晋升后移回一行即回退 |

**状态：影子已上线（本次提交），无需决策；晋升时逐因子再决策。**

## P9. 下跌二分法闸：恐慌抄底放松只给基本面未走坏的票

**改动**：`signals/buypoint.py` 恐慌放松分支（fear≥75 时环境闸 60%→40%）叠加
基本面闸——最近一期单季+累计净利同比双负（"连续负增长"的可取数近似）的票
**不享受放松**，维持原环境闸。`fear_fundamental_gate=True` 默认开启。

方法论出处：投资篇#3《为什么亏钱受伤的总是散户》——下跌分估值驱动（系统性
恐慌，可用宽基/优质票抄底等修复）与业绩驱动（保险/中概式基本面走坏，万不可
抄底）；不知谁错先怀疑自己错、去财报找原因。

| | 说明 |
|---|---|
| 预期收益 | 防"接飞刀"：恐慌窗口里技术买点常在业绩走坏票上先出现（跌得深反弹形态好），此闸把 fear 特例限定回它的本意——只抄"估值驱动的错杀" |
| 风险 | ① 财报滞后：增速刚转正的反转票会被上一期双负挡住（错过左侧第一波——与"确认增速恢复才买"的原则一致，属接受的代价）；② 数据缺失 fail-open（无 q_profit_yoy 时不拦，行为=今天） |
| 为何默认开启 | 只收紧 fear 特例分支（正常路径逐字不变）；最坏影响=恐慌窗口少抄一次底，方向与"确定性>收益率"一致 |
| 观测指标 | 观察池理由文案"业绩驱动下跌禁抄"出现频次；恐慌窗口（fear≥75 的调仓日）被拦票 vs 放行票的后续 20 日收益对比 |
| 回退 | `BuyPointConfig(fear_fundamental_gate=False)` 一处 |

**状态：已实施默认开启（本次提交）。**

## P10. MA20 止损的新仓/未盈利缓冲：消除「回踩买点 = 破位止损」的自打架

**改动**：`risk.evaluate_holding` 第 3 步 `ma_trailing` 的「破MA20清仓」，对**未盈利新仓**
加缓冲（宽限 N 日 / 幅度阈值 / 未盈利降级为减半），把 MA5/MA10 梯子（`replay_ladder_tier`）
已实现的「盈利后才收紧、否则洗掉刚启动的票」同款保护补到 MA20。硬止损 -8% 不变，真下跌即时兜底。

| | 说明 |
|---|---|
| 缺陷定位 | 买点=「回踩≈MA20 企稳放量」、止损=「收盘破MA20清仓」锚**同一条线**、零缓冲；MA5/10 梯子有「盈利后才收紧」保护，唯独 MA20 清仓对未盈利新仓无差别生效 → 回踩买入遇假跌破即被清（实例 2026-07-06 通信ETF 建仓 4 日、当日 +0.9% 仍判破MA20清） |
| 预期收益 | 降低「回踩买入→假跌破→止损→反弹」的误洗，减无谓换手/踏空；趋势中继回踩的健康仓给观察期 |
| 风险 | ① 缓冲=延后离场，真转弱多亏一点（硬止损 -8% 封顶）；② 宽限/阈值是新超参，须回测定；③ 与盈利保护/时间止损档位交互需回归逐字对齐 |
| **验证（E8 预登记）** | `scripts/validation/e8_ma20_buffer.py`：逐仓事件驱动，四臂 A 现状/B 宽限5日/C 幅度2%/D 未盈利减半；指标含逐仓净收益 + MA20 触发占比 + **误洗率**（被 MA20 清后若只留硬止损持有到期反多赚 >2% 的占比）。**过关判据**：某缓冲臂 vs A 均值不降（≥A-0.5pp）、大亏率不升、误洗率显著下降 |
| 数据/功效 | 入场集=投顾 long（历史短、功效弱，随累积增强）；可扩全 universe 回踩事件提功效（v2）。每周随 validation 重跑，回帖 issue #14 |
| 回退 | 缓冲为 `RiskConfig` 新开关（默认关=现状逐字一致）；晋升=开关置 True，一处切换、可回退 |

**状态：预登记 + E8 影子验证（本次提交），生产逻辑零改动；E8 过关且样本充分再决策实现。**

### P10.1 修正：P10 在实盘路径失效 + 「转盈即清」缺陷（复核修复）

P10 落地后复核发现三处缺陷（均已修复，回归 122 测试全绿）：

1. **实盘对新仓每日重复减半（分期清仓）**：`action_plan` 用 `replay_tier(hist[窗口切片])`
   重建 prev_tier，切片内前 19 行 MA20=NaN → 新仓（窗口 <20 行，恰是 P10 目标人群）
   破位永远记不上档、prev_tier 恒为 0 → 「首破减半」每天重新触发（1/2→1/4→1/8…）。
2. **实盘 tier 锁死 3、收复后转盈即被清**：窗口 ≥20 行时 `replay_tier` 首破即记档 3
   （不知道 P10 的减半档），此后哪怕收盘已站回 MA20 上方，一转盈就按档 3 判「破MA20清仓」。
3. **两路径共有的「转盈即清」**：盈利判据 `close/cost ≥ gate(0)` 是**水平**条件——缓冲仓
   在 MA20 下方恢复途中涨回成本价当天即被当「止盈」清仓，恢复行情一点吃不到，缓冲只把
   -2% 止损换成 0% 止损。且 E8 的 D 臂用的是 `peak/cost ≥ 15%`（事前验证的不是部署的规则）。

修复（`ma20_unprofit_trim` 开关关闭仍逐字恢复原行为）：
- `risk.replay_hold_tier`：P10 感知的档位回放——均线在整段序列上算（含预热）、只在窗口内
  推进档位、迁移规则与 `evaluate_holding` 同构；`action_plan` 改用之（修 1、2）。
- `evaluate_holding`：盈利仓破 MA20 清仓改为**新鲜破位**事件语义（上一收盘不在 MA20 下方、
  今日跌破才清）；缓冲仓线下转盈 → 持有，站回 MA20 后再破位才止盈（修 3）。趋势仓首次
  破位止盈、硬止损 -8% 兜底、逻辑证伪清仓均不变。
- `e8_ma20_buffer.py` D 臂改为逐字镜像生产语义（每周重跑验证的即部署规则）；
  `live_check` 增加 `prev_below`（前日已破位不重复给减半/清仓卖单，防盘中重复执行）。
- 回归：`tests/test_ma20_buffer.py` 新增实盘/回测双路径逐日一致性测试（含硬止损、
  收复再破位、开关关闭等 4 组序列）。

---

## 建议实施顺序与依据

```
P1 影子观察（已上线，零风险）
   ↓ 攒 12 期 IC（约 12 个月，可用历史回填加速：跑一次 --mode all 即补全历史 IC）
P4 组合层换手惩罚        ← 不动选股模型、净收益改善最确定，建议最先批
   ↓ 观察 4 期
P2 组合预测              ← 低风险、模型层第一步
   ↓ 观察 8 期
P3 rank 目标 / P1 晋升   ← 视 P2 期间攒的观测数据决策
```

## 通用观测与回退机制（已具备，无需新建）

1. **version 隔离**：所有预测/组合/回测按 `version` 字段落库，影子版本与生产版本
   并行共存，切换=改 LoopConfig.version 一处，回退同理。
2. **model_registry**：每次 train 记录 cv_ic_mean / cv_ic_ir / cv_hit_rate,
   跨 version 直接 SQL 对比。
3. **health 段**（results/latest.json + report 日志）：recent_factor_ic、
   candidate_factor_ic、candidate_ic_periods、universe 覆盖告警。
4. **周六复盘 issue**：投顾分级胜率、模型分档价差、持仓归因——模型变更后的
   实盘退化会在这里最先显形。
5. **红线（任一触发即回退）**：连续 3 期合成 IC < 0 且低于旧版本同期；或实盘
   月度跑输旧版本影子组合 > 3%；或回测 MaxDD 较旧版本恶化 > 5 个百分点。
