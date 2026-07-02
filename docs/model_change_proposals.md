# 模型层变更提案（待决策）

> 约定：数据层增量直接实施；**模型层大变动先列收益/风险/观测方案，经确认后再动**。
> 本文档即该清单。所有提案共用同一套试运行与回退机制（见文末「通用观测与回退」）。

---

## P1. 候选因子晋升：nb_ratio_chg_20（北向持股变化）

**改动**：把影子观察中的北向因子从 `CANDIDATE_DIRECTION` 移入 `FACTOR_DIRECTION`，
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

**改动**：CSRanker 的标签从截面 zscore 收益改为截面**分位数**（0~1 rank），
目标函数改 `reg:quantileerror` 或换 LightGBM `lambdarank`。

| | 说明 |
|---|---|
| 预期收益 | 选股本质是排序问题；rank 目标对 A 股肥尾/涨停板极值天然稳健，文献中 rank 目标较原值回归的截面 IC 常提升 5~15% |
| 风险 | ① 引入新依赖（LightGBM 路线）；② 超参重新调整期模型不稳定；③ 与 P2 叠加实施时归因困难——**建议 P2 观察 ≥8 期后再动 P3** |
| 观测指标 | 同 P2 的 version 隔离对比（`rankq_v1`）；重点看 cv_hit_rate（月度 IC>0 占比）是否 ≥ 现 ranker |
| 回退 | version 切回 |
| 工作量 | 中（改标签小，LightGBM 路线加依赖+调参） |

## P4. 组合层：波动率倒数加权 + 换手惩罚

**改动**：`build_targets` 新增 `scheme="inv_vol"`（rank 权重 × 20 日波动倒数），
并加换手带宽：新目标与现持仓差 < 带宽的票不动（现引擎已有 min_trade=1%，
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
`pf_v2`（`--scheme inv_vol --hold-buffer 1.5`）对照回测。**看结果**：
`SELECT name, metrics FROM backtest_run ORDER BY run_id DESC LIMIT 2`（cs_ic_v1
vs cs_pf_v2 两行），或 build-model 日志末尾的两段回测指标。**晋升条件**：
连续 4 期（约 4 个月的周六重建）pf_v2 的 turnover_total 显著更低且
annual_return/sharpe 不差于 ic_v1 → 把默认 PortfolioConfig 切到
inv_vol + hold_buffer=1.5（一处配置）。**回退**：影子版本不影响生产，无需回退。

## P5. 分域/regime 建模（低优先，暂不建议）

按市值/行业分域训练，或加 regime 特征。**样本量不足**（月频×每期约 2000 票，
分域后单域样本过薄），当前阶段过拟合风险大于预期收益，建议永远排在 P2~P4 验证完之后。

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
2. **model_registry**：每次 train 记录 cv_ic_mean / cv_ic_ir / cv_hit_rate，
   跨 version 直接 SQL 对比。
3. **health 段**（results/latest.json + report 日志）：recent_factor_ic、
   candidate_factor_ic、candidate_ic_periods、universe 覆盖告警。
4. **周六复盘 issue**：投顾分级胜率、模型分档价差、持仓归因——模型变更后的
   实盘退化会在这里最先显形。
5. **红线（任一触发即回退）**：连续 3 期合成 IC < 0 且低于旧版本同期；或实盘
   月度跑输旧版本影子组合 > 3%；或回测 MaxDD 较旧版本恶化 > 5 个百分点。
