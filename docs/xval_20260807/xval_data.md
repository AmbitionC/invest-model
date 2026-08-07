# D 通道（数据层独立交叉验证）报告 — invest-model 2026-08-02 ~ 08-07

工作分支 `claude/invest-model-weekly-review-6t4ktf`（HEAD `6f26586`），对比基线 `85e19db`。
**全程只读**：未改任何仓内文件、未落库、未 commit。临时脚本在 `/tmp/xd/`。

> **数字来源标注约定**（本轮硬性要求）
> - **【自算】** = 我在 `/tmp/xd/` 下从零写代码算出来的，可复现。
> - **【文档】** = 从 CLAUDE.md / 代码注释 / manifest 抄的，**不构成证据**。
> 本报告的所有裁决性结论只建立在【自算】数字上。

---

## 一、校验面清单

### 1.1 实际验到的

| 对象 | 手段 | 深度 |
|---|---|---|
| `results/bias_meanrev/*.csv` + `manifest.json` | 用 `bias_meanrev_data.py` 重建到 `/tmp/xd/`，逐个 md5 对账 | 7/7 md5 **逐字节一致**【自算】 |
| 乖离率算式 `close/MA60−1` | 纯 python（`csv` + 手写滑动均值，不用 pandas、不 import 主线）复算 7 个指数全历史 | 与 manifest spot 点差 ≤ **1.1e−15**（ULP 级）【自算】 |
| 滚动排名（2500 交易日双尾名次） | 从零实现，与主线 `_index_bias_states` 逐指数比对 | **2/7 不一致**（见 F-A）【自算】 |
| `action_plan.py:_index_bias_states / _bias_extreme_hint / _persist_index_bias_daily` | stub repo（无 DB 增量）实跑，逐字段打印 | 全量 |
| `_broad_leg_states` 的 `bias60/bias_pct/bias_rank` 三列 | 代码走查 + 与 P70 口径对拍 | 全量 |
| 基底 CSV 完整性 | 排序/重复/区间内缺失交易日/年均交易日/2500 日真实跨度/右端参差 | 7 个 CSV 全量【自算】 |
| 价格数据外部常识对撞 | 各指数历史最高/最低收盘 + 日期，与我独立知道的市场史核对 | 见 §2 F-F |
| `macro_series` schema / `ingest_macro.py` / `signals/macro.py` / `macro_digest.py` / `tests/test_macro_layer.py` | 逐行走查 + 与 tushare 接口形态对拍 | 代码层全量，**数据层零覆盖**（无 DB、无网络） |
| 刷新链路 | `update.py` / `faas/jobs.py` / 全部 49 个 workflow 的触发条件 | 全量【自算】 |
| `e59_bias_rolling10y.py` 的排名实现 | 走查，确认是否有与生产同型的缺陷 | **干净**（见 F-A 影响面） |

### 1.2 **没验到的**（覆盖缺口，如实列）

- **全部 DB 内容**：`macro_series` / `index_bias_daily` / `broad_leg_state` / `index_daily` 里到底有什么行、覆盖到哪一天，本环境无凭据，一行都没看到。下文凡涉及都标 `[待DB确认]` 并附 SQL。
- **tushare 接口的真实返回列名与单次行数上限**：无网络。F-C / F-D 是从接口形态推的，须用 `--probe` 或 SQL 证实。
- **fe-journey-faas `/invest/bias` 与 `/invest/broad` 的 handler**：不在本 session 的仓里，只从 `invest-journey` 前端代码反推消费方式。
- **`broad_export_web.py` 导出的 `broadIndex.json` 实际内容**：产物是前端仓的静态文件，未比对。
- 08-02~08-07 改动共 96 个文件，其中约 55 个是 `scripts/analysis/*` **一次性研究脚本**。我只深查了与数据层直接相关的 6 个（`bias_meanrev_data/calib`、`e57`、`e59`、`bias_rank_extremes`、`broad_export_web`），**其余未审**。
- 已知既有结论 F-1~F-6（恐慌贪婪端失灵、中证1000 回溯段 45.3% 等）**按要求未重复确认**。

---

## 二、发现

### 🔴 F-A ｜ `[新发现]` ｜ 生产乖离率排名把「今天自己」多数了一次 —— 同一个量走了两条浮点路径

**⚠️ 只影响本层（P70/P39 均为提示-only），但它改写了发到邮件里的 🚨 判定，且改写方向是单边的。**

**现象。** `_index_bias_states`（`action_plan.py:464-474`）把当日乖离率算了**两遍**：

```python
ma   = float(c[-BIAS_MA:].mean())                                    # 路径 A
bias = c[-1] / ma - 1.0
b_all = (pd.Series(c) / pd.Series(c).rolling(BIAS_MA).mean() - 1.0).dropna()   # 路径 B
w = b_all.to_numpy(dtype=float)[-BIAS_WIN:]        # ← w[-1] 就是今天，但走的是路径 B
rank_low  = int((w < bias).sum()) + 1              # ← 拿 A 去比含 B 的窗口
rank_high = int((w > bias).sum()) + 1
```

A 与 B 数学上是同一个数，浮点上差 1~4 ULP（求和顺序不同）。因为**今天自己在窗口 `w` 里**，
`w[-1]` 与 `bias` 的这点差就决定了今天会不会被自己计入 `(w < bias)` / `(w > bias)`，
于是 `rank_low` 或 `rank_high` **恰好虚增 1**。

**可机检的不变量**：无并列时应有 `rank_low + rank_high == win + 1`。
主线在 20260731 对 创业板 与 中证红利 都给出 **2502**（应为 2501）【自算】。

**量级【自算】**（`/tmp/xd/d2_sweep.py`，7 指数 29,794 个可算日全历史复演）：

| | 主线 rank ≠ 自洽 rank | 前4🚨 误报 | 前4🚨 **漏报** |
|---|---|---|---|
| 合计 | **13,352 天（44.81%）** | **0** | **22** |

- 误报恒为 0、漏报恒为正，是有原因的：偏差只会把 `rank_low` 或 `rank_high` **往大了推**，
  ⟹ **读数只会显得比真实更不极端，绝不会更极端**。这是单边失效，不是随机噪声。
- 分母：全历史真·前 4 极值日（自洽口径）**411 天** ⟹ **漏报率 5.4%**【自算】。
- 22 天里 **只有 1 天在 2015 年之后**（科创50 20220415）⟹ 向前看咬人的频率低，但不是零。

**已经咬到的一次（文档级）**：CLAUDE.md「P70 落地·当前读数（20260731）」写的是
「创业板第 **13** 低」【文档】。我从零复算是 **第 12 低**【自算】；主线函数实跑也确实吐出 13
（`/tmp/xd/d1_mainline.py`），即那个 13 就是这个 bug 的产物。同批 中证红利 `rank_high`
主线 852 / 自洽 **851**【自算】。其余 5 腿一致。

**影响面。**
- `index_bias_daily.rank_low / rank_high / pct_low`（落库 → `/invest/bias` → 前端表格）。
- `_bias_extreme_hint` 的 🚨 触发判定 → issue #9 → **邮件**。
- `broad_leg_state.bias_rank / bias_pct`（`action_plan.py:361-369`）**同型同构**，同样受影响。
- **研究脚本不受影响**：`e59_bias_rolling10y.py:rolling_rank_signals` 的 `b[i]` 与窗口 `w`
  取自同一个数组，自洽；`e57` 的 `causal_topk` 用 `hist`（不含当日）也自洽。
  ⟹ **E56/E57/E58/E59/E60 的 FAIL 裁决不受本条影响。**

> 🔴 **值得单独记的元教训**：这个缺陷 V1/V2/V3/V4/RED 五个通道都不可能抓到——
> 它们重写的是**研究引擎**，而这个 bug 只存在于**生产读数函数**里。
> 「从零重写引擎」覆盖不到 `orchestration/` 的消费端。

**复现**
```bash
python /tmp/xd/d1_recompute.py      # 从零算（不 import 主线）
python /tmp/xd/d1_mainline.py       # 跑主线 _index_bias_states（stub repo）
python /tmp/xd/d1_rank_diag.py      # A/B/C 三条路径逐 ULP 拆解 + 越界那一天
python /tmp/xd/d2_sweep.py          # 全历史 29,794 天扫描（约 3 分钟）
python /tmp/xd/d2_denom.py          # 411 天分母
```

**建议动作**（一行修复，只改本层）
1. 删掉路径 A，`bias` 直接取 `float(b_all.iloc[-1])`，让读数与窗口同源。
2. 加断言/日志：`assert rank_low + rank_high == len(w) + 1 or 有并列`。
3. 顺手修 `_broad_leg_states` 同型代码。
4. 把 CLAUDE.md「创业板第 13 低」订正为 12（或注明重算后为准）。

---

### 🔴 F-B ｜ `[新发现]` ｜ 7 个 P70 指数里 5 个、4 条宽基腿里 3 条 **没有任何自动刷新路径**

**这条超出提示层，波及 P26/P27 的买卖闸读数。**

**现象。** 日更链路 `job_daily_update_plan` → `run_pipeline --mode update` → `update.py:169-174`：

```python
BENCHMARKS = ["000300.SH", "000905.SH", "000906.SH"]     # update.py:16
for code in BENCHMARKS:
    df = client.get_index_daily(code, start, end)
    repo.upsert("index_daily", df, ["code", "trade_date"])
```

`index_daily` 每天只增量 **这三个** 代码。而：

| 用途 | 需要的代码 | 日更覆盖？ |
|---|---|---|
| P70 七腿 | 000300.SH ✅ · 000905.SH ✅ · **399006.SZ / 000688.SH / 000922.CSI / 000016.SH / 000852.SH ❌** | 2/7 |
| P26/P27 四腿闸 | 沪深300 ✅ · **创业板 / 科创50 / 红利 ❌** | 1/4 |

其余代码只能靠 `ops/index-backfill.trigger`（`backfill_index.py` 的 `DEFAULT_CODES`）手动 bump。
**我逐个查了全部 49 个 workflow：只有 `us-update.yml` 有 `schedule:`**【自算】；
`index-backfill.yml` / `index-dump.yml` / `spread-median.yml` 全是 `workflow_dispatch + push trigger`。
`faas/jobs.py` 里也没有任何 index 回填 job。

⟹ **创业板腿最脆**：它的基底是 `spread_full_history.csv`（`spread-median.yml` 产出，手动触发），
DB 增量代码 `399006.SZ` 又不在 BENCHMARKS ⟹ **两条腿都不自动**。

**当前证据【自算】**（基底 CSV 右端，`/tmp/xd/d2_cover.py`）：

```
沪深300  20260729（滞后并集末日 2 个交易日）    创业板  20260728（滞后 3）
科创50   20260730（滞后 1）                     中证500 20260730（滞后 1）
中证红利/上证50/中证1000  20260731（滞后 0）
```

**放大它的是 F-C**：`_persist_index_bias_daily` 把 **`s["date"]` 直接丢掉**，
每行一律盖 `trade_date = 计划日 dt`。

**影响面。** 陈旧收盘会被当成当日收盘去 ①排名 ②判 🚨 ③过买卖闸（P26/P27）④落库⑤上网站上邮件，
且**没有任何 staleness 告警**（`_index_bias_states` 与 `_broad_leg_states` 都是裸 `except Exception: continue`，
单腿失败或数据陈旧一律静默）。

**`[待DB确认]`** — 陈旧程度取决于最后一次手动 backfill，跑这条即知：
```sql
SELECT code, MAX(trade_date) AS last_td, COUNT(*) AS n
FROM index_daily
WHERE code IN ('000300.SH','000905.SH','000906.SH','399006.SZ',
               '000688.SH','000922.CSI','000016.SH','000852.SH')
GROUP BY code ORDER BY last_td;
-- 期待：全部 = 最近一个交易日。若只有 000300/000905/000906 是新的 ⟹ F-B 坐实。

SELECT trade_date, code, close, bias60, rank_low
FROM index_bias_daily ORDER BY trade_date DESC, code LIMIT 30;
-- 若某 code 连续多日 close 完全不变而 trade_date 天天在走 ⟹ 陈旧被盖上今天的日期。
```

**建议动作**
1. `update.py` 的指数循环从 `BENCHMARKS` 换成「BENCHMARKS ∪ P70 七腿 ∪ 宽基四腿」的并集常量
   （或直接复用 `backfill_index.DEFAULT_CODES`）。
2. `_index_bias_states` / `_broad_leg_states` 保留 `date` 字段并落库为 `src_date`；
   `dt − src_date > N 个交易日` 时提示行显式打「⚠️ 数据陈旧 N 日」，别静默。
3. 创业板腿改用 `index_dump_399006_SZ.csv` 基底，切断对研究脚本产物 `spread_full_history.csv` 的依赖。

---

### ⚠️ F-C ｜ `[新发现]` ｜ `index_bias_daily` 丢掉了收盘的真实日期

`_index_bias_states` 返回 `date=str(h["trade_date"].iloc[-1])`（收盘真实日），
`_persist_index_bias_daily`（`action_plan.py:513-519`）**没有用它**：

```python
rows = [{"trade_date": str(dt), "code": s["code"], ...}]   # s["date"] 被丢弃
```

表里因此**无法区分**「今天的收盘」与「上周五的收盘盖了今天的戳」。
前端 `pages/Broad/index.tsx:297` 显示的 `bias.date` 正是这个计划日。
`_bias_extreme_hint` 也不打真实日期 ⟹ 邮件同样看不出来。

对照：同一份 `manifest.json` **自己就写了** 🔴「七腿末日不一致 ⟹ 跨腿横截面须先对齐」，
研究侧有这个警告，**生产侧一个都没有**。

**建议**：`index_bias_daily` 加一列 `src_date`（或把 PK 换成 `(src_date, code)`），
落库时同时写；hint 在参差时显式标注。

---

### ⚠️ F-D ｜ `[新发现]` ｜ `melt_frame` 会把长表接口压成一个点 —— 国债收益率曲线只存下 1 个期限

`_DAILY` 里注册了 `("yc_cb", dict(ts_code="1001.CB", curve_type="0"))`（中债国债收益率曲线）。
tushare `yc_cb` 的返回是**已经是长表**的：每个 `trade_date` 有多行，一行一个 `curve_term`
（0.08/0.25/0.5/1/3/5/7/10/30 年…），值在 `yield` 列。

而 `melt_frame`（`ingest_macro.py:70-99`）的收尾是：

```python
return out.drop_duplicates(subset=["period", "series"], keep="last")
```

`series` 只到 `"yc_cb.yield"` 这一层，**不含 `curve_term`** ⟹ 同一天的十来个期限
被 `keep="last"` 压成 **1 条**，而且落库后**看不出留下的是哪个期限**
（`yc_cb.curve_term` 那条 series 同样只剩最后一行，只能间接倒推）。

CLAUDE.md 说 P54「覆盖 …/国债曲线/…」【文档】——**实际存下的是曲线上的一个点**。
`us_tycr`（美债，宽表 y1m/y2m/…）不受影响；`fx_daily`/`shibor_lpr`/`cn_*` 都是宽表，也不受影响。
所以问题只出在 `yc_cb` 这一个接口，**但正是唯一的期限结构数据源**。

**`[待DB确认]`**
```sql
SELECT series, COUNT(*) n, MIN(period), MAX(period)
FROM macro_series WHERE source='yc_cb' GROUP BY series;
-- 若 yc_cb.yield 的 n ≈ 交易日数（而不是 交易日数 × 期限数）⟹ F-D 坐实。
SELECT period, value FROM macro_series
WHERE series='yc_cb.curve_term' ORDER BY period DESC LIMIT 10;
-- 看留下的是哪个期限、是否逐日漂移。
```

**建议**：`melt_frame` 增一个可选 `extra_keys`（如 `("curve_term",)`），
把这些列拼进 `series`（`yc_cb.yield@10Y`），或者对 `yc_cb` 单独走一条 pivot 分支。
**顺带**：`drop_duplicates(keep="last")` 现在是静默的 —— 一旦丢行应该 `logger.warning` 出行数差，
否则将来任何新的长表接口都会以同一种方式静默丢数。

---

### ⚠️ F-E ｜ `[新发现]` ｜ 别名表不对称：`sf_stock` 缺 `sf_month` 回退，测试恰好绕开了它

`signals/macro.py:_ALIAS`：

```python
"sf_stock": ("cn_sf.stk_endperiod",),                    # ← 只有一个候选，无回退
"sf_inc":   ("cn_sf.inc_month", "sf_month.inc_month"),   # ← 有回退，但 chen_readings 根本没用它
```

`chen_readings` 只消费 `sf_stock`（第④项「信用扩张强度＝社融存量同比」），**不消费 `sf_inc`**。
而 `_MONTHLY` 里 `cn_sf` 与 `sf_month` 两个接口名并列注册，说明作者自己也不确定哪个可用。
⟹ 若实际可用的是 `sf_month`，`sf_stock_yoy` 会**永远返回 None**，
`macro_digest` 打一个「—」，没有任何报错、也不会有人发现第④项一直是空的。

**测试掩盖了它**：`tests/test_macro_layer.py:test_scissors_and_credit_growth` 用
`cn_sf.stk_endperiod` 播种 ⟹ 走的正好是唯一存在的那个别名，**结构上不可能命中这个缺口**。

**`[待DB确认]`**
```sql
SELECT source, COUNT(DISTINCT series) k, COUNT(*) n, MIN(period), MAX(period)
FROM macro_series GROUP BY source ORDER BY source;
-- 看 cn_sf 与 sf_month 谁真的有数；若只有 sf_month ⟹ 第④项恒 None。
```

**建议**：`"sf_stock": ("cn_sf.stk_endperiod", "sf_month.stk_endperiod")`；
并给 `chen_readings` 的每个 None 项加一条「缺哪条 series」的说明，别只给「—」。

---

### ⚠️ F-F ｜ `[新发现]` ｜ 基数校正算错了对象：`seasonality()` 拿到的是 **M1 同比**，不是 M1 余额

`seasonality()` 的 docstring 自己写死了依据【代码原文】：

> 他 2024-02 判"假开门红"的全部依据就是这一步——"过去十年除 2016 外，1 月 M1 **环比**均减少"

但 `chen_readings` 的调用是：

```python
m1 = pick(panel, "m1_yoy")          # ← 同比序列（%）
out["m1_seasonality"] = seasonality(m1, ...)
```

`seasonality()` 里 `mom = d.diff()` ⟹ 算出来的是 **「M1 同比读数的逐月一阶差分」(pp)**，
不是 **「M1 余额的环比」(%)**。这两个量的季节性完全不同：
M1 **余额**在 1 月因春节前企业活期转现金而近乎必减（这才是他那句话的经济含义）；
M1 **同比**的一阶差分在 1 月并没有这种结构性符号。

`macro_digest.py` 把它原样标成「`{m} 月 M1 的历史**环比**：… 中位环比 {median_mom:+.2f}」，
**面向用户的文案也是错的**（单位其实是 pp，不是 %）。

`_ALIAS` 里根本没有 M1 余额（`cn_m.m1`）的条目，所以现在拿不到正确输入。

**建议**：`_ALIAS` 加 `"m1_level": ("cn_m.m1",)`，`chen_readings` 改为
`seasonality(pick(panel,"m1_level"), ...)`；digest 文案区分「余额环比(%)」与「同比变化(pp)」。
这是**执行纪律层**的问题（算术可复核、不改仓位），按 §7.5 不需要 E 判据。

---

### ⚠️ F-G ｜ `[新发现]` ｜ 宏观入库：绕过客户端封装 + 无分页 + 部分失败仍绿灯 + 无定时器

四条都在 `ingest_macro.py` / `ingest-macro.yml`，性质是「入库结果不可信且不可察」：

1. **绕过封装**：脚本用 `client.pro.<iface>()` 裸调，**没有走** `TushareClient` 上的
   `@_retry` / `@_rate_limit` 装饰器（那些只装在方法上）⟹ 一次网络抖动 = 该接口整段缺失。
2. **无分页**：`get_namechange` 里已有现成的 `limit/offset` 分页范式
   （`tushare_client.py:336-346`），宏观脚本一处都没用。日度接口默认从 `20050101` 拉
   ≈5000+ 行，tushare 单次有行数上限 ⟹ **早期历史可能被静默截断**，且没有
   「返回行数 == 上限」的告警。
3. **部分失败仍绿灯**：`collect()` 每个接口 `try/except → logger.warning` 后继续；
   `main()` 只在**全部**为空时 `SystemExit`。⟹ 8 个接口里挂 7 个，job 依然 success，
   `macro_series` 静默变成残表，**与完整入库不可区分**。
4. **没有定时器**：`ingest-macro.yml` 只有 `workflow_dispatch` + `ops/ingest-macro.trigger` push，
   `faas/jobs.py` 里也没有对应 job【自算：逐个查了 49 个 workflow，只有 `us-update.yml` 有 schedule】。
   ⟹ CLAUDE.md 写的 E47 前置条件「**数据层连续入库 ≥6 个月后方可首评**」【文档】
   在当前配置下**要靠人每月手动 bump 才能满足**。

**`[待DB确认]`**
```sql
SELECT source, freq, COUNT(*) n, MIN(period) p0, MAX(period) p1
FROM macro_series GROUP BY source, freq ORDER BY freq, source;
-- ① 日度源的 p0 若远晚于 20050101，或 n 恰为 1000/2000/5000 这类整数 ⟹ 行数上限截断（第 2 条）。
-- ② 缺哪个 source ⟹ 那个接口一直在静默失败（第 3 条）。
SELECT MAX(created_at) FROM macro_series;   -- 上次真正入库是什么时候（第 4 条）
```

**建议**：①改走 `TushareClient` 的重试/限速；②日度接口加 offset 分页 + 命中上限告警；
③`main()` 增「期望接口清单 vs 实到清单」对账，缺失即非零退出；
④给 `ingest-macro.yml` 加 `schedule`（月中一次即可，宏观是月频），或挂进 `job_weekly_rebuild_review`。

---

### ℹ️ F-H ｜ `[新发现]` ｜ 同一张网页上并排放着两种「第几低」，其中一种是系统自己判过是退化的口径

- `/invest/broad`（`broad_leg_state.bias_rank`）＝ **全历史**逐日排名
  → 前端 `pages/Broad/index.tsx:216` 渲染成「（**历史**第 N 低）」。
- `/invest/bias`（`index_bias_daily.rank_low`）＝ **近十年滚动 2500 日**排名
  → 同页 `:330` 渲染成「**近十年**第几低」。

同一天同一指数两个数差很远【自算，20260728 创业板】：全历史 **第 92 低** vs 近十年 **第 12 低**。
标签确实做了区分（不算 bug），但系统自己的裁决是「全历史排名会在 2008/2015 后被永久锁死，
所以 P70 才选滚动窗口」【文档】——**页面上却把那个被判过退化的口径摆在更显眼的主表里**。

另有第三条路径：`broad_export_web.py:91` 的 `bias_rank_day` 也是全历史逐日排名，
但它自洽（`_cur` 与 `_bv` 同源），**不受 F-A 影响**。
⟹ 全库现在有 **3 套 bias 排名实现**，只有生产那套有 F-A 的缺陷。

**建议**：三处统一 import 一个 `bias_rank(series, win)` 唯一真源
（照 2026-08-05 `invest_model/broad_gates.py` 的做法），物理上消除漂移。

---

### ℹ️ F-I ｜ `[确认既有 + 补强]` ｜ 数据包可确定性重建，且价格底座经得起外部常识对撞

这一条是**正面结论**，但因为是「一致」，按本轮规则它**不构成证据**，只作背景。

- **重建一致**：`bias_meanrev_data.py --out /tmp/xd/...` 重跑，7/7 md5 与仓内 `manifest.json`
  **逐字节一致**【自算】⟹ `.gitignore` 掉 CSV 只留 manifest 的做法是站得住的。
- **恒等式**：`Δlnbias = 价格腿 − 均线腿` 的残差 max **1.6e−15 ~ 3.4e−15**【自算】。
- **无日历洞**：7 个 CSV 各自区间内**零缺失交易日**、年均 242.3~242.9 个交易日、
  排序单调、无重复日期；2500 日窗口真实跨度 **10.28~10.29 年**（科创50 1535 日 ＝ 6.58 年）【自算】。
- **外部常识对撞**（`/tmp/xd/` 最后一段，我独立知道的市场史 vs 数据）【自算】：

| 检验点 | 数据 | 我独立知道的 | 判 |
|---|---|---|---|
| 沪深300 历史最高收盘 | **5877.20 @ 20071016** | 5877.20，2007-10-16 | ✅ |
| 创业板指 历史最高收盘 | **3982.25 @ 20150603** | 3982.25，2015-06-03（盘中 4037.96 在 06-05） | ✅ |
| 中证500 历史最高收盘 | **11545.89 @ 20150612** | 2015-06-12 见顶 | ✅ |
| 创业板 2024-10-08 乖离率 | **+55.83%** | 与博主原文一致 | ✅ |
| 沪深300 2024-10-08 乖离率 | **+25.98%** | — | ✅（与【文档】一致） |
| 沪深300 2005 最低收盘 | 818.03 @ 20050603；20050711=824.10 | 常引的 807.78 是**盘中**价，CSV 只存收盘 | ✅ 不矛盾 |
| 2021-02-10 抱团顶横截面 | 沪深300 +11.0% / 上证50 +10.6% / 创业板 +15.6% 但 **中证1000 −4.1% / 红利 −1.7%** | 2021 顶极窄，大盘成长涨、小盘价值跌 | ✅ 高度吻合 |

⟹ **价格底座本身没问题**；本轮所有问题都在**读数实现、口径与刷新链路**上，
和 2026-08 之前那几轮的性质一致。

---

## 三、我没能验证的部分（诚实清单）

1. **`macro_series` 里实际有什么**。表结构、melt 逻辑、读数还原我都逐行看了，
   但**一行真实数据都没见过**。F-D/F-E/F-G 三条最有价值的宏观发现全部依赖
   `[待DB确认]` 的 SQL，请务必跑一次再采信。
2. **tushare 各接口的真实列名与行数上限**。无网络。`yc_cb` 是长表、`us_tycr` 是宽表
   这类判断来自我对接口形态的知识，不是本环境实测。**`--probe` 模式跑一次即可证实/证伪 F-D。**
3. **`index_daily` 的真实新鲜度**。F-B 的「结构上没有自动刷新路径」是**从代码证明的**
   （`update.py` 只循环 3 个 BENCHMARKS、49 个 workflow 只有 1 个有 schedule），
   但「今天实际陈旧几天」必须查库。
4. **fe-journey-faas 的 `/invest/bias` handler**：不在 session 里。
   前端 `BIAS_TOPK` 前后端各有一份【文档】，我只核了 Python 端那份（=4），
   **没核前端那份是不是也 4**。
5. **`broadIndex.json` 的实际内容**与页面渲染效果。
6. **约 55 个 `scripts/analysis/*` 研究脚本未审**（本轮改动的大头）。我只查了 E59/E57
   的排名实现是否有 F-A 同型缺陷（结论：没有），其余的统计推断、bootstrap、
   episode 聚合逻辑**一律没看**。若要覆盖那一面，需要单独一轮。
7. **F-A 的 22 个漏报日是否会改变任何既有裁决**：我判断不会（研究脚本自洽），
   但没有逐个 E 去回归验算。
8. **并列（tie）处理**：我用 `rank_low + rank_high == win + 1` 作不变量，
   隐含假设窗口内无严格相等的 bias 值。全历史扫描中未见反例，但**没有专门统计并列数**。
