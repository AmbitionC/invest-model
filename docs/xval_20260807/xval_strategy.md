# 策略/规则层独立交叉验证 — 2026-08-02 ~ 08-07 改动面

工作目录 `/home/user/invest-model`（`claude/invest-model-weekly-review-6t4ktf` @ `6f26586`）。
比对基线 `85e19db`。**全程只读**：未改任何仓内文件、未 commit、未 push；扰动实验一律在内存里做
（`/tmp/xval/*.py` 里 monkeypatch），临时产物落 `/tmp/xval/`。

数字来源标注贯穿全文：**〔跑〕= 我自己在本环境跑出来的**；**〔抄〕= 从 CLAUDE.md / docs 抄的**。

---

## 0. 一句话结论

「唯一真源」**只在卖出闸上成立**。买入闸那一半是装饰性的——`BUY_MUL` 被三处消费者读去
**画线、落库、发到网页上**，但**没有任何一个做买入判定的地方读它**。而生产的四腿状态机
与 `docs/broad_index_playbook.md` 写死的规格在**两个买点上**对不上：B2 恐慌腿丢了价格闸和冷却期，
B3 阶梯腿在生产里**根本没实现**（谓词恒 `False`），却仍在页面上显示一条它自己不认的「买入线」。

---

## 1. 唯一真源核查结论

### 1.1 方法：扰动实验（不是读代码，是真的改值看谁跟着动）

`_sell_above()`（`action_plan.py:312-315`）在**模块 import 时**把 `_SELL_MUL[name]` 固化进闭包，
所以 import 之后再改字典是无效的——第一次实验因此得出了错误结论，已重做：在 `import action_plan`
**之前**改 `invest_model.broad_gates` 的值，DT=20260731，脚本 `/tmp/xval/tamper2.py`。

| 扰动 | 沪深300 | 创业板 | 科创50 | 红利 | 判定 |
|---|---|---|---|---|---|
| 基线 | sell | sell | sell | sell | — |
| `SELL_MUL` 全设 9.99 | **hold** | **hold** | **hold** | **hold** | ✅ 卖出判定确实读 broad_gates |
| `BUY_MUL` 全设 1.60 | sell | sell | sell | sell | 🔴 **买入判定完全没读** |

〔跑〕`BUY_MUL=1.60` 那一行里，四腿的**展示买入线**分别涨到 5497.7 / 2993.8 / 1760.2 / 6370.0，
而收盘是 4600.3 / 3327.0 / 1588.4 / 5569.4 —— 沪深300 与科创50 的收盘已经**低于展示的买入线**，
状态却还是「🔴卖出区」。也就是说：改这个数只会让页面上的线和状态**互相矛盾**，不会改变任何决策。

### 1.2 谁真的读 broad_gates

| 消费者 | SELL_MUL | BUY_MUL | 备注 |
|---|---|---|---|
| 生产 `action_plan._BROAD_LEGS` | ✅ 闭包（`:312`） | ❌ **硬编码在 lambda**（`:322/326/331/336`） | 见下 |
| 回测 `long_window_backtest.run` | ✅（`:145`） | ❌ 硬编码 `0.90 if nm=="创业板" else 1.0`（`:143`） | |
| 网页导出 `broad_export_web` | ✅ | ⚠️ 只用于画线/落 `gates` 字段 | 交易仍走 `long_window_backtest.run` |
| `broad_now_chart` / `broad_history_chart` | ✅ | ⚠️ 同上（只画线） | |
| `e56_bias_low_tail` | ✅ | ❌ 硬编码（`:168`） | |

**买入闸的实际定义处一共 20 处**，全部是字面量 `0.90 / 1.0`，没有一处 import `BUY_MUL`
（`grep "0.90 if nm"` 命中 20 行：`long_window_backtest:143`、`broad_trades_chart:106,159`、
`review_disposition_calib:95`、`pnl_window:114,298`、`sell_timing_scan:68,149`、`broad_playbook:47`、
`red_calib_attack:115,258,493`、`intraweek/intramonth_timing_bound`、`e56:168`、`e48_e49:67` 等）。

### 1.3 完全绕过 broad_gates 的文件（卖出闸也硬编码）

12 个 `scripts/analysis/*.py` 含 1.30/1.43 字面量且不 import broad_gates：
`broad_trades_chart` · `review_disposition_calib` · `review_disposition_chart` · `pnl_window` ·
`sell_timing_scan` · `broad_playbook` · `red_calib_attack` · `intraweek_timing_bound` ·
`intramonth_timing_bound` · `v3_independent_verdict` · `e48_e49_sell_variants` · `e50_greed_sell`。

其中两个是**面向 owner 的产物生成器**，不是一次性研究脚本：
- **`broad_playbook.py:47-48`** 硬编码 `bm=0.90/1.00`、`sm=1.43/1.30` —— 它生成
  `results/broad_playbook.png`（`docs/broad_index_playbook.md` 的配图）**和 `results/broad_trades.csv`**（423 笔流水）。
- **`broad_trades_chart.py:108,160,165`** 硬编码 `1.30*1.10` / `1.30` —— 生成 `results/broad_trades.png`。
  🔴 **这个文件是 08-05「建立唯一真源」那次提交（f5315da）之后才加的（c1e5d46）**，加进来时就没接真源。

⚠️ `1.30*1.10 = 1.4300000000000002 ≠ 1.43`〔跑〕，相对差 2.2e-16。实测不改变任何一笔成交
（`results/broad_trades.csv` 计数 42/76、47/77、4/7、34/136 与 broad_gates 驱动的引擎逐条一致〔跑〕），
只是潜在漂移。

### 1.4 owner 看到的文案里的闸位也是字面量，不是真源

- `action_plan.py:303` `_p31_sell_hint` 正文写死「中位线×1.30，创业板×1.43」；
- `_BROAD_LEGS` 的 `sell_txt`/`buy_txt`（`:323/326/333/336`）同样写死「＞中位线×1.30（月减5%）」等。

〔跑〕把 `_SELL_MUL["沪深300"]` 改成 9.99 后，`_BROAD_LEGS[0][8]` 仍返回「＞中位线×1.30（月减5%）」。
⟹ **改闸位会让每日计划的正文说谎**，且不会有任何断言拦住。

---

## 2. 可跑性核查

| 脚本 | 结果 | 说明 |
|---|---|---|
| `long_window_backtest.py` | ⚠️ **裸跑失败**，`PYTHONPATH=<repo>` 后 EXIT=0 | `ModuleNotFoundError: No module named 'invest_model'`——`:29` import 了 invest_model 却**没有 sys.path 引导**（兄弟脚本 `broad_history_chart:31`、`broad_export_web:27-28`、`e56:34-35` 都有）。文件头注释说「复现不再依赖任何外部目录」——依赖没了，但默认调用方式仍然跑不起来 |
| `sell_gate_sweep.py` | ✅ EXIT=0 | |
| `broad_export_web.py` | ✅ EXIT=0，327 KB JSON | |
| `broad_history_chart.py` | ✅ EXIT=0 | |
| `broad_now_chart.py` | ✅ EXIT=0 | 同样缺 sys.path 引导 |
| **`broad_playbook.py`** | 🔴 **EXIT=1** | `_price_series` 在 `:236` 写死 `root = Path(".")`，忽略 `--data`。文档里的复现命令 `--data <数据目录> --out-dir results` 在 repo 根跑会在**打印完全部表格之后**崩在画图。cwd 切到 `results/` 才 EXIT=0〔跑〕。⚠️ 典型的「stderr 不看就以为成功」——前 80 行输出完全正常 |
| **`broad_trades_chart.py`** | 🔴 **EXIT=1** | `FileNotFoundError: 'results/hs300.csv'` |
| **`e37_deviation_top.py`** | 🔴 **EXIT=1** | 同上 |
| `style_rotation/agent_R{1,2,3}*.py` | 🔴 EXIT=1 | 先缺 scipy（**不在 requirements.txt**），装上后同样 `hs300.csv` 缺失 |
| `pytest tests/` | ⚠️ **199 passed / 3 failed / 1 skipped**〔跑〕 | 见 §3.9 |

### 2.1 跑通的数与文档对得上（这一节是确认，不是发现）

〔跑〕`long_window_backtest.py`（PYTHONPATH 后）：

```
沪深300  2007-01~2026-07 19.5y  6.41% vs 3.02%  +3.39  夏普0.32  回撤 -33.0%  均仓52%
创业板   2012-06~2026-07 14.1y 17.18% vs 11.52% +5.66  夏普0.69  回撤 -43.8%  均仓61%
科创50   2019-12~2026-07  6.6y 11.65% vs  7.29% +4.37  夏普0.53  回撤 -20.1%  均仓30%
红利     2007-01~2026-07 19.5y  6.50% vs  8.50% -2.00  夏普0.37  回撤 -33.7%  均仓40%
✅两腿全窗 20070129 19.5y 6.45% 夏普0.354 -33.0%   ✅四腿共同在场 20191231 6.6y 12.58% 0.696 -18.0%
```
与 CLAUDE.md〔抄〕的 0.32/0.69/0.53/0.37、−33.0/−43.8/−20.1/−33.7、6.45%/0.354、12.58%/0.696
**逐位一致**。`sell_gate_sweep.py` 同样复现：1.30 处 ±0.10 邻域年化极差 0.25~0.35pp、
峰在 1.350/1.525/1.425/1.600、分半分歧 0.20/0.25/0.43、网格能动 0.95~2.81pp、四腿同闸差 10.78pp〔跑〕。
`_first_lot_cap(1.30×med) = 32.5%`〔跑〕，与 P51 声称一致。

ℹ️ 一处细节：红利腿的样本内最优闸 **1.600 落在扫描网格上界**，即它的 argmax 是**被截断的**，
真值 ≥1.60 未知。「1.30 不占任何一腿的样本内峰值」这条论证对红利腿其实没被真正检验。

---

## 3. 发现

### 3.1 🔴波及生产 · [新发现] `BUY_MUL` 是纯装饰，且被当权威展示

见 §1.1/1.2。加重情节：网页 `invest-journey/src/pages/Broad/index.tsx:141-166` 的表格标题
是「**当前闸位（生产与回测同一份定义）**」，下面第 155 行渲染 `锚 × ${r.buy_mul}`。
这个数在页面上被明确声明为「生产与回测同一份」，而实测**两边都不读它**。

### 3.2 🔴波及生产 · [新发现] 科创50 的阶梯买点（B3）在生产里根本没实现

`action_plan.py:331` 科创50 的买入谓词是 `lambda c, m, r: False`，紧跟其后的说明文字却完整描述了
「距全历史峰 −50/−55/−60/−65 四档」。全仓无第二处计算科创50 距峰回撤（`peak` 只在 P28/P30 的沪深300 用）。

〔跑〕复刻回测的 ladder 分支，取它历史上真正开火的三天，喂给**生产函数** `_broad_leg_states`：

| 日期 | 距峰 | 回测 | 生产 state | 生产展示的「买入线」 | 当日收盘 |
|---|---|---|---|---|---|
| 20231215 | −51% | B3 L50 买 30% 现金 | `hold` | 1167.1 | 847.5（低于买入线 **−27%**） |
| 20240119 | −56% | B3 L55 买 35% | `hold` | 1152.3 | 765.5（**−34%**） |
| 20240202 | −61% | B3 L60 买 40% | `panic` | 1144.3 | 673.4（**−41%**） |

⟹ 卡片上同时显示「买入线 1167 / 收盘 847 / ⚪持有区」。这三笔是科创50 历史 4 笔买入里的 3 笔
（`docs/broad_index_playbook.md` §三〔抄〕），也就是**这条腿的主引擎在提示层是哑的**。

### 3.3 🔴波及生产 · [新发现] B2 恐慌腿丢了价格闸与冷却期，且优先级压过卖出区

`action_plan.py:371` `panic = fear is not None and fear >= 75` —— 只有情绪一条。
规格（`docs/broad_index_playbook.md` §二 B2〔抄〕）与回测（`long_window_backtest.py:127`）要求
**三条 AND**：`fear≥75` **且** `距上次恐慌日 > 20 交易日` **且** `收盘 < r1250(近5年中位线)`。
且 `:372-377` 的优先级是 buy > panic > sell，所以 **panic 会盖掉 sell**。

〔跑〕历史共现（`/tmp/xval/chk_panic.py`）：

| 腿 | fear≥75 天数 | 过回测价格闸 | **不过闸但生产照样报「恐慌抢买窗」** | 其中还同时在卖出区 |
|---|---|---|---|---|
| 沪深300 | 69 | 21 | **48** | 20 |
| 创业板 | 69 | 22 | **40** | 34 |
| 科创50 | 21 | 1 | 3 | 3 |
| 红利 | 69 | 14 | **55** | 54 |

最近一次是**上个月**〔跑〕，直接调生产函数复现 20260713（fear 80.8）：

```
沪深300 state=panic close=4695.4 卖线=4460.7  (收盘/近5年中位=1.17)  回测B2门：不过
创业板  state=panic close=3723.5 卖线=2671.4  (=1.57，比卖出闸还高 39%)  不过
科创50  state=panic close=1994.3 卖线=1423.6  (=1.94)                 不过
红利    state=panic close=5198.1 卖线=5168.6  (=0.99)                 **过**（回测确实在 0714 买了）
```
⟹ 那一天四条腿的提示行都写「🟢恐慌抢买窗 … 恐慌≥75 任意腿抢买池 50%」，而按规格只有红利该买，
另外三条**正处在该减仓的卖出区**。0713/0717/0720 三天连报（20 日冷却也没实现），
按规格是一次。前端 hint 文案（`Broad/index.tsx:224` 附近）同样写「恐慌 X（≥75 时任意腿可抢买）」，
把这个缺口一并复制到了页面上。

### 3.4 🔴波及生产 · [新发现] 同一张网页上「买入线」有两个互斥定义

- **静态历史**（`broad_export_web.py:118-120`）对 ladder 腿正确处理：`buy_line = peak × 0.50`，
  且表格渲染成「距峰 −50/−55/−60/−65%」（`Broad/index.tsx:155`）。
- **当日状态**（`action_plan.py:538` → `broad_leg_state.buy_line` → `/invest/broad`）无论腿型
  一律 `med × buy_mul`，页面渲染成「买 1100 / 卖 1430」（`Broad/index.tsx:211`），
  hover 提示写「买入线 = 锚×1.00」。

〔跑〕20260730 科创50：`med×1.00 = 1100.2` vs `peak×0.50 = 1103.9`，**目前只差 −0.3%**，
所以这个矛盾现在肉眼看不出来。但 `peak` 是单调不减的、`med` 走得很慢，两条线会分开——
**现在是巧合，不是一致**。`broad_leg_state` 表也没有 `mode` 列（`schema.py:583-610`），
FaaS/前端拿不到「这条腿不走锚买」的信息。

### 3.5 🔴波及生产 · [新发现] 乖离率排名在同一页有两个定义

- `broad_leg_state.bias_rank`（`action_plan.py:365`）＝ **全历史**逐日排名，
  前端渲染成「（历史第 N 低）」（`Broad/index.tsx:217`）。
- `index_bias_daily.rank_low`（`action_plan.py:466`，P70）＝ **近十年 2500 交易日滚动窗口**内排名，
  前端「近十年第几低」（`Broad/index.tsx:330`）。

〔跑〕同一天两个数：

| 腿 | bias60 | 全历史第N低（旧列，仍在页面上） | 近十年窗第N低（P70） |
|---|---|---|---|
| 沪深300 | −4.85% | **995** | 294 |
| 创业板 | −15.28% | **92** | **12** |
| 科创50 | −13.33% | 31 | 31（窗口仅 1535 天） |
| 红利 | +1.88% | 3113 | 1650 |

CLAUDE.md〔抄〕明写「全历史排名会在 2008/2015 后被永久锁死」正是 P70 改用滚动窗口的理由，
但旧口径那一列没有下线，仍以「历史第 92 低」的形式挂在页面上，和「近十年第 13 低」隔了两屏。

### 3.6 ⚠️只影响回测 · [新发现] 恐慌腿的「不可用期」披露不完整

`r1250 = rolling(1250).median()` 是在**每条腿自己的 CSV**上滚的，所以恐慌腿在 CSV 起点后
1250 个交易日内**结构性不可能触发**〔跑〕：

| 腿 | 数据首日 | r1250 首个可用日 |
|---|---|---|
| 沪深300 / 红利 | 20050104 | 20100226 |
| 创业板 | 20100601 | 20150724 |
| **科创50** | 20191231 | **20250303** |

已披露的口径提醒（脚本 docstring、`broad_playbook` 输出、`broadIndex.json:caveats`）**只说了
「2015 前无恐慌数据」**。对科创50 而言真正的约束是 2025-03——**它 6.6 年窗口里有 5.1 年
恐慌腿是关着的**，这一条哪儿都没写。

### 3.7 ⚠️只影响回测 · [新发现] 「回撤诚实读数」是在**不连续**的序列上算的

`long_window_backtest.py:169-174`：`m = pos >= 0.20` 取布尔掩码，然后对 `v[m]` 直接
`np.maximum.accumulate`。掩码挑出来的日子**不连续**，把它们首尾相接再求 running max，
得到的不是任何真实时间段的最大回撤。§A 表里的「子区间策略 −33.0% / 子区间买持 −46.7%」
就是这么算的，而这组数是 2026-08-04 红队 F3 用来**取代**已作废的「70%→17~44%」说法的替代读数。
门槛 `m.sum() > 250` 也是任意值、无对照臂。

### 3.8 ⚠️只影响回测 · [新发现] 已被撤回的结论仍印在图上 / 已被修掉的 bug 仍活在另一个文件里

- `long_window_backtest.py:441` 子图 [5] 标题：**「策略把 70% 级别的回撤压到 17~44%\n这是这套东西最大的单一价值」**
  ——CLAUDE.md〔抄〕2026-08-04 已明确「作废」。〔跑〕这张图今天仍然这么出（`results/long_window.png` 会被覆盖）。
  同图 `:458`「策略最差窗口全部为正，买入持有会亏钱」也是写死在标题里的断言。
- `broad_trades_chart.py:37` 科创50 起点仍硬编码 **`"20200601"`** ——这正是红队 F1 认定
  「把科创50 超额从 +4.07 虚增到 +8.77pp」的那个缺陷；`long_window_backtest.py:48-62` 已改成自动对齐，
  但 08-05 新加的这个图表脚本把旧写法又抄了一遍。它同时把窗口起点写死 `D0="20150601"`，
  而它的提交信息是「时间跨度拉到数据极限」。（该脚本目前跑不起来，见 §2。）

### 3.9 ℹ️记录 · [新发现] 测试从来没有被跑过

- 〔跑〕`pytest tests/` = **3 failed, 199 passed, 1 skipped**。
  失败项：`test_crowding.py::test_crowding_hints_holdings_by_weight`、
  `::test_crowding_hints_advisor_theme_pileup`（真断言失败，非缺依赖）、
  `test_faas_scheduler.py::test_use_keepalive_session_patches_module`（缺 tushare）。
  两个 crowding 失败的代码与测试最后一次改动都远早于本审计窗（`crowding.py` @ d401032），
  ⟹ **属既有、非本窗引入**。
- 本窗新增的 `tests/test_macro_layer.py:28` 在**模块级** import `scripts/ingest_macro.py`，
  后者模块级 import `tushare` ⟹ 没装 tushare 时**整个 collection 被中断**（`Interrupted: 1 error`），
  连其余 199 个测试都跑不了。
- 🔴 `grep -rln "pytest" .github/` **零命中**（68 个 workflow）⟹ **没有任何 CI 跑测试**，
  所以上面两条都不会有人发现。

### 3.10 ℹ️记录 · [新发现] `_base_sleeve_hint`（P27 底仓 25%）是死代码

`action_plan.py:696` 定义了 `_base_sleeve_hint`，读 `BASE_SLEEVE_TARGET`（默认 0.25），
**全仓无调用点**（grep 只命中定义、注释、文档）。P27 v2 用 `_broad_legs_hint` 取代它是有意的
（`:398` docstring 写了），但 `docs/model_change_proposals.md:565`〔抄〕仍写着
「系统只出提示行 `_base_sleeve_hint`」——文档描述了一个不再发生的生产行为。

### 3.11 ℹ️记录 · [新发现] P52 与状态机对「科创50 是否触发买入」判定不一致（同一文件内）

`_broad_no_action_hint`（`:655`）用 `s["last"] < s["med"] * s["buy_mul"]` 判断「有腿触发」，
对科创50 等价于 `close < 中位线`；而 `_broad_leg_states`（`:370`）对同一条腿恒给 `False`。
〔跑〕历史上科创50 有 **829 / 1094** 个可算日满足 `close < 中位线`（20220121~20250801），
这些日子 P52 会认为「有腿触发」而**静默不输出**「不动也是决策」行，尽管状态机说四腿都没触发。

### 3.12 ℹ️记录 · [待DB确认] 恐慌值的取数口径生产比回测宽

`_fear_score`（`:680`）= `SELECT score FROM fear_daily WHERE trade_date<=:d ORDER BY trade_date DESC LIMIT 1`
—— 当日无恐慌行时**沿用最近一条**（可能是几天前的）。回测 `fmap.get(d[i])` 取不到就是 NaN、不触发。
在恐慌 EOD 落库延迟的日子里，生产会用陈旧恐慌值点亮 panic/P30。需要 DB 才能量化发生频率。

### 3.13 ℹ️记录 · [新发现] 两张表对同一个指数用不同的腿名

`_BROAD_LEGS` 叫「红利」，`_BIAS_UNIVERSE`（`:426`）叫「中证红利」，
分别落 `broad_leg_state.leg` 与 `index_bias_daily.name`。跨表 join / 前端对齐要小心。

### 3.14 ℹ️记录 · [确认既有] P58 收口的四腿取值与文档一致

〔跑〕`broad_gates.SELL_MUL = {沪深300:1.30, 创业板:1.43, 科创50:1.30, 红利:1.30}`，
生产闭包、回测、网页导出（`public/data/broadIndex.json` 的 `gates` 字段，generated_at 2026-08-06）三处一致。
E51 越过的记账、平顶论证、代价披露都在 `broad_gates.py` 文件头，与 CLAUDE.md 一致。**这一条无异议。**

---

## 4. 未写进判据的实现选择清单

「两个分支分别是什么 / 有没有做过对照臂」。除特别注明外，**均无对照臂**。

| # | 位置 | 分支 A（现行） | 分支 B（未测） | 影响 |
|---|---|---|---|---|
| 1 | `long_window_backtest.py:32` `WARM=500` | 500 | 350/650/800 | 已知会让两条腿变号〔抄〕。**有对照臂**（唯一一条） |
| 2 | `:69` `np.median(c[:i+1])` | 锚**含当日收盘** | `c[:i]`（不含当日） | 锚与被比较的价格互相包含；小样本期影响大 |
| 3 | `:127` `i - last > 20`，而 `:130-131` 的 `last` 在**任何** fear≥75 日刷新 | 冷却按「上次恐慌日」 | 冷却按「上次成交日」 | 〔跑〕沪深300 有 **17 个满足价格闸的日子被挡掉**，含 2018-10 整段底部（1011/1012/1015/1016/1018）与 2020-03-23。规格文字写的是「距上次恐慌日」故不算偏离，但从没测过另一支 |
| 4 | `:127` `r1250` 在**各腿自己的 CSV** 上滚 | 逐腿 1250 根 | 用统一日历/更短窗 | 科创50 恐慌腿 5.1/6.6 年不可用（§3.6） |
| 5 | `:137-140` ladder `j = max(...)`，只把最深那档标记已用 | 浅档保持 armed ⟹ **回升途中还能再买** | 一次性作废所有更浅档 | 科创50 全部 4 笔买入里 3 笔来自这条腿 |
| 6 | `:141` episode 复位阈值 `-RUNG[0]*0.5` = −25%，且 `armed[:]=True` **四档全部重装** | −25% 全重装 | 其他阈值 / 只重装未用档 | 同上 |
| 7 | `:143/146` `r.we`/`r.me` 用 `!= shift(-1)` | **最后一行永远既是周末又是月末** | 真日历判定 | 滚动十年窗（115×4 个）每个都在任意日期截断，右端点必被判为周末+月末 |
| 8 | `:149` `min(i + 1, i1 - 1)` | 窗口右端点上 exec_lag 塌缩为 0 | 丢弃越界信号 | 同上，115 个窗口每个都吃到一次 |
| 9 | `:169-174` 「持仓≥20% 子区间」回撤 | 布尔掩码拼接后求 running max（**非连续**） | 取最长连续段 / 逐段取最差 | §3.7，这组数被用作已作废结论的替代读数 |
| 10 | `:171` `m.sum() > 250` | 250 | 任意 | 决定该读数是否输出 |
| 11 | `:143` 买入闸 `0.90 / 1.0` 字面量 | 硬编码 | `BUY_MUL` | §1.2 |
| 12 | `action_plan.py:371` panic 判定 | 只看 fear | 规格三条 AND | §3.3 🔴 |
| 13 | `:331` 科创50 买入谓词 | 恒 `False` | 实现 B3 | §3.2 🔴 |
| 14 | `:372-377` 状态优先级 | buy > **panic > sell** | sell 优先 / 并列显示 | 恐慌日把「卖出区」显示成「🟢抢买窗」 |
| 15 | `:312-315` `_sell_above` 在 **import 时**求值 `_SELL_MUL`，`:384` 展示值在**调用时**求值 | 两次读取时机不同 | 统一 | 当前同值；热改配置/测试时会不一致（我的第一次扰动实验就被它骗了） |
| 16 | `:538` ladder 腿的 `buy_line` | `med × buy_mul` | `peak × 0.50`（静态导出就是这么做的） | §3.4 🔴 |
| 17 | `:365` vs `:466` 乖离率排名窗口 | 全历史 / 2500 日**两套并存** | 统一 | §3.5 🔴 |
| 18 | `:680` `_fear_score` 取 `<= dt` 的最新一条 | 陈旧值前推 | 严格当日 | §3.12 |
| 19 | `:503` `_index_bias_states` 有 `drop_duplicates(keep="last")`，`:262` `_index_hist_by` 没有 | 两套拼接 | 统一 | 若 `index_daily` 与基底 CSV 重叠会分歧（当前查询用 `> max` 故无重叠） |
| 20 | `broad_playbook.py:236` / `broad_trades_chart.py:35-38` | `Path(".")` / scratchpad 文件名 | 跟随 `--data` | §2、§3.8 |
| 21 | 价格网格棘轮 `grid_reset` | **生产里不存在价格网格**（卖出是月频），仅 `red_calib_attack.py:44` 有 `grid_reset=False`（主线）/`True`（红队）两臂 | — | 生产不受影响；E54/E54-b 的重测义务仍在〔抄〕 |

---

## 5. 我没能验证的部分

1. **一切要 DB 的**：`index_daily` 增量、`fear_daily` 当日值、`broad_leg_state` / `index_bias_daily` /
   `leverage_signal` 的实际落库内容、issue #9 的真实推送文本。本报告里所有「生产会怎么说」
   都是**用仓内基底 CSV 喂给真实生产函数**跑出来的（`/tmp/xval/prod_state.py`、`ladder.py`），
   增量段（基底 CSV 末日之后）没有覆盖。
2. **fe-journey-faas 是否已部署当前 invest-model**：`/invest/broad`、`/invest/bias` 的线上行为
   取决于 FC 上跑的是哪个 ref，本环境查不到。
3. **前端静态 JSON 是否与线上一致**：`invest-journey/public/data/broadIndex.json` generated_at
   2026-08-06，本地复跑一致；是否已 Vercel 发布未验证。
4. **未重跑 E56/E57/E58/E59/E60**：本次聚焦实现一致性，这些脚本只做了 import 级检查。
   E37 的复现脚本已确认**跑不起来**（§2）。
5. **ETF 上市日 / 指数发布日**：V2 的三个否决项依赖它，本环境不能联网，未核实。
6. **`macro.py` / `ingest_macro.py`（P54/P64）**：只读了代码，未跑（需要 tushare + DB）。
   P64-A 说的 vintage 缺口在 `schema.py:529` 的主键上确认存在〔跑：读代码〕，未实测。
7. **两个 crowding 测试为何失败**：确认代码与测试都早于本窗，未深挖根因。
