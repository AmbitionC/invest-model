# ETF vs 个股评分模型分离方案

> 版本：v1.0  
> 日期：2026-06-13  
> 状态：设计文档（待实施）

---

## 1. 背景与问题

### 当前状态

系统对个股和 ETF 使用完全相同的 `CompositeScorer`，权重固定为：

```
tech_score    45%   ← ETF 有效（技术指标从 etf_daily 价格计算）
fund_score    15%   ← ETF 无数据（无 PE/PB/ROE），始终 = 0
flow_score    25%   ← ETF 无数据（无主力净流入/融资融券），始终 = 0
sent_score    15%   ← ETF 无数据（无股东人数/内部交易），始终 = 0
```

**核心缺陷：** ETF 的 `composite_score` 实际量程被压缩到 `[-0.45, +0.45]`，  
但 Advisor 使用与个股相同的阈值判断方向（如 `±0.1`），导致 ETF 长期被判为"中性"，  
错失大量有效信号。

### 典型问题案例

以 `516120.SH（化工ETF）` 为例，历史数据显示：
- 即使趋势明显向上（技术面 +0.3），composite 也只有 +0.135（45% × 0.3）
- Advisor 输出 HOLD（低置信度），实际应该 BUY
- 造成 ETF 永远"滞后半拍"的进出场效果

---

## 2. 目标架构

```
BaseScorer（抽象基类）
├── StockScorer（个股，从现有 CompositeScorer 演进）
│     weights: tech=0.40, fund=0.20, flow=0.25, sent=0.15
│     signals: 4 类现有信号生成器（不变）
│     advisor: 现有 StockAdvisor（不变）
│
└── ETFScorer（新建）
      weights: technical=0.55, etf_flow=0.30, market_context=0.15
      signals: ETF 专属信号生成器（4 个新建）
      advisor: ETFAdvisor（复用 StockAdvisor + ETFAdvisorConfig）
```

---

## 3. ETF 专属信号设计

### 3.1 信号列表

| 信号名 | 类别 | 数据来源 | 计算逻辑 | 方向规则 |
|--------|------|---------|---------|---------|
| `etf_volume_momentum` | technical | `etf_daily` | 5日成交额均值 / 60日均值 - 1 | >0.3 → bullish, <-0.3 → bearish |
| `etf_price_vs_nav` | technical | `etf_daily`, `etf_nav` | (close - unit_nav) / unit_nav | >0.02 → 溢价过高(bearish); <-0.01 → 折价买入机会(bullish) |
| `sector_relative_strength` | market_context | `etf_daily`, `index_daily` | ETF 20日涨跌 / 沪深300 20日涨跌 - 1 | >0.05 → bullish, <-0.05 → bearish |
| `etf_momentum_acceleration` | technical | `etf_daily` | 5日收益率均值 - 20日收益率均值 | >0.002 → 动量加速(bullish); <-0.002 → 减速(bearish) |

### 3.2 信号实现说明

```python
# invest_model/signals/etf_signals.py

@register(
    name="etf_volume_momentum",
    category="technical",
    scope="time_series",
    required_tables=["etf_daily"],
    asset_types=["etf"],   # 新增字段，限定只对 ETF 生效
)
class ETFVolumeMomentumGenerator(CategorizedSignalGenerator):
    def generate(self, code: str, context: dict) -> list[Signal]:
        df = context.get("etf_daily", pd.DataFrame())
        if df.empty or len(df) < 60:
            return []
        
        vol_5d = df["amount"].tail(5).mean()
        vol_60d = df["amount"].tail(60).mean()
        ratio = vol_5d / vol_60d - 1 if vol_60d > 0 else 0
        
        score = np.clip(ratio / 0.5, -1, 1)  # 归一化到 [-1, 1]
        return [Signal(
            name="etf_volume_momentum",
            score=score,
            direction="bullish" if score > 0.2 else "bearish" if score < -0.2 else "neutral",
            strength=score_to_strength(abs(score)),
            value=round(ratio, 4),
            label=f"成交量动量 {ratio:+.1%}",
        )]
```

### 3.3 需要新增的数据表

**`etf_nav`（ETF 每日净值）**

```sql
CREATE TABLE IF NOT EXISTS etf_nav (
    code        VARCHAR(16)  NOT NULL,
    trade_date  VARCHAR(8)   NOT NULL,
    unit_nav    DECIMAL(10,4),          -- 单位净值
    accum_nav   DECIMAL(10,4),          -- 累计净值
    adj_nav     DECIMAL(10,4),          -- 复权净值
    premium_discount DECIMAL(8,6),      -- 溢价折价率（自动计算）
    PRIMARY KEY (code, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF 每日净值';
```

**数据来源：** Tushare `fund_nav` 接口  
**采集器：** `invest_model/collectors/etf_nav_collector.py`（新建，继承 `BaseCollector`）

---

## 4. ETFScorer 实现设计

```python
# invest_model/scoring/etf_scorer.py

class ETFScorer(BaseScorer):
    """ETF 专属评分器。"""
    
    DEFAULT_WEIGHTS = {
        "technical":       0.55,   # 技术面（含 ETF 特有信号）
        "etf_flow":        0.30,   # ETF 资金动量信号
        "market_context":  0.15,   # 市场相对强弱
    }
    
    # ETF 专属信号映射到新类别
    SIGNAL_CATEGORY_MAP = {
        "etf_volume_momentum":      "etf_flow",
        "etf_price_vs_nav":         "technical",
        "sector_relative_strength": "market_context",
        "etf_momentum_acceleration": "technical",
        # 保留通用技术信号（MACD/RSI/Bollinger 等）→ "technical"
    }
    
    def score_batch(self, codes: list[str], trade_date: str):
        """仅处理 ETF 代码，输出格式与 StockScorer 一致。"""
        ...
```

---

## 5. ETF 专属 Advisor 配置

ETF 与个股在风险特征上有差异，需要独立的操作配置：

```python
# invest_model/advisor/advisor.py - 新增

ETF_ADVISOR_CONFIG = {
    "action_threshold": 55,          # 比个股（60）略低，ETF 流动性好，可适当灵活
    "strong_threshold": 75,
    "position_map": {
        "strong_buy": 0.60,          # ETF 可配置更高仓位（流动性强）
        "buy":        0.35,
        "reduce":     0.20,          # 减仓保留比例更高（ETF 成本低）
        "clear":      0.00,
    },
}
```

---

## 6. 流水线分流改造

在 `daily_pipeline.py` 的 `_generate_signals()` 步骤中，按 pool_group 分流：

```python
# invest_model/pipeline/daily_pipeline.py

def _generate_signals(self):
    from invest_model.scoring.scorer import CompositeScorer      # StockScorer
    from invest_model.scoring.etf_scorer import ETFScorer

    core_codes = self.pool_repo.get_pool_codes("core")
    etf_codes  = self.pool_repo.get_pool_codes("etf")
    trade_date = self._get_latest_trade_date()

    # 个股评分
    if core_codes:
        stock_scorer = CompositeScorer(self.engine)
        stock_scorer.score_batch(core_codes, trade_date, persist=True)

    # ETF 评分（独立评分器）
    if etf_codes:
        etf_scorer = ETFScorer(self.engine)
        etf_scorer.score_batch(etf_codes, trade_date, persist=True)
    
    return f"个股{len(core_codes)}只 + ETF{len(etf_codes)}只"
```

---

## 7. 文件变更清单

| 文件路径 | 操作 | 工作量 |
|---------|------|--------|
| `invest_model/scoring/base_scorer.py` | **新建** | M - 抽取公共接口 |
| `invest_model/scoring/scorer.py` | **重构** | M - 继承 BaseScorer |
| `invest_model/scoring/etf_scorer.py` | **新建** | L - ETF 专属评分逻辑 |
| `invest_model/signals/etf_signals.py` | **新建** | M - 4 个信号生成器 |
| `invest_model/models/ddl.py` | **修改** | S - 新增 etf_nav 表 |
| `invest_model/collectors/etf_nav_collector.py` | **新建** | S - NAV 数据采集 |
| `invest_model/pipeline/daily_pipeline.py` | **修改** | S - 信号生成分流 |
| `invest_model/advisor/advisor.py` | **修改** | S - ETFAdvisorConfig |
| `config/config.yaml` | **修改** | S - 新增 etf_scorer 权重配置 |

> 工作量估计：S=1天，M=2-3天，L=4-5天；总计约 2 周

---

## 8. 预期效果

| 指标 | 改造前 | 改造后（预期） |
|------|--------|---------------|
| ETF composite_score 量程 | [-0.45, +0.45] | [-1.0, +1.0] |
| ETF 信号有效率（触发 buy/sell 比例） | ~5% | ~25% |
| ETF 平均建仓时机偏差（vs 最优） | 5-8 天 | 2-3 天 |
| 组合整体夏普比率 | 基准 | 预计提升 0.2-0.4 |

---

## 9. 实施顺序

```
Phase 1 - 数据层（1周）
  Step 1: 修改 ddl.py，新增 etf_nav 表
  Step 2: 实现 ETFNavCollector，回填历史净值
  
Phase 2 - 信号层（1周）
  Step 3: 实现 etf_signals.py（4 个信号）
  Step 4: 注册到 signals.registry（添加 asset_type 过滤）

Phase 3 - 评分层（3天）
  Step 5: 抽取 BaseScorer
  Step 6: 实现 ETFScorer，继承 BaseScorer

Phase 4 - 集成层（2天）
  Step 7: 修改 daily_pipeline 分流
  Step 8: 添加 ETFAdvisorConfig

Phase 5 - 验证（2天）
  Step 9: 对比改造前后 516120.SH 历史信号质量
  Step 10: 回测对比（全池含 ETF vs 不含 ETF）
```

---

## 10. 风险与注意事项

1. **etf_nav 数据可用性**：Tushare `fund_nav` 接口需要一定积分等级，请确认权限
2. **溢价折价信号延迟**：净值通常 T+1 公布，需要注意计算 premium_discount 时的时间对齐
3. **不同 ETF 的 NAV 频率**：部分 ETF 净值只有每周/每月更新，需要做空值处理
4. **与个股共用 advisor 表**：stock_advisor_signal 表共用，无需改 DDL
5. **权重验证**：ETFScorer 新权重上线后，需 A/B 对比至少 20 个交易日才能统计显著性
