# 数据层技术架构

**版本**: v0.2  
**创建时间**: 2026-04-02  
**更新时间**: 2026-04-02  
**定位**: A 股交易系统数据基础设施

---

## 一、数据层架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                      数据采集层                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Tushare    │  │  AkShare    │  │  数据校验   │         │
│  │  (主数据源)  │  │  (补充数据)  │  │  (质量检查)  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                      数据存储层                              │
│                    SQLite 数据库                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  原始数据区 (raw_*)                                  │   │
│  │  - 行情数据 (stock_daily, etf_daily)                │   │
│  │  - 财务数据 (stock_fundamentals)                    │   │
│  │  - 事件数据 (holder_trade, lockup_release)          │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  计算指标区 (calc_*)                                 │   │
│  │  - 技术因子 (calc_technical_factors)                │   │
│  │  - 基本面因子 (calc_fundamental_factors)            │   │
│  │  - 事件因子 (calc_event_factors)                    │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  信号记录区 (signals)                                │   │
│  │  - 综合置信度 (signals)                             │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                      数据访问层                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  RawRepo    │  │  CalcRepo   │  │  SignalRepo │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、数据分类设计

### 2.1 数据分层

| 层级 | 表前缀 | 说明 | 更新频率 | 示例表 |
|------|--------|------|---------|--------|
| **原始数据** | `raw_` 或无前缀 | 直接从数据源获取，不做计算 | 每日/季度 | `stock_daily`, `stock_fundamentals` |
| **计算指标** | `calc_` | 基于原始数据计算的技术/基本面因子 | 每日 | `calc_technical_factors`, `calc_fundamental_factors` |
| **信号记录** | 无前缀 | 最终生成的交易信号 | 不定期 | `signals` |

### 2.2 为什么要分开存？

| 考虑 | 原始数据 | 计算指标 |
|------|---------|---------|
| **可追溯性** | 财报修正后，可以重新计算历史指标 | 保留计算时的参数版本 |
| **性能** | 不需要每次都重新算 | 直接读取，秒级响应 |
| **回测** | 避免未来函数 | 保留历史时点的指标快照 |
| **调试** | 可以验证计算逻辑是否正确 | 快速定位问题 |

---

## 三、原始数据表设计

### 3.1 个股原始数据

```sql
-- 日线行情表（原始数据）
CREATE TABLE stock_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,              -- 股票代码
    trade_date TEXT NOT NULL,        -- 交易日期 YYYYMMDD
    open REAL,                       -- 开盘价
    high REAL,                       -- 最高价
    low REAL,                        -- 最低价
    close REAL,                      -- 收盘价
    pre_close REAL,                  -- 昨收价
    change REAL,                     -- 涨跌额
    pct_chg REAL,                    -- 涨跌幅%
    volume REAL,                     -- 成交量（手）
    amount REAL,                     -- 成交额（千元）
    adjusted_close REAL,             -- 复权收盘价
    data_source TEXT DEFAULT 'tushare',  -- 数据来源
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,  -- 入库时间
    updated_at TEXT,                 -- 更新时间
    UNIQUE(code, trade_date)
);
CREATE INDEX idx_stock_daily_code ON stock_daily(code);
CREATE INDEX idx_stock_daily_date ON stock_daily(trade_date);

-- 财务指标表（原始数据）
CREATE TABLE stock_fundamentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    report_date TEXT NOT NULL,       -- 财报日期
    ann_date TEXT,                   -- 公告日期
    PE REAL,                         -- 市盈率（TTM）
    PB REAL,                         -- 市净率
    PS REAL,                         -- 市销率
    PE_TTM REAL,                     -- 市盈率（TTM）
    ROE REAL,                        -- 净资产收益率（加权）
    ROA REAL,                        -- 总资产收益率
    revenue_growth REAL,             -- 营收增速%
    profit_growth REAL,              -- 净利润增速%
    gross_margin REAL,               -- 毛利率%
    debt_to_asset REAL,              -- 资产负债率%
    eps REAL,                        -- 每股收益
    bps REAL,                        -- 每股净资产
    operating_cash_flow REAL,        -- 经营现金流净额（万元）
    net_profit REAL,                 -- 净利润（万元）
    total_revenue REAL,              -- 营业总收入（万元）
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT,
    UNIQUE(code, report_date)
);

-- 资金流向表（原始数据）
CREATE TABLE stock_cashflow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    main_net_inflow REAL,            -- 主力净流入（万元）
    small_net_inflow REAL,           -- 小单净流入
    medium_net_inflow REAL,          -- 中单净流入
    large_net_inflow REAL,           -- 大单净流入
    super_large_net_inflow REAL,     -- 超大单净流入
    north_capital_hold REAL,         -- 北向资金持股量
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, trade_date)
);

-- 股东增减持表（原始数据）
CREATE TABLE stock_holder_trade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    holder_name TEXT,                -- 股东名称
    holder_type TEXT,                -- 股东类型：控股股东/高管/财务投资人
    trade_date TEXT,                 -- 交易日期
    trade_type TEXT,                 -- 增持/减持
    trade_volume REAL,               -- 交易数量
    trade_price REAL,                -- 交易价格
    trade_method TEXT,               -- 集中竞价/大宗交易
    remaining_shares REAL,           -- 剩余持股数
    remaining_ratio REAL,            -- 剩余持股比例%
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, holder_name, trade_date, trade_type)
);
CREATE INDEX idx_holder_trade_code ON stock_holder_trade(code);

-- 限售股解禁表（原始数据）
CREATE TABLE stock_lockup_release (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    release_date TEXT NOT NULL,      -- 解禁日期
    release_volume REAL,             -- 解禁数量
    release_ratio REAL,              -- 占流通盘比例%
    holder_name TEXT,                -- 解禁股东
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, release_date, holder_name)
);
CREATE INDEX idx_lockup_release_code ON stock_lockup_release(code);

-- 股东户数表（原始数据）
CREATE TABLE stock_holder_count (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    report_date TEXT NOT NULL,
    holder_count INTEGER,            -- 股东户数
    change_ratio REAL,               -- 环比变化%
    avg_holdings REAL,               -- 户均持股数
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, report_date)
);
```

### 3.2 ETF 原始数据

```sql
-- ETF 日线行情表（原始数据）
CREATE TABLE etf_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    pre_close REAL,
    change REAL,
    pct_chg REAL,
    volume REAL,
    amount REAL,
    IOPV REAL,                       -- 实时净值
    premium_rate REAL,               -- 折溢价率%
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, trade_date)
);
CREATE INDEX idx_etf_daily_code ON etf_daily(code);

-- ETF 基金信息表（原始数据）
CREATE TABLE etf_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT,
    tracking_index TEXT,             -- 跟踪指数
    scale REAL,                      -- 基金规模（亿元）
    expense_ratio REAL,              -- 费率%
    manager TEXT,                    -- 基金经理
    成立_date TEXT,
    update_date TEXT,
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ETF 持仓表（原始数据）
CREATE TABLE etf_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    report_date TEXT NOT NULL,
    stock_code TEXT,                 -- 重仓股代码
    stock_name TEXT,                 -- 重仓股名称
    holding_ratio REAL,              -- 持仓比例%
    holding_rank INTEGER,            -- 持仓排名（1-10）
    data_source TEXT DEFAULT 'tushare',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, report_date, stock_code)
);
```

---

## 四、计算指标表设计（核心更新！）

### 4.1 技术因子计算表

```sql
-- 技术因子计算结果表（每日更新）
CREATE TABLE calc_technical_factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    
    -- 布林带因子
    bollinger_position REAL,         -- 股价在布林带位置 (0-1)
    bollinger_bandwidth_pct REAL,    -- 带宽分位数 (0-1)
    bollinger_signal INTEGER,        -- 信号：-1/0/1
    
    -- MACD 因子
    macd_dif REAL,
    macd_dea REAL,
    macd_hist REAL,
    macd_dif_pct REAL,               -- DIF 分位数 (0-1)
    macd_divergence INTEGER,         -- 背离：1 顶/-1 底/0 无
    macd_signal INTEGER,
    
    -- RSI 因子
    rsi REAL,
    rsi_pct REAL,                    -- RSI 分位数 (0-1)
    rsi_is_strong INTEGER,           -- 是否强势股 (0/1)
    rsi_divergence INTEGER,
    rsi_signal INTEGER,
    
    -- 动量因子
    momentum_20 REAL,
    momentum_pct REAL,               -- 动量分位数 (0-1)
    momentum_acceleration REAL,
    momentum_signal INTEGER,
    
    -- 成交量因子
    volume_ratio REAL,
    volume_pct REAL,                 -- 成交量分位数 (0-1)
    volume_price_signal INTEGER,
    volume_signal INTEGER,
    
    -- 波动率因子
    volatility REAL,
    volatility_pct REAL,             -- 波动率分位数 (0-1)
    atr_ratio REAL,
    volatility_signal INTEGER,
    
    -- 偏离度因子
    deviation_20 REAL,
    deviation_pct REAL,              -- 偏离度分位数 (0-1)
    trend_strength REAL,
    deviation_signal INTEGER,
    
    -- 计算元数据
    lookback_period INTEGER DEFAULT 250,  -- 回溯期
    calc_version TEXT DEFAULT 'v1.0',     -- 计算逻辑版本
    data_source TEXT DEFAULT 'calculation',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code, trade_date)
);
CREATE INDEX idx_calc_tech_code ON calc_technical_factors(code);
CREATE INDEX idx_calc_tech_date ON calc_technical_factors(trade_date);

-- 技术因子综合评分表
CREATE TABLE calc_technical_score (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    
    -- 各因子得分（-1 到 1）
    bollinger_score REAL,
    macd_score REAL,
    rsi_score REAL,
    momentum_score REAL,
    volume_score REAL,
    volatility_score REAL,
    deviation_score REAL,
    
    -- 综合技术分（0-1）
    technical_score REAL,
    
    -- 计算元数据
    weights_version TEXT DEFAULT 'v1.0',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code, trade_date)
);
```

### 4.2 基本面因子计算表

```sql
-- 基本面因子计算结果表（季度更新）
CREATE TABLE calc_fundamental_factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    report_date TEXT NOT NULL,       -- 财报报告期
    
    -- 估值因子
    pe_percentile REAL,              -- PE 在过去 5 年的分位数
    pb_percentile REAL,              -- PB 在过去 5 年的分位数
    peg REAL,
    pe_industry_ratio REAL,          -- 相对行业 PE
    valuation_score REAL,            -- 估值分 (0-1)
    
    -- 成长因子
    revenue_growth REAL,
    profit_growth REAL,
    growth_trend REAL,               -- 增速趋势
    expected_growth REAL,            -- 一致预期增速
    growth_score REAL,               -- 成长分 (0-1)
    
    -- 质量因子
    roe REAL,
    roe_trend REAL,                  -- ROE 趋势
    gross_margin REAL,
    cashflow_ratio REAL,             -- 现金流/净利润
    debt_ratio REAL,                 -- 资产负债率
    quality_score REAL,              -- 质量分 (0-1)
    
    -- 综合基本面分（0-1）
    fundamental_score REAL,
    
    -- 行业信息
    industry TEXT,                   -- 所属行业
    industry_code TEXT,
    
    -- 计算元数据
    lookback_years REAL DEFAULT 5,   -- 估值回溯年数
    calc_version TEXT DEFAULT 'v1.0',
    data_source TEXT DEFAULT 'calculation',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code, report_date)
);
CREATE INDEX idx_calc_fund_code ON calc_fundamental_factors(code);
CREATE INDEX idx_calc_fund_date ON calc_fundamental_factors(report_date);
```

### 4.3 事件因子计算表

```sql
-- 事件因子计算结果表（不定期更新）
CREATE TABLE calc_event_factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    calc_date TEXT NOT NULL,         -- 计算日期
    
    -- 股东减持事件
    has_holder_reduce INTEGER,       -- 是否有减持 (0/1)
    reduce_type TEXT,                -- 减持类型：控股股东/高管/财务投资人
    reduce_count_90d INTEGER,        -- 90 天内减持次数
    reduce_ratio_90d REAL,           -- 90 天内减持比例%
    holder_reduce_score REAL,        -- 减持评分 (-1 到 0)
    
    -- 限售解禁事件
    has_lockup_60d INTEGER,          -- 60 天内是否有解禁 (0/1)
    lockup_ratio REAL,               -- 解禁比例%
    lockup_days INTEGER,             -- 距离解禁日天数
    lockup_score REAL,               -- 解禁评分 (-1 到 0)
    
    -- 综合事件分（-0.3 到 0）
    event_score REAL,
    
    -- 计算元数据
    calc_version TEXT DEFAULT 'v1.0',
    data_source TEXT DEFAULT 'calculation',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code, calc_date)
);
CREATE INDEX idx_calc_event_code ON calc_event_factors(code);
```

### 4.4 综合置信度表

```sql
-- 综合置信度计算结果表
CREATE TABLE calc_confidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    calc_date TEXT NOT NULL,
    
    -- 各维度得分
    technical_score REAL,            -- 技术面分 (0-1)
    fundamental_score REAL,          -- 基本面分 (0-1)
    event_score REAL,                -- 事件面分 (-0.3 到 0)
    
    -- 综合置信度 (0-1)
    confidence REAL,
    
    -- 信号类型
    signal_type TEXT,                -- strong_buy/buy/watch/sell/strong_sell
    
    -- 权重版本（便于追溯）
    weights_version TEXT DEFAULT 'v1.0',
    calc_version TEXT DEFAULT 'v1.0',
    
    -- 元数据
    data_source TEXT DEFAULT 'calculation',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code, calc_date)
);
CREATE INDEX idx_confidence_code ON calc_confidence(code);
CREATE INDEX idx_confidence_date ON calc_confidence(calc_date);
```

---

## 五、信号记录表设计

```sql
-- 交易信号表
CREATE TABLE signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id TEXT NOT NULL UNIQUE,  -- 信号 ID
    signal_time TEXT NOT NULL,       -- 信号产生时间
    code TEXT NOT NULL,
    name TEXT,                       -- 股票名称
    signal_type TEXT NOT NULL,       -- strong_buy/buy/watch/sell/strong_sell
    confidence REAL NOT NULL,        -- 置信度 (0-1)
    suggested_position REAL,         -- 建议仓位%
    stop_loss REAL,                  -- 止损位
    target_price REAL,               -- 目标位
    
    -- 因子快照（JSON 格式，便于追溯）
    factors_snapshot TEXT,           -- {
                                     --   "technical": {...},
                                     --   "fundamental": {...},
                                     --   "event": {...}
                                     -- }
    
    -- 推送状态
    is_pushed INTEGER DEFAULT 0,
    pushed_time TEXT,
    push_channel TEXT,               -- wechat/sms
    
    -- 执行状态
    is_executed INTEGER DEFAULT 0,
    executed_time TEXT,
    executed_price REAL,             -- 实际成交价
    executed_volume INTEGER,         -- 实际成交量
    
    -- 元数据
    calc_version TEXT DEFAULT 'v1.0',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(signal_id)
);
CREATE INDEX idx_signals_code ON signals(code);
CREATE INDEX idx_signals_time ON signals(signal_time);
CREATE INDEX idx_signals_type ON signals(signal_type);
```

---

## 六、数据版本控制（重要！）

### 6.1 为什么需要版本控制？

**问题场景**：
1. Tushare 修正了历史财报数据（如某公司后来修正了去年净利润）
2. 你修改了因子计算逻辑（如 PE 分位数从 3 年改为 5 年）
3. 回测时需要知道"当时看到的数据"而不是"最新修正的数据"

**解决方案**：
- 原始数据表加 `data_version` 字段
- 计算指标表加 `calc_version` 字段
- 每次重新计算时保留历史版本

### 6.2 版本控制实现

```sql
-- 原始数据表增加版本字段
ALTER TABLE stock_fundamentals ADD COLUMN data_version TEXT DEFAULT 'v1.0';

-- 或者用独立的历史表
CREATE TABLE stock_fundamentals_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    report_date TEXT NOT NULL,
    
    -- 数据字段（同 stock_fundamentals）
    PE REAL, PB REAL, ROE REAL, ...
    
    -- 版本信息
    data_version TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,   -- 快照日期（什么时候看到的数据）
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code, report_date, data_version)
);
```

### 6.3 版本管理策略

| 场景 | 版本策略 | 说明 |
|------|---------|------|
| **日常更新** | 覆盖更新 | 数据不变，只更新最新值 |
| **财报修正** | 保留历史版本 | `data_version` 递增（v1.0 → v2.0） |
| **计算逻辑变更** | 重新计算 + 新版本 | `calc_version` 递增，保留旧版本 |
| **回测需求** | 使用历史快照 | 查询指定 `snapshot_date` 的数据 |

---

## 七、数据访问层设计

### 7.1 Repository 分层

```python
# data/storage/repositories/
├── base_repo.py           # 仓库基类
├── raw/                   # 原始数据仓库
│   ├── stock_daily_repo.py
│   ├── stock_fundamentals_repo.py
│   └── ...
├── calc/                  # 计算指标仓库
│   ├── technical_factors_repo.py
│   ├── fundamental_factors_repo.py
│   └── confidence_repo.py
└── signal/                # 信号仓库
    └── signal_repo.py
```

### 7.2 计算指标 Repository 示例

```python
# data/storage/repositories/calc/technical_factors_repo.py
class TechnicalFactorsRepo:
    """技术因子计算结果仓库"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_by_code(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取某股票的技术因子历史"""
        sql = """
            SELECT * FROM calc_technical_factors
            WHERE code = ? AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
        """
        return pd.read_sql_query(sql, self.conn, params=(code, start_date, end_date))
    
    def get_latest(self, code: str) -> Optional[dict]:
        """获取最新的技术因子"""
        sql = """
            SELECT * FROM calc_technical_factors
            WHERE code = ?
            ORDER BY trade_date DESC
            LIMIT 1
        """
        cursor = self.conn.cursor()
        cursor.execute(sql, (code,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def save(self, code: str, trade_date: str, factors: dict, version: str = 'v1.0'):
        """保存技术因子计算结果"""
        sql = """
            INSERT OR REPLACE INTO calc_technical_factors
            (code, trade_date, bollinger_position, macd_dif, ..., calc_version, created_at)
            VALUES (?, ?, ?, ?, ..., ?, CURRENT_TIMESTAMP)
        """
        # 实现略
```

---

## 八、数据更新流程

### 8.1 日线数据更新流程

```
1. 采集原始数据
   Tushare API → stock_daily 表

2. 计算技术因子
   读取 stock_daily → 计算 6 个因子 → calc_technical_factors 表

3. 计算综合评分
   读取 calc_technical_factors → 加权合成 → calc_technical_score 表

4. 计算综合置信度
   读取 calc_technical_score + calc_fundamental_factors + calc_event_factors
   → calc_confidence 表

5. 生成信号
   读取 calc_confidence → 应用阈值 → signals 表
```

### 8.2 财报数据更新流程

```
1. 采集原始数据
   Tushare API → stock_fundamentals 表

2. 计算基本面因子
   读取 stock_fundamentals + 历史数据 → 计算估值/成长/质量分
   → calc_fundamental_factors 表

3. 更新综合置信度
   读取 calc_fundamental_factors → 重新计算 calc_confidence
```

---

## 九、性能优化

### 9.1 索引设计

```sql
-- 所有表都按 code + date 建立复合索引
CREATE INDEX idx_stock_daily_code_date ON stock_daily(code, trade_date);
CREATE INDEX idx_calc_tech_code_date ON calc_technical_factors(code, trade_date);
CREATE INDEX idx_calc_fund_code_date ON calc_fundamental_factors(code, report_date);
```

### 9.2 分区策略（未来优化）

如果数据量过大（>1000 万行），考虑：
- 按年份分表：`stock_daily_2021`, `stock_daily_2022`, ...
- 或用 PostgreSQL 的分区表功能

### 9.3 缓存策略

```python
# 计算指标缓存（减少数据库查询）
class FactorsCache:
    def get_latest_factors(self, code: str):
        # 1. 先查 Redis/内存缓存
        # 2. 缓存 miss 再查数据库
        # 3. 写入缓存（TTL=1 小时）
        pass
```

---

## 十、数据备份策略

### 10.1 备份频率

| 数据类型 | 备份频率 | 保留期限 |
|---------|---------|---------|
| 原始数据 | 每周 | 永久 |
| 计算指标 | 每周 | 1 年 |
| 信号记录 | 每月 | 永久 |

### 10.2 备份方式

```bash
# SQLite 备份脚本
#!/bin/bash
BACKUP_DIR="./backups/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# 完整备份
cp data/invest.db $BACKUP_DIR/invest.db

# 压缩
tar -czf $BACKUP_DIR.tar.gz $BACKUP_DIR

# 删除旧备份（保留 90 天）
find ./backups -name "*.tar.gz" -mtime +90 -delete
```

---

## 十一、待确认事项

### 11.1 数据版本
- [ ] 是否需要完整的数据版本控制？（增加复杂度）
- [ ] 还是简单处理（只保留最新数据）？
- **建议**：Phase 1 简单处理，Phase 2 再加版本控制

### 11.2 计算指标更新频率
- [ ] 技术因子：每日收盘后计算？
- [ ] 基本面因子：财报更新后计算？
- [ ] 事件因子：每日检查？

### 11.3 历史数据回溯
- [ ] 5 年历史数据是否需要重新计算所有指标？
- [ ] 还是从当前开始逐步积累？
- **建议**：一次性计算 5 年历史指标（需要时间）

---

## 十二、总结

### 数据分层架构

```
原始数据层 (raw_*)
    ↓ 计算
计算指标层 (calc_*)
    ↓ 合成
综合置信度层 (calc_confidence)
    ↓ 应用
信号记录层 (signals)
```

### 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| **原始/计算分离** | 是 | 可追溯、性能好、便于回测 |
| **版本控制** | Phase 2 | Phase 1 先简单处理 |
| **因子快照** | JSON 格式 | 便于追溯和调试 |
| **索引优化** | code+date 复合索引 | 查询性能 |
| **缓存策略** | 文件缓存 | 减少数据库查询 |

---

**文档版本**: v0.2  
**最后更新**: 2026-04-02  
**状态**: 待确认
