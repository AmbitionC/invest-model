#!/usr/bin/env python3
"""
数据质量修复和优化脚本
"""

import sqlite3
from pathlib import Path
from datetime import datetime

db_path = Path('data/invest.db')
conn = sqlite3.connect(db_path)

print("=" * 70)
print("🔧 数据质量修复")
print("=" * 70)

# 1. 创建数据质量监控表
print("\n【1】创建数据质量监控表")
conn.execute("""
CREATE TABLE IF NOT EXISTS data_quality_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    check_date TEXT NOT NULL,
    check_type TEXT NOT NULL,
    ts_code TEXT,
    issue_type TEXT,
    issue_desc TEXT,
    severity TEXT,
    is_fixed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()
print("✅ data_quality_log 表已创建")

# 2. 创建系统配置表
print("\n【2】创建系统配置表")
conn.execute("""
CREATE TABLE IF NOT EXISTS system_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_key TEXT UNIQUE NOT NULL,
    config_value TEXT,
    config_desc TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# 插入默认配置
configs = [
    ('factor.lookback_days', '250', '因子计算回溯天数'),
    ('risk.single_position_limit', '0.20', '单只股票仓位上限'),
    ('risk.total_position_limit', '0.80', '总仓位上限'),
    ('risk.stop_loss', '0.10', '止损比例'),
    ('risk.take_profit', '0.30', '止盈比例'),
]

for key, value, desc in configs:
    conn.execute(
        "INSERT OR REPLACE INTO system_config (config_key, config_value, config_desc) VALUES (?, ?, ?)",
        (key, value, desc)
    )
conn.commit()
print("✅ system_config 表已创建")

# 3. 创建交易日历表
print("\n【3】创建交易日历表")
conn.execute("""
CREATE TABLE IF NOT EXISTS trade_calendar (
    trade_date TEXT PRIMARY KEY,
    is_trading_day INTEGER NOT NULL,
    day_of_week INTEGER
)
""")
conn.commit()
print("✅ trade_calendar 表已创建")

# 4. 添加数据校验规则文档
print("\n【4】创建数据校验规则文档")
validation_rules = """
# 数据校验规则

## 1. 价格数据校验
- 价格不能为 0 或负数
- 单日涨跌幅不能超过 50%（除新股）
- 复权价格应该连续（无跳空）

## 2. 成交量校验
- 成交量不能为负
- 成交量突然放大 5 倍以上需要标记

## 3. 因子校验
- 分位数因子应该在 0-1 之间
- NaN 比例不能超过 50%
- 均值应该接近 0.5（均匀分布）

## 4. 时间一致性
- 信号日期必须是交易日
- 不能有周末或节假日的信号
- 数据更新应该每日连续

## 5. 前视偏差检查
- 因子计算只能使用历史数据
- 财务数据只能用公告日期后的数据
- 不能用未来数据计算历史信号
"""

with open(Path('docs/data_validation_rules.md'), 'w', encoding='utf-8') as f:
    f.write(validation_rules)
print("✅ 数据校验规则文档已创建")

# 5. 创建数据质量检查脚本
print("\n【5】创建数据质量检查脚本")
check_script = """#!/usr/bin/env python3
\"\"\"
每日数据质量检查
\"\"\"

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

def check_data_quality():
    db_path = Path('data/invest.db')
    conn = sqlite3.connect(db_path)
    
    issues = []
    
    # 1. 检查零价格
    zero_price = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM stock_daily WHERE close <= 0",
        conn
    )['count'].iloc[0]
    
    if zero_price > 0:
        issues.append({
            'type': 'zero_price',
            'severity': 'high',
            'desc': f'发现{zero_price}条零价格记录'
        })
    
    # 2. 检查价格突变
    price_jump = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM stock_daily WHERE ABS(pct_chg) > 50",
        conn
    )['count'].iloc[0]
    
    if price_jump > 0:
        issues.append({
            'type': 'price_jump',
            'severity': 'medium',
            'desc': f'发现{price_jump}条价格突变记录'
        })
    
    # 3. 检查因子范围
    factors = ['macd_dif_pct', 'rsi_pct', 'momentum_pct', 'volume_pct']
    for factor in factors:
        out_of_range = pd.read_sql_query(
            f"SELECT COUNT(*) as count FROM calc_technical_factors WHERE {factor} < 0 OR {factor} > 1",
            conn
        )['count'].iloc[0]
        
        if out_of_range > 0:
            issues.append({
                'type': 'factor_range',
                'severity': 'medium',
                'desc': f'{factor}有{out_of_range}条记录超出 [0,1] 范围'
            })
    
    # 4. 检查 NaN 比例
    for factor in factors:
        nan_pct = pd.read_sql_query(
            f"SELECT AVG(CASE WHEN {factor} IS NULL THEN 1 ELSE 0 END) * 100 as nan_pct FROM calc_technical_factors",
            conn
        )['nan_pct'].iloc[0]
        
        if nan_pct > 50:
            issues.append({
                'type': 'high_nan',
                'severity': 'low',
                'desc': f'{factor}的 NaN 比例为{nan_pct:.1f}%'
            })
    
    # 记录到数据库
    check_date = datetime.now().strftime('%Y-%m-%d')
    for issue in issues:
        conn.execute("""
            INSERT INTO data_quality_log 
            (check_date, check_type, issue_type, issue_desc, severity)
            VALUES (?, ?, ?, ?, ?)
        """, (check_date, 'daily_check', issue['type'], issue['desc'], issue['severity']))
    
    conn.commit()
    conn.close()
    
    return issues

if __name__ == '__main__':
    issues = check_data_quality()
    if issues:
        print(f"发现 {len(issues)} 个数据质量问题：")
        for issue in issues:
            print(f"  [{issue['severity']}] {issue['desc']}")
    else:
        print("✅ 数据质量检查通过")
"""

with open(Path('scripts/daily_data_check.py'), 'w', encoding='utf-8') as f:
    f.write(check_script)
print("✅ 数据质量检查脚本已创建")

conn.close()

print("\n" + "=" * 70)
print("✅ 数据质量修复完成")
print("=" * 70)
print("\n新增功能：")
print("  1. data_quality_log - 数据质量日志表")
print("  2. system_config - 系统配置表")
print("  3. trade_calendar - 交易日历表")
print("  4. docs/data_validation_rules.md - 校验规则文档")
print("  5. scripts/daily_data_check.py - 每日检查脚本")
