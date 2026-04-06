#!/usr/bin/env python3
"""
数据校验系统 - 确保数据准确性
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

class DataValidator:
    """数据校验器"""
    
    def __init__(self, logger=None):
        self.logger = logger
        self.errors = []
        self.warnings = []
    
    def log(self, level, message):
        """记录日志"""
        if level == 'error':
            self.errors.append(message)
            if self.logger:
                self.logger.error(f"[数据校验] {message}")
        elif level == 'warning':
            self.warnings.append(message)
            if self.logger:
                self.logger.warning(f"[数据校验] {message}")
        elif self.logger:
            self.logger.info(f"[数据校验] {message}")
    
    def check_price_jump(self, df, threshold=0.5):
        """检查价格突变（单日涨跌幅超过阈值）"""
        if 'close' not in df.columns or len(df) < 2:
            return True
        
        df = df.sort_values('trade_date')
        pct_change = df['close'].pct_change().abs()
        abnormal = pct_change[pct_change > threshold]
        
        if len(abnormal) > 0:
            for idx in abnormal.index:
                date = df.loc[idx, 'trade_date']
                pct = abnormal[idx] * 100
                self.log('warning', f"价格突变：{date} 涨跌幅 {pct:.1f}% (阈值 {threshold*100}%)")
            return False
        return True
    
    def check_volume_spike(self, df, threshold=5.0):
        """检查成交量异常（突然放大 threshold 倍）"""
        if 'vol' not in df.columns or len(df) < 20:
            return True
        
        df = df.sort_values('trade_date')
        vol_ma20 = df['vol'].rolling(20).mean()
        vol_ratio = df['vol'] / vol_ma20
        abnormal = vol_ratio[vol_ratio > threshold]
        
        if len(abnormal) > 0:
            for idx in abnormal.index:
                date = df.loc[idx, 'trade_date']
                ratio = abnormal[idx]
                self.log('warning', f"成交量异常：{date} 放量 {ratio:.1f}倍 (阈值 {threshold}倍)")
            return False
        return True
    
    def check_missing_data(self, df, expected_days=250):
        """检查数据缺失"""
        if len(df) < expected_days * 0.8:  # 少于 80% 预期数据
            self.log('error', f"数据缺失：只有 {len(df)} 条记录 (预期 {expected_days}条)")
            return False
        return True
    
    def check_zero_price(self, df):
        """检查零价格或负价格"""
        if 'close' not in df.columns:
            return True
        
        zero_price = df[df['close'] <= 0]
        if len(zero_price) > 0:
            self.log('error', f"零价格数据：{len(zero_price)} 条记录")
            return False
        return True
    
    def check_factor_range(self, df, factor_name, min_val=0, max_val=1):
        """检查因子值范围（如分位数应该在 0-1 之间）"""
        if factor_name not in df.columns:
            self.log('warning', f"因子不存在：{factor_name}")
            return True
        
        out_of_range = df[(df[factor_name] < min_val) | (df[factor_name] > max_val)]
        if len(out_of_range) > 0:
            self.log('warning', f"因子超范围：{factor_name} 有 {len(out_of_range)} 条记录超出 [{min_val}, {max_val}]")
            return False
        return True
    
    def validate_stock_data(self, df, stock_code):
        """校验单只股票数据"""
        self.log('info', f"开始校验 {stock_code} 数据...")
        
        all_passed = True
        
        # 1. 零价格检查
        if not self.check_zero_price(df):
            all_passed = False
        
        # 2. 价格突变检查
        if not self.check_price_jump(df, threshold=0.5):
            all_passed = False
        
        # 3. 成交量异常检查
        if not self.check_volume_spike(df, threshold=5.0):
            all_passed = False
        
        # 4. 数据量检查
        if not self.check_missing_data(df, expected_days=250):
            all_passed = False
        
        # 5. 因子范围检查
        factors = ['boll_position', 'macd_dif_pct', 'rsi_pct', 'momentum_pct', 'volume_pct', 'volatility_pct', 'deviation_pct']
        for factor in factors:
            if factor in df.columns:
                if not self.check_factor_range(df, factor, 0, 1):
                    all_passed = False
        
        if all_passed:
            self.log('info', f"✅ {stock_code} 数据校验通过")
        else:
            self.log('warning', f"⚠️  {stock_code} 数据校验发现问题")
        
        return all_passed
    
    def generate_report(self):
        """生成校验报告"""
        report = []
        report.append("=" * 60)
        report.append("数据校验报告")
        report.append("=" * 60)
        report.append(f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"错误数：{len(self.errors)}")
        report.append(f"警告数：{len(self.warnings)}")
        
        if self.errors:
            report.append("\n错误列表：")
            for err in self.errors:
                report.append(f"  ❌ {err}")
        
        if self.warnings:
            report.append("\n警告列表：")
            for warn in self.warnings:
                report.append(f"  ⚠️  {warn}")
        
        report.append("=" * 60)
        
        return "\n".join(report)

# 使用示例
if __name__ == '__main__':
    import sqlite3
    from utils.logger import setup_logger
    
    logger = setup_logger()
    validator = DataValidator(logger)
    
    db_path = Path('data/invest.db')
    conn = sqlite3.connect(db_path)
    
    # 校验所有股票
    stocks = pd.read_sql_query("SELECT DISTINCT ts_code FROM stock_daily", conn)
    
    for code in stocks['ts_code']:
        df = pd.read_sql_query(f"SELECT * FROM stock_daily WHERE ts_code='{code}'", conn)
        validator.validate_stock_data(df, code)
    
    print(validator.generate_report())
    conn.close()
