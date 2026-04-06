#!/usr/bin/env python3
"""
事件因子 - 股东减持、限售解禁
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

class EventFactors:
    """事件因子计算器"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def calc_holder_reduce_score(self, ts_code, days=90):
        """
        计算股东减持因子
        
        Args:
            ts_code: 股票代码
            days: 统计天数（默认 90 天）
        
        Returns:
            dict: 减持记录数、减持评分
        """
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        query = """
            SELECT holder_type, trade_volume, trade_price
            FROM stock_holder_trade
            WHERE ts_code = ? AND trade_type = '减持' AND trade_date >= ?
        """
        df = pd.read_sql_query(query, self.conn, params=(ts_code, cutoff_date))
        
        if len(df) == 0:
            return {'reduce_count': 0, 'holder_reduce_score': 0}
        
        # 按股东类型扣分
        penalty = 0
        for _, row in df.iterrows():
            holder_type = str(row['holder_type'])
            if '控股' in holder_type or '实际控制人' in holder_type:
                penalty += 0.15
            elif '高管' in holder_type or '董事' in holder_type:
                penalty += 0.08
            else:
                penalty += 0.03
        
        # 扣分上限 -0.3
        holder_reduce_score = max(-0.3, -penalty)
        
        return {
            'reduce_count': len(df),
            'holder_reduce_score': round(holder_reduce_score, 3)
        }
    
    def calc_lockup_score(self, ts_code, days=60):
        """
        计算限售解禁因子
        
        Args:
            ts_code: 股票代码
            days: 统计天数（默认 60 天）
        
        Returns:
            dict: 解禁比例、解禁评分
        """
        # 获取未来 60 天内的解禁
        today = datetime.now().strftime('%Y%m%d')
        future_date = (datetime.now() + timedelta(days=days)).strftime('%Y%m%d')
        
        query = """
            SELECT release_ratio FROM stock_lockup_release
            WHERE ts_code = ? AND release_date BETWEEN ? AND ?
        """
        df = pd.read_sql_query(query, self.conn, params=(ts_code, today, future_date))
        
        if len(df) == 0:
            return {'lockup_ratio': 0, 'lockup_score': 0}
        
        total_ratio = df['release_ratio'].sum()
        
        # 按比例扣分
        if total_ratio > 0.2:
            lockup_score = -0.15
        elif total_ratio > 0.1:
            lockup_score = -0.08
        else:
            lockup_score = -0.03
        
        return {
            'lockup_ratio': round(total_ratio, 3),
            'lockup_score': round(lockup_score, 3)
        }
    
    def calc_event_score(self, ts_code):
        """计算综合事件分"""
        reduce = self.calc_holder_reduce_score(ts_code)
        lockup = self.calc_lockup_score(ts_code)
        
        event_score = reduce['holder_reduce_score'] + lockup['lockup_score']
        
        return {
            **reduce,
            **lockup,
            'event_score': round(event_score, 3)
        }
    
    def close(self):
        self.conn.close()

# 使用示例
if __name__ == '__main__':
    db_path = Path('../../data/invest.db')
    ef = EventFactors(db_path)
    
    for code in ['002594.SZ', '002648.SZ', '000833.SZ', '300442.SZ']:
        event = ef.calc_event_score(code)
        print(f"{code}: 事件分={event['event_score']}, 减持={event['reduce_count']}次")
    
    ef.close()
