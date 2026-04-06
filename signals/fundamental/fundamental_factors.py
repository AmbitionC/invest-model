#!/usr/bin/env python3
"""
基本面因子 - 估值、成长、质量
"""

import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
from scipy import stats

class FundamentalFactors:
    """基本面因子计算器"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def calc_valuation_score(self, ts_code):
        """
        计算估值因子
        
        Returns:
            dict: PE 分位、PB 分位、估值评分
        """
        # 获取财务数据
        query = """
            SELECT pe_ttm, pb, ann_date FROM stock_fundamentals
            WHERE ts_code = ? AND pe_ttm IS NOT NULL AND pb IS NOT NULL
            ORDER BY ann_date DESC
            LIMIT 20
        """
        df = pd.read_sql_query(query, self.conn, params=(ts_code,))
        
        if len(df) < 5:
            return {'pe_pct': 0.5, 'pb_pct': 0.5, 'valuation_score': 0.5}
        
        # 计算历史分位
        pe_pct = 1 - stats.percentileofscore(df['pe_ttm'], df.iloc[0]['pe_ttm']) / 100
        pb_pct = 1 - stats.percentileofscore(df['pb'], df.iloc[0]['pb']) / 100
        
        # 估值分（低估值高分）
        valuation_score = pe_pct * 0.6 + pb_pct * 0.4
        
        return {
            'pe_pct': round(pe_pct, 3),
            'pb_pct': round(pb_pct, 3),
            'valuation_score': round(valuation_score, 3)
        }
    
    def calc_growth_score(self, ts_code):
        """
        计算成长因子
        
        Returns:
            dict: 营收增速、利润增速、成长评分
        """
        query = """
            SELECT revenue_growth, profit_growth, ann_date
            FROM stock_fundamentals
            WHERE ts_code = ? AND revenue_growth IS NOT NULL
            ORDER BY ann_date DESC
            LIMIT 8
        """
        df = pd.read_sql_query(query, self.conn, params=(ts_code,))
        
        if len(df) < 2:
            return {'revenue_growth': 0, 'profit_growth': 0, 'growth_score': 0.5}
        
        latest = df.iloc[0]
        
        # 增速趋势
        if len(df) >= 2:
            prev = df.iloc[1]
            growth_trend = latest['profit_growth'] - prev['profit_growth']
        else:
            growth_trend = 0
        
        # 成长分
        rev_score = min(1, max(0, (latest['revenue_growth'] + 50) / 100))
        profit_score = min(1, max(0, (latest['profit_growth'] + 50) / 100))
        trend_score = min(1, max(0, (growth_trend + 50) / 100))
        
        growth_score = rev_score * 0.4 + profit_score * 0.4 + trend_score * 0.2
        
        return {
            'revenue_growth': round(latest['revenue_growth'], 3),
            'profit_growth': round(latest['profit_growth'], 3),
            'growth_trend': round(growth_trend, 3),
            'growth_score': round(growth_score, 3)
        }
    
    def calc_quality_score(self, ts_code):
        """
        计算质量因子
        
        Returns:
            dict: ROE、毛利率、质量评分
        """
        query = """
            SELECT roe, grossprofit_margin, debt_to_assets, ann_date
            FROM stock_fundamentals
            WHERE ts_code = ? AND roe IS NOT NULL
            ORDER BY ann_date DESC
            LIMIT 4
        """
        df = pd.read_sql_query(query, self.conn, params=(ts_code,))
        
        if len(df) < 1:
            return {'roe': 0, 'gross_margin': 0, 'quality_score': 0.5}
        
        latest = df.iloc[0]
        
        # ROE 趋势
        if len(df) >= 2:
            roe_trend = latest['roe'] - df.iloc[1]['roe']
        else:
            roe_trend = 0
        
        # 质量分
        roe_score = min(1, latest['roe'] / 20)  # ROE>20% 得满分
        margin_score = min(1, latest['grossprofit_margin'] / 40)  # 毛利率>40% 得满分
        debt_score = 1 - (latest['debt_to_assets'] / 100)  # 负债率越低越好
        
        quality_score = roe_score * 0.5 + margin_score * 0.3 + debt_score * 0.2
        
        return {
            'roe': round(latest['roe'], 3),
            'roe_trend': round(roe_trend, 3),
            'gross_margin': round(latest['grossprofit_margin'], 3),
            'quality_score': round(quality_score, 3)
        }
    
    def calc_all_factors(self, ts_code):
        """计算所有基本面因子"""
        valuation = self.calc_valuation_score(ts_code)
        growth = self.calc_growth_score(ts_code)
        quality = self.calc_quality_score(ts_code)
        
        # 综合基本面分
        fundamental_score = (
            valuation['valuation_score'] * 0.375 +
            growth['growth_score'] * 0.375 +
            quality['quality_score'] * 0.25
        )
        
        return {
            **valuation,
            **growth,
            **quality,
            'fundamental_score': round(fundamental_score, 3)
        }
    
    def close(self):
        self.conn.close()

# 使用示例
if __name__ == '__main__':
    db_path = Path('../../data/invest.db')
    ff = FundamentalFactors(db_path)
    
    factors = ff.calc_all_factors('002594.SZ')
    print("比亚迪基本面因子：")
    for k, v in factors.items():
        print(f"  {k}: {v}")
    
    ff.close()
