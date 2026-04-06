#!/usr/bin/env python3
"""
数据收集和可视化 - 无决策推荐
只显示数据指标和历史对比
"""

import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from scipy import stats
import plotly.graph_objects as go
from plotly.subplots import make_subplots

db_path = Path('data/invest.db')
conn = sqlite3.connect(db_path)

print("=" * 80)
print("📊 数据指标收集和历史对比")
print("=" * 80)

stocks = {
    '002648.SZ': '卫星化学',
    '002594.SZ': '比亚迪',
    '000833.SZ': '粤桂股份',
    '300442.SZ': '润泽科技',
    '516120.SH': '化工 50ETF'
}

for code, name in stocks.items():
    print(f"\n{'='*80}")
    print(f"{name} ({code})")
    print(f"{'='*80}")
    
    # 获取日线数据
    daily = pd.read_sql_query(
        f"SELECT trade_date, open, high, low, close, vol, amount FROM stock_daily WHERE ts_code='{code}' ORDER BY trade_date",
        conn
    )
    
    if len(daily) == 0:
        print("  无数据")
        continue
    
    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')
    current = daily.iloc[-1]
    current_date = current['trade_date']
    
    print(f"\n【基础数据】")
    print(f"  数据量：{len(daily)} 条")
    print(f"  时间范围：{daily['trade_date'].min().strftime('%Y-%m-%d')} 至 {current_date.strftime('%Y-%m-%d')}")
    print(f"  最新日期：{current_date.strftime('%Y-%m-%d')}")
    
    # 价格指标
    print(f"\n【价格指标】")
    print(f"  当前价：{current['close']:.2f} 元")
    print(f"  52 周最高：{daily['high'].max():.2f} 元")
    print(f"  52 周最低：{daily['low'].min():.2f} 元")
    print(f"  历史分位：{stats.percentileofscore(daily['close'], current['close']):.1f}%")
    
    # 均线系统
    for ma_period in [5, 10, 20, 60, 250]:
        if len(daily) >= ma_period:
            ma = daily['close'].rolling(ma_period).mean().iloc[-1]
            pct = (current['close'] - ma) / ma * 100
            above_below = "上方" if pct > 0 else "下方"
            print(f"  MA{ma_period}: {ma:.2f} 元 (现价在{above_below}{abs(pct):.1f}%)")
    
    # 成交量指标
    print(f"\n【成交量指标】")
    print(f"  当前成交量：{current['vol']:.0f} 手")
    print(f"  20 日均量：{daily['vol'].rolling(20).mean().iloc[-1]:.0f} 手")
    print(f"  量比：{current['vol'] / daily['vol'].rolling(20).mean().iloc[-1]:.2f}")
    
    # 历史对比
    periods = [
        (5, '5 日'),
        (20, '20 日'),
        (60, '60 日'),
        (250, '250 日/年')
    ]
    
    print(f"\n【历史水位对比】")
    for days, period_name in periods:
        if len(daily) >= days:
            hist = daily['close'].tail(days)
            current_pct = stats.percentileofscore(hist, current['close'])
            hist_min = hist.min()
            hist_max = hist.max()
            hist_mean = hist.mean()
            
            level = "极高" if current_pct > 90 else "高" if current_pct > 75 else "偏高" if current_pct > 60 else "中性" if current_pct > 40 else "偏低" if current_pct > 25 else "低" if current_pct > 10 else "极低"
            
            print(f"  {period_name:8} 分位：{current_pct:5.1f}% ({level:4}) | 区间：[{hist_min:.2f}, {hist_max:.2f}] | 均值：{hist_mean:.2f}")

conn.close()

print("\n" + "=" * 80)
print("✅ 数据收集完成")
print("=" * 80)
