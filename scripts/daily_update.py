#!/usr/bin/env python3
"""每日自动更新脚本"""

import os, sys, pandas as pd, numpy as np, sqlite3, tushare as ts
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from scipy import stats

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / '.env')
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()
db_path = PROJECT_ROOT / 'data' / 'invest.db'
conn = sqlite3.connect(db_path)

STOCKS = ['002648.SZ', '002594.SZ', '000833.SZ', '300442.SZ', '516120.SH']
STOCK_NAMES = {'002648.SZ': '卫星化学', '002594.SZ': '比亚迪', '000833.SZ': '粤桂股份', '300442.SZ': '润泽科技', '516120.SH': '化工 50ETF'}

print("=" * 60)
print(f"每日更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# 1. 采集日线
print("\n[1/4] 采集最新日线数据...")
end_date = datetime.now().strftime('%Y%m%d')
start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')

for code in STOCKS:
    try:
        if 'ETF' in STOCK_NAMES[code]:
            df = pro.fund_daily(ts_code=code, start_date=start_date, end_date=end_date)
            df.to_sql('etf_daily', conn, if_exists='append', index=False)
        else:
            df = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
            df.to_sql('stock_daily', conn, if_exists='append', index=False)
        if len(df) > 0:
            print(f"  ✅ {STOCK_NAMES[code]}: {len(df)} 条")
    except Exception as e:
        print(f"  ⚠️  {STOCK_NAMES[code]}: {e}")

# 2. 计算因子
print("\n[2/4] 重新计算技术因子...")

def calc_pct(series, lookback=250):
    return series.rolling(lookback).apply(lambda x: stats.percentileofscore(x, x.iloc[-1])/100 if len(x)>10 else np.nan)

def calc_boll(close):
    m = close.rolling(20).mean()
    s = close.rolling(20).std()
    u, l = m + 2*s, m - 2*s
    return {'pos': (close-l)/(u-l), 'bw': (u-l)/m}

def calc_macd(close):
    e1 = close.ewm(span=12).mean()
    e2 = close.ewm(span=26).mean()
    return {'dif': e1-e2}

def calc_rsi(close):
    d = close.diff()
    g = d.where(d>0, 0).rolling(14).mean()
    l = (-d.where(d<0, 0)).rolling(14).mean()
    return 100 - 100/(1+g/l)

all_factors = []
for code in [c for c in STOCKS if 'ETF' not in STOCK_NAMES[c]]:
    df = pd.read_sql_query(f"SELECT * FROM stock_daily WHERE ts_code='{code}' ORDER BY trade_date", conn)
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    if len(df) < 250: continue
    
    df.set_index('trade_date', inplace=True)
    close, vol = df['close'], df['vol']
    
    boll = calc_boll(close)
    df['boll_position'] = boll['pos']
    df['boll_bandwidth_pct'] = calc_pct(boll['bw'])
    
    macd = calc_macd(close)
    df['macd_dif'] = macd['dif']
    df['macd_dif_pct'] = calc_pct(macd['dif'])
    
    rsi = calc_rsi(close)
    df['rsi'] = rsi
    df['rsi_pct'] = calc_pct(rsi)
    
    df['momentum_20'] = close/close.shift(20)-1
    df['momentum_pct'] = calc_pct(df['momentum_20'])
    df['volume_ratio'] = vol/vol.rolling(20).mean()
    df['volume_pct'] = calc_pct(vol)
    df['volatility'] = close.pct_change().rolling(20).std()*np.sqrt(252)
    df['volatility_pct'] = calc_pct(df['volatility'])
    df['deviation_20'] = (close-close.rolling(20).mean())/close.rolling(20).mean()
    df['deviation_pct'] = calc_pct(df['deviation_20'])
    
    df.reset_index(inplace=True)
    df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d')
    all_factors.append(df[['ts_code','trade_date','boll_position','boll_bandwidth_pct','macd_dif','macd_dif_pct','rsi','rsi_pct','momentum_20','momentum_pct','volume_ratio','volume_pct','volatility','volatility_pct','deviation_20','deviation_pct']])

if all_factors:
    pd.concat(all_factors, ignore_index=True).to_sql('calc_technical_factors', conn, if_exists='replace', index=False)
    print("  ✅ 技术因子完成")

# 3. 置信度
print("\n[3/4] 计算综合置信度...")

def tech_score(row):
    b = 1-abs(row['boll_position']-0.5)*2
    r = 0.8 if row['rsi_pct']<0.2 else 0.2 if row['rsi_pct']>0.8 else 0.5
    return max(0,min(1, b*0.15+row['macd_dif_pct']*0.2+r*0.15+row['momentum_pct']*0.2+0.5*0.1+(1-row['volatility_pct'])*0.1+(1-row['deviation_pct'])*0.1))

latest = pd.read_sql_query("SELECT * FROM calc_technical_factors", conn)
ld = latest['trade_date'].max()
lt = latest[latest['trade_date']==ld].copy()

records = []
for _, row in lt.iterrows():
    ts = tech_score(row)
    conf = 0.5 + 0.5*0.4 + ts*0.3
    st = 'strong_buy' if conf>0.75 else 'buy' if conf>0.65 else 'watch'
    records.append({'ts_code':row['ts_code'],'calc_date':ld,'technical_score':round(ts,3),'fundamental_score':0.5,'event_score':0,'confidence':round(conf,3),'signal_type':st})

pd.DataFrame(records).to_sql('calc_confidence', conn, if_exists='replace', index=False)
print("  ✅ 置信度完成")

# 4. 信号
print("\n[4/4] 生成交易信号...")
signals = [{'signal_id':f"SIG_{r['calc_date']}_{r['ts_code']}",'signal_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'ts_code':r['ts_code'],'signal_type':r['signal_type'],'confidence':r['confidence'],
    'suggested_position':0.2 if r['signal_type']=='strong_buy' else 0.1,'stop_loss':0.1,'target_price':0.3,
    'factors_snapshot':f'{{"technical":{r["technical_score"]}}}','is_pushed':0,'is_executed':0} for r in records]

pd.DataFrame(signals).to_sql('signals', conn, if_exists='replace', index=False)

print("\n" + "=" * 60)
print("今日信号")
print("=" * 60)
result = pd.read_sql_query("SELECT * FROM calc_confidence ORDER BY confidence DESC", conn)
result['name'] = result['ts_code'].map(STOCK_NAMES)
print(result[['name','ts_code','signal_type','confidence','technical_score']].to_string(index=False))

conn.close()
print("\n✅ 每日更新完成")
