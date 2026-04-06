#!/usr/bin/env python3
"""每日自动更新脚本 v2 - 优化信号差异化"""

import os, pandas as pd, numpy as np, sqlite3, tushare as ts
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / '.env')
ts.set_token(os.getenv('TUSHARE_TOKEN'))
pro = ts.pro_api()
db_path = PROJECT_ROOT / 'data' / 'invest.db'
conn = sqlite3.connect(db_path)

STOCKS = ['002648.SZ', '002594.SZ', '000833.SZ', '300442.SZ', '516120.SH']
STOCK_NAMES = {'002648.SZ': '卫星化学', '002594.SZ': '比亚迪', '000833.SZ': '粤桂股份', '300442.SZ': '润泽科技', '516120.SH': '化工 50ETF'}

print("=" * 70)
print(f"每日更新 v2 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# 1. 采集日线
print("\n[1/4] 采集最新日线...")
end_date = datetime.now().strftime('%Y%m%d')
start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')

for code in STOCKS:
    try:
        df = pro.fund_daily(ts_code=code, start_date=start_date, end_date=end_date) if 'ETF' in STOCK_NAMES[code] else pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
        if len(df) > 0:
            df.to_sql('etf_daily' if 'ETF' in STOCK_NAMES[code] else 'stock_daily', conn, if_exists='append', index=False)
            print(f"  ✅ {STOCK_NAMES[code]}: {len(df)} 条")
    except Exception as e:
        print(f"  ⚠️  {STOCK_NAMES[code]}: {e}")

# 2. 计算因子
print("\n[2/4] 计算技术因子...")

def calc_pct(s, lb=250):
    return s.rolling(lb).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1] if len(x)>10 else np.nan)

def calc_score(df):
    scores = []
    for _, row in df.iterrows():
        s = (row['macd_dif_pct']*0.30 + row['momentum_pct']*0.25 + 
             (1-abs(row['boll_position']-0.5)*2)*0.15 + 
             (0.8 if row['rsi_pct']<0.2 else 0.2 if row['rsi_pct']>0.8 else 0.5)*0.15 +
             (1-row['volatility_pct'])*0.10 + (1-row['deviation_pct'])*0.05)
        scores.append(max(0, min(1, s)))
    return scores

all_f = []
for code in [c for c in STOCKS if 'ETF' not in STOCK_NAMES[c]]:
    df = pd.read_sql_query(f"SELECT * FROM stock_daily WHERE ts_code='{code}' ORDER BY trade_date", conn)
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    if len(df) < 250: continue
    df.set_index('trade_date', inplace=True)
    c, v = df['close'], df['vol']
    
    df['boll_position'] = (c - c.rolling(20).mean() + 2*c.rolling(20).std()) / (4*c.rolling(20).std())
    df['boll_bandwidth_pct'] = calc_pct((c.rolling(20).std()*4)/c.rolling(20).mean())
    df['macd_dif'] = c.ewm(span=12).mean() - c.ewm(span=26).mean()
    df['macd_dif_pct'] = calc_pct(df['macd_dif'])
    d = c.diff()
    df['rsi'] = 100 - 100/(1+d.where(d>0,0).rolling(14).mean()/(-d.where(d<0,0)).rolling(14).mean())
    df['rsi_pct'] = calc_pct(df['rsi'])
    df['momentum_20'] = c/c.shift(20)-1
    df['momentum_pct'] = calc_pct(df['momentum_20'])
    df['volume_ratio'] = v/v.rolling(20).mean()
    df['volume_pct'] = calc_pct(v)
    df['volatility'] = c.pct_change().rolling(20).std()*np.sqrt(252)
    df['volatility_pct'] = calc_pct(df['volatility'])
    df['deviation_20'] = (c-c.rolling(20).mean())/c.rolling(20).mean()
    df['deviation_pct'] = calc_pct(df['deviation_20'])
    
    df.reset_index(inplace=True)
    df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d')
    all_f.append(df[['ts_code','trade_date','boll_position','boll_bandwidth_pct','macd_dif','macd_dif_pct','rsi','rsi_pct','momentum_20','momentum_pct','volume_ratio','volume_pct','volatility','volatility_pct','deviation_20','deviation_pct']])

if all_f:
    pd.concat(all_f, ignore_index=True).to_sql('calc_technical_factors', conn, if_exists='replace', index=False)
    print("  ✅ 技术因子完成")

# 3. 优化后的置信度（差异化）
print("\n[3/4] 计算综合置信度（优化版）...")

latest = pd.read_sql_query("SELECT * FROM calc_technical_factors", conn)
ld = latest['trade_date'].max()
stocks_only = latest[~latest['ts_code'].str.contains('516120')].copy()
stocks_only = stocks_only[stocks_only['trade_date'] == ld].copy()

stocks_only['tech_score'] = calc_score(stocks_only)

# 历史分位
for idx, row in stocks_only.iterrows():
    code_hist = latest[latest['ts_code']==row['ts_code']].copy()
    code_hist['tech_score'] = calc_score(code_hist)
    pct = (pd.Series(code_hist['tech_score']) < row['tech_score']).mean()
    stocks_only.loc[idx, 'hist_pct'] = pct

stocks_only['final_score'] = stocks_only['tech_score']*0.6 + stocks_only['hist_pct']*0.4
stocks_only['rank'] = stocks_only['final_score'].rank(pct=True)

def get_signal(row):
    if row['rank'] >= 0.75 and row['final_score'] > 0.6: return 'strong_buy'
    elif row['rank'] >= 0.5 and row['final_score'] > 0.5: return 'buy'
    elif row['rank'] >= 0.25: return 'watch'
    else: return 'sell'

stocks_only['signal_type'] = stocks_only.apply(get_signal, axis=1)

records = [{'ts_code':r['ts_code'],'calc_date':ld,'technical_score':round(r['tech_score'],3),
    'fundamental_score':0.5,'confidence':round(r['final_score'],3),'signal_type':r['signal_type']} 
    for _,r in stocks_only.iterrows()]

pd.DataFrame(records).to_sql('calc_confidence_v2', conn, if_exists='replace', index=False)
print("  ✅ 置信度完成（差异化）")

# 4. 信号
print("\n[4/4] 生成交易信号...")
signals = [{'signal_id':f"SIGv2_{ld}_{r['ts_code']}",'signal_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'ts_code':r['ts_code'],'signal_type':r['signal_type'],'confidence':r['confidence'],
    'suggested_position':0.2 if r['signal_type']=='strong_buy' else 0.1 if r['signal_type']=='buy' else 0,
    'stop_loss':0.1,'target_price':0.3,'factors_snapshot':f'{{"tech":{r["technical_score"]}}}',
    'is_pushed':0,'is_executed':0} for r in records]

pd.DataFrame(signals).to_sql('signals_v2', conn, if_exists='replace', index=False)

print("\n" + "=" * 70)
print("今日信号（优化版 - 有差异化）")
print("=" * 70)
result = pd.DataFrame(records)
result['name'] = result['ts_code'].map(STOCK_NAMES)
result = result.sort_values('confidence', ascending=False)
print(result[['name','ts_code','signal_type','confidence','technical_score']].to_string(index=False))

print("\n" + "=" * 70)
print("信号分布")
print("=" * 70)
dist = result['signal_type'].value_counts()
for s in ['strong_buy','buy','watch','sell']:
    c = dist.get(s, 0)
    e = {'strong_buy':'🟢','buy':'🟡','watch':'⚪','sell':'🟠'}.get(s,'⚪')
    print(f"{e} {s:12}: {c} 只 ({c/len(result)*100:.0f}%)")

conn.close()
print("\n✅ 每日更新 v2 完成")
