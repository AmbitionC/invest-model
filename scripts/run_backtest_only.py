#!/usr/bin/env python3
"""回测 + 今日信号（训练已完成，直接跑 STEP 5-7）"""
import sys, os, time
from datetime import datetime
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from invest_model.db import get_engine
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.repositories.etf_repo import ETFRepository

engine = get_engine()
pool_repo = StockPoolRepository(engine)
_core = pool_repo.get_pool('core')
_etf = pool_repo.get_pool('etf')
all_pool = pd.concat([_core, _etf], ignore_index=True)
all_codes = all_pool['code'].tolist()
code_name_map = dict(zip(all_pool['code'], all_pool['name']))

# ── 直接从子模块导入（绕过新的 __init__）──
from invest_model.backtest.engine import BacktestEngine, BacktestConfig
from invest_model.backtest.metrics import compute_metrics, compute_per_stock_contribution
from invest_model.backtest.persistence import save_backtest_result
from invest_model.advisor import StockAdvisor
from invest_model.advisor.decision import DecisionConfig

EXEC_TIERS = ['normal', 'confident', 'strict']
PRIMARY_TIER = 'normal'
BACKTEST_MODEL_VERSION = 'v1_oos'

DECISION_BASE = DecisionConfig(
    min_trade_size=0.05,
    score_to_position_scale=30.0,
    sell_score_threshold=-0.005,
    min_holding_days=3,
    min_flat_days=2,
    take_profit_min_conditions=3,
    buy_threshold=0.005,
)

def make_cfg(tier):
    return BacktestConfig(
        name=f'ml_advisor_v1_oos_{tier}',
        strategy='ml_target_position',
        start_date='20250101',
        end_date=datetime.now().strftime('%Y%m%d'),
        initial_capital=1_000_000.0,
        fee_rate=0.0003,
        stamp_tax=0.001,
        slippage=0.0005,
        rebalance_days=1,
        min_position_change=DECISION_BASE.min_trade_size,
        max_position_per_stock=0.5,
        benchmark_code='000300.SH',
        decision_config=DECISION_BASE,
        execution_tier=tier,
    )

# ════════════════════════════════════════════════
# STEP 5: 回测 (3 档)
# ════════════════════════════════════════════════
print('=' * 70)
print('  STEP 5: 回测 (normal / confident / strict)')
print('=' * 70)
t0 = time.time()

results_by_tier = {}
engines_by_tier = {}

for _tier in EXEC_TIERS:
    _cfg = make_cfg(_tier)
    _advisor = StockAdvisor(
        engine, version=BACKTEST_MODEL_VERSION,
        decision_config=DECISION_BASE,
        ic_weighting=True,
    )
    _engine_bt = BacktestEngine(
        engine=engine, advisor=_advisor, config=_cfg,
        codes=all_codes, code_name_map=code_name_map,
    )
    _r = _engine_bt.run()
    results_by_tier[_tier] = _r
    engines_by_tier[_tier] = _engine_bt
    run_id = save_backtest_result(engine, _r)
    print(f'  [{_tier:9s}] NAV {len(_r.nav_df)} 行, {len(_r.trades)} 笔交易, '
          f'累计收益 {_r.metrics.get("total_return", 0)*100:+.2f}%, '
          f'换手 {_r.metrics.get("turnover_total", 0):.2f}, run_id={run_id}')

print(f'回测完成, 耗时 {time.time()-t0:.1f}s')

# ════════════════════════════════════════════════
# STEP 6: 回测指标汇总
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 6: 回测指标汇总')
print('=' * 70)

def _pct(v): return f'{v * 100:+.2f}%' if v is not None else '—'
def _pct_abs(v): return f'{v * 100:.2f}%' if v is not None else '—'
def _num(v): return f'{v:+.4f}' if v is not None else '—'
def _x(v): return f'{v:.2f}' if v is not None else '—'

groups = [
    ('收益', [
        ('total_return',  '累计收益',     _pct),
        ('annual_return', '年化收益',     _pct),
        ('annual_vol',    '年化波动率',   _pct_abs),
    ]),
    ('风险', [
        ('max_drawdown',  '最大回撤',     _pct_abs),
        ('sharpe',        '夏普比率',     _num),
        ('sortino',       '索提诺比率',   _num),
        ('calmar',        '卡玛比率',     _num),
    ]),
    ('交易', [
        ('win_rate',           '日度胜率',     _pct_abs),
        ('avg_position_count', '日均持仓只数', _x),
        ('turnover_total',     '累计换手',     _x),
        ('n_days',             '回测交易日',   str),
    ]),
]

print(f'\n{"指标":16s}  {"normal":>12s}  {"confident":>12s}  {"strict":>12s}')
print('-' * 60)
for grp, items in groups:
    print(f'  ── {grp} ──')
    for key, label, fn in items:
        vals = []
        for _tier in EXEC_TIERS:
            _r = results_by_tier[_tier]
            vals.append(fn(_r.metrics.get(key)))
        print(f'  {label:14s}  {vals[0]:>12s}  {vals[1]:>12s}  {vals[2]:>12s}')

# 超额
print(f'\n  ── 超额对比 ──')
for _tier in EXEC_TIERS:
    _m = results_by_tier[_tier].metrics
    _bench = _m.get('benchmark_total_return')
    if _bench is not None:
        _excess = _m['total_return'] - _bench
        print(f'  [{_tier:9s}] 累计 {_m["total_return"]*100:+6.2f}%  vs 基准 {_bench*100:+6.2f}%  '
              f'超额 {_excess*100:+6.2f}%  Sharpe {_m.get("sharpe", 0):+.2f}  '
              f'换手 {_m.get("turnover_total", 0):.2f}')
    else:
        print(f'  [{_tier:9s}] 累计 {_m["total_return"]*100:+6.2f}%  (无基准)  '
              f'Sharpe {_m.get("sharpe", 0):+.2f}  换手 {_m.get("turnover_total", 0):.2f}')

# ════════════════════════════════════════════════
# STEP 7: 今日信号 (v1 live 模型)
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 7: 今日信号 (v1 live 模型)')
print('=' * 70)

result_main = results_by_tier[PRIMARY_TIER]
engine_bt_main = engines_by_tier[PRIMARY_TIER]

end_positions = {c: 0.0 for c in all_codes}
for _t in sorted(result_main.trades, key=lambda x: x.trade_date):
    end_positions[_t.code] = float(_t.weight_after)

_daily_repo = StockDailyRepository(engine)
_etf_repo = ETFRepository(engine)
_latest_dates = []
for _c in all_codes:
    _d = _daily_repo.get_latest_date(code=_c) or _etf_repo.get_latest_date(_c)
    if _d:
        _latest_dates.append(_d)
today_date = max(_latest_dates) if _latest_dates else result_main.nav_df['trade_date'].iloc[-1]

advisor_live = StockAdvisor(
    engine, version='v1',
    decision_config=DECISION_BASE,
    ic_weighting=True,
)

_last_action_dates = {c: {'open': None, 'clear': None} for c in all_codes}
for _t in sorted(result_main.trades, key=lambda x: x.trade_date):
    prev_w = _t.weight_before
    new_w = _t.weight_after
    if prev_w <= 1e-6 and new_w > 1e-6:
        _last_action_dates[_t.code]['open'] = _t.trade_date
    if prev_w > 1e-6 and new_w <= 1e-6:
        _last_action_dates[_t.code]['clear'] = _t.trade_date

today_signals = advisor_live.advise_batch(
    all_codes, today_date, code_name_map,
    current_positions=end_positions,
    last_action_dates=_last_action_dates,
)

print(f'\n  日期: {today_date}')
print(f'  模型: v1 (live)')
print(f'  回测末日仓位:')
total_w = 0.0
for c, w in end_positions.items():
    total_w += w
    if w >= 0.005:
        print(f'    → {c} {code_name_map.get(c, ""):8s} {w:7.2%}')
print(f'    合计持仓: {total_w:.2%}    现金: {1.0 - total_w:.2%}')
print()

print(f'{"代码":12s} {"名称":10s} {"操作":6s} {"置信度":6s} {"当前":6s} {"目标":6s} {"调仓":6s} {"score":8s}')
print('-' * 75)
for s in today_signals:
    delta_str = f'{s.delta_position:+.0%}' if abs(s.delta_position) >= 0.005 else '0%'
    print(f'{s.code:12s} {s.name:10s} {s.action_cn:6s} {s.confidence:5d}  '
          f'{s.current_position:5.0%}  {s.target_position:5.0%}  {delta_str:>5s}  '
          f'{s.horizon_score:+7.4f}')

print()
print('逐票归因:')
for s in today_signals:
    print(f'  [{s.action_cn}] {s.code} {s.name} (置信度 {s.confidence})')
    print(f'    {s.attribution}')
    print()

print('=' * 70)
print('  ✅ 全部完成！')
print('=' * 70)
