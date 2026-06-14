#!/usr/bin/env python3
"""从 notebook 14+15 提取的核心训练+回测脚本。

步骤：
1. 综合评分历史回填 (backfill)
2. 特征构造 + 标签生成
3. 训练 v1 (live) + v1_oos (截断到 2024-12-31)
4. 模型注册 + 落库
5. 三档回测 (normal / confident / strict)
6. 输出回测指标 + 今日信号
"""
import sys
import os
import time
from datetime import datetime

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from invest_model.db import get_engine
from invest_model.models.ddl import create_all_tables
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.repositories.etf_repo import ETFRepository

# ── 初始化 ──
engine = get_engine()
create_all_tables(engine)

pool_repo = StockPoolRepository(engine)
_core = pool_repo.get_pool('core')
_etf = pool_repo.get_pool('etf')
all_pool = pd.concat([_core, _etf], ignore_index=True)
# 严格限定为 core/etf 组，防止孤儿标的（曾入池后被移除）污染训练集
all_codes = all_pool['code'].tolist()
_valid_pool_codes = set(all_codes)
code_name_map = dict(zip(all_pool['code'], all_pool['name']))

TRAIN_START = '20210101'
OOS_CUTOFF  = '20241231'
TRAIN_END   = datetime.now().strftime('%Y%m%d')

cal = CalendarRepository(engine)
all_trade_dates = cal.get_trade_dates(TRAIN_START, TRAIN_END)
if all_trade_dates:
    TRAIN_END = all_trade_dates[-1]

oos_dates = cal.get_trade_dates(TRAIN_START, OOS_CUTOFF)
n_oos_train = len(oos_dates) if oos_dates else 0
n_oos_test = len(all_trade_dates) - n_oos_train

print(f'训练区间: {TRAIN_START} ~ {TRAIN_END} ({len(all_trade_dates)} 个交易日)')
print(f'OOS 切分点: {OOS_CUTOFF}')
print(f'标的池: {len(all_codes)} 只 (core {len(_core)} + etf {len(_etf)})')
for c in all_codes:
    print(f'  {c} {code_name_map.get(c, "")}')

# ════════════════════════════════════════════════
# STEP 1: 综合评分历史回填
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 1: 综合评分历史回填 (backfill)')
print('=' * 70)
t0 = time.time()

from invest_model.scoring.scorer import CompositeScorer
scorer = CompositeScorer(engine)
n = scorer.backfill_history(
    codes=all_codes,
    start_date=TRAIN_START,
    end_date=TRAIN_END,
    step_days=1,
    skip_existing=True,
    persist=True,
)
print(f'回填完成: 综合分 {n} 条, 耗时 {time.time()-t0:.1f}s')

# ════════════════════════════════════════════════
# STEP 2: 特征构造 + 标签生成
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 2: 特征构造 + 标签生成')
print('=' * 70)
t0 = time.time()

from invest_model.ml import FeatureBuilder, make_forward_returns, LABEL_HORIZONS

builder = FeatureBuilder(engine)
daily_repo = StockDailyRepository(engine)
etf_repo = ETFRepository(engine)

stock_data = {}
for code in all_codes:
    if code not in _valid_pool_codes:
        print(f'  [skip] {code}: 不在当前 core/etf 池中，跳过（幽灵标的防护）')
        continue
    X = builder.build_history(code, TRAIN_START, TRAIN_END)
    if X.empty:
        print(f'  [skip] {code}: 特征矩阵为空')
        continue

    daily = daily_repo.get_daily(code, TRAIN_START, TRAIN_END)
    if daily.empty:
        daily = etf_repo.get_daily(code, TRAIN_START, TRAIN_END)
    if daily.empty:
        print(f'  [skip] {code}: 日线为空')
        continue

    labels = make_forward_returns(daily, horizons=LABEL_HORIZONS)
    labels = labels.set_index('trade_date')

    aligned = X.join(labels, how='left')
    y_dict = {h: aligned[f'fwd_ret_{h}d'] for h in LABEL_HORIZONS}
    X_clean = aligned[list(X.columns)]

    stock_data[code] = {'X': X_clean, 'y': y_dict, 'name': code_name_map.get(code, '')}
    n_valid_5d = aligned['fwd_ret_5d'].notna().sum()
    print(f'  {code} {code_name_map.get(code, ""):8s} | features={X_clean.shape} | valid_5d={n_valid_5d}')

print(f'特征构造完成, 耗时 {time.time()-t0:.1f}s')

# ════════════════════════════════════════════════
# STEP 3: 模型训练 (v1 live + v1_oos)
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 3: 模型训练')
print('=' * 70)
t0 = time.time()

from invest_model.ml import (
    PerStockMultiHorizonTrainer,
    PurgedWalkForwardSplit,
    save_results,
    effective_feature_columns,
)

MODEL_VERSION_LIVE = 'v1'
MODEL_VERSION_OOS  = 'v1_oos'

_etf_codes_set = set(_etf['code'].tolist()) if not _etf.empty else set()

cv_live = PurgedWalkForwardSplit(n_splits=5, train_window=1000, val_window=60, embargo=10)
trainer_live = PerStockMultiHorizonTrainer(cv=cv_live, etf_codes=_etf_codes_set)

cv_oos = PurgedWalkForwardSplit(n_splits=5, train_window=600, val_window=60, embargo=10)
trainer_oos = PerStockMultiHorizonTrainer(cv=cv_oos, etf_codes=_etf_codes_set)


def _train_and_save(trainer, version, x_filter_end=None):
    tag = f'截断到 {x_filter_end}' if x_filter_end else '全量'
    print(f'  开始训练 [{version}] ({tag})  cv.train_window={trainer.cv.train_window}')

    results = []
    for code, data in stock_data.items():
        if x_filter_end:
            X_use = data['X'][data['X'].index <= x_filter_end]
            y_use = {h: y[y.index <= x_filter_end] for h, y in data['y'].items()}
        else:
            X_use = data['X']
            y_use = data['y']

        eff_cols = effective_feature_columns(code, etf_codes=_etf_codes_set)
        eff_cols = [c for c in eff_cols if c in X_use.columns]
        X_use = X_use[eff_cols]

        if X_use.empty:
            print(f'  [skip] {code}: 截断后无样本')
            continue

        n_valid = (~y_use[5].isna()).sum() if 5 in y_use else len(X_use)
        is_etf = code in _etf_codes_set
        ftag = 'ETF精简' if is_etf else '完整'
        print(f'  === {code} {data["name"]:8s}  样本 {len(X_use)} | y_5d_valid {n_valid} | 特征={X_use.shape[1]}维({ftag}) ===')
        res = trainer.fit(code, X_use, y_use)
        if res.horizons:
            results.append(res)

    n_saved = save_results(engine, results, version=version)
    print(f'  → 落库 {n_saved} 个模型 [version={version}]')
    return n_saved


n_oos = _train_and_save(trainer_oos, MODEL_VERSION_OOS, x_filter_end=OOS_CUTOFF)
n_live = _train_and_save(trainer_live, MODEL_VERSION_LIVE, x_filter_end=None)
print(f'\n训练汇总：v1 落库 {n_live} 个；v1_oos 落库 {n_oos} 个, 耗时 {time.time()-t0:.1f}s')

# ════════════════════════════════════════════════
# STEP 4: 模型注册表查看
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 4: 模型注册表')
print('=' * 70)

from invest_model.ml import list_registry

show_cols = ['code', 'horizon', 'n_samples', 'n_features',
             'cv_avg_ic', 'cv_avg_rmse', 'cv_hit_rate', 'train_start', 'train_end']

for version_label, version in [('live (v1)', MODEL_VERSION_LIVE), ('OOS (v1_oos)', MODEL_VERSION_OOS)]:
    print(f'\n  {version_label}')
    print('-' * 60)
    registry = list_registry(engine, codes=all_codes, version=version)
    if registry.empty:
        print(f'  无注册模型 (version={version})')
        continue
    for _, row in registry.iterrows():
        print(f'    {row["code"]:12s} h={row["horizon"]}d  IC={row["cv_avg_ic"]:+.4f}  '
              f'RMSE={row["cv_avg_rmse"]:.4f}  Hit={row["cv_hit_rate"]:.2%}  '
              f'samples={row["n_samples"]}')

# ════════════════════════════════════════════════
# STEP 5: 回测 (3 档)
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 5: 回测 (normal / confident / strict)')
print('=' * 70)
t0 = time.time()

from invest_model.backtest.engine import BacktestEngine, BacktestConfig
from invest_model.backtest.persistence import save_backtest_result
from invest_model.advisor import StockAdvisor, DecisionConfig

EXEC_TIERS = ['normal', 'confident', 'strict']
PRIMARY_TIER = 'normal'
BACKTEST_MODEL_VERSION = 'v1_oos'

DECISION_BASE = DecisionConfig(
    min_trade_size=0.05,
    score_to_position_scale=30.0,
    sell_score_threshold=-0.003,   # Phase1: narrowed dead-zone
    min_holding_days=3,
    min_flat_days=2,
    take_profit_min_conditions=3,
    buy_threshold=0.003,           # Phase1: narrowed dead-zone
    max_single_position=0.20,      # Phase A: single-stock cap
    stop_loss_threshold=-0.10,     # Phase A: hard stop-loss
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
        max_position_per_stock=0.20,   # Phase A: single-stock cap matches DecisionConfig
        benchmark_code='000300.SH',
        decision_config=DECISION_BASE,
        execution_tier=tier,
    )

results_by_tier = {}
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
    results_by_tier[_tier] = (_r, _engine_bt)
    save_backtest_result(engine, _r)
    print(f'  [{_tier:9s}] NAV {len(_r.nav_df)} 行, {len(_r.trades)} 笔交易, '
          f'累计收益 {_r.metrics.get("total_return", 0)*100:+.2f}%')

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
            _r = results_by_tier[_tier][0]
            vals.append(fn(_r.metrics.get(key)))
        print(f'  {label:14s}  {vals[0]:>12s}  {vals[1]:>12s}  {vals[2]:>12s}')

# 超额
print(f'\n  ── 超额对比 ──')
for _tier in EXEC_TIERS:
    _m = results_by_tier[_tier][0].metrics
    _bench = _m.get('benchmark_total_return')
    if _bench is not None:
        _excess = _m['total_return'] - _bench
        print(f'  [{_tier:9s}] 累计 {_m["total_return"]*100:+6.2f}%  vs 基准 {_bench*100:+6.2f}%  '
              f'超额 {_excess*100:+6.2f}%  Sharpe {_m.get("sharpe", 0):+.2f}')
    else:
        print(f'  [{_tier:9s}] 累计 {_m["total_return"]*100:+6.2f}%  (无基准)  '
              f'Sharpe {_m.get("sharpe", 0):+.2f}')

# ════════════════════════════════════════════════
# STEP 7: 今日信号 (基于最新 v1 模型 + 回测末日仓位)
# ════════════════════════════════════════════════
print('\n' + '=' * 70)
print('  STEP 7: 今日信号 (v1 live 模型)')
print('=' * 70)

# 从主档位回测结果推末日仓位
result_main, engine_bt_main = results_by_tier[PRIMARY_TIER]
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

# 从回测交易反推 last_action_dates
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
print()

print(f'{"代码":12s} {"名称":10s} {"操作":6s} {"置信度":6s} {"当前":6s} {"目标":6s} {"调仓":6s} {"score":8s}')
print('-' * 70)
for s in today_signals:
    delta_str = f'{s.delta_position:+.0%}' if abs(s.delta_position) >= 0.005 else '0%'
    print(f'{s.code:12s} {s.name:10s} {s.action_cn:6s} {s.confidence:5d}  '
          f'{s.current_position:5.0%}  {s.target_position:5.0%}  {delta_str:>5s}  '
          f'{s.horizon_score:+7.4f}')

print()
for s in today_signals:
    print(f'  [{s.action_cn}] {s.code} {s.name} (置信度 {s.confidence})')
    print(f'    {s.attribution}')
    print()

print('=' * 70)
print('  ✅ 全部完成！')
print('=' * 70)
