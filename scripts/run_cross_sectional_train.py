#!/usr/bin/env python3
"""截面模型训练入口（Phase 2）。

使用原始市场特征（RAW_FEATURE_COLUMNS，20维）联合训练截面排序模型，
替代现有 per-stock XGBoost 回归的"垃圾放大器"架构。

步骤：
1. 从 DB 加载 core+etf 池的原始特征矩阵（via RawFeatureBuilder）
2. 构造截面排名标签（未来5日收益的截面百分位）
3. Walk-forward CV + 全量重训
4. 输出 CV IC / hit-rate，保存模型到 model_artifacts
5. 用当日特征预测截面强弱，输出推荐排序

使用方式：
    python3 scripts/run_cross_sectional_train.py
    python3 scripts/run_cross_sectional_train.py --start 20220101 --end 20260613
"""

import argparse
import sys
import os
import time
from datetime import datetime

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from invest_model.db import get_engine
from invest_model.models.ddl import create_all_tables
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.repositories.stock_daily_repo import StockDailyRepository
from invest_model.repositories.etf_repo import ETFRepository
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.ml.raw_features import RawFeatureBuilder, RAW_FEATURE_COLUMNS
from invest_model.ml.labels import make_forward_returns, LABEL_HORIZONS
from invest_model.ml.cross_sectional_trainer import CrossSectionalTrainer

# ── 参数 ──────────────────────────────────────────────
parser = argparse.ArgumentParser(description="截面模型训练")
parser.add_argument("--start",   default="20210101", help="训练开始日期 YYYYMMDD")
parser.add_argument("--end",     default=None,        help="训练结束日期（默认今日）")
parser.add_argument("--version", default="v2_cs",     help="模型版本标识")
parser.add_argument("--no-save", action="store_true", help="不保存模型到 DB（调试用）")
args = parser.parse_args()

TRAIN_START = args.start
TRAIN_END   = args.end or datetime.now().strftime("%Y%m%d")
VERSION     = args.version
SAVE        = not args.no_save

# ── 初始化 ──────────────────────────────────────────
engine = get_engine()
create_all_tables(engine)

pool_repo = StockPoolRepository(engine)
_core = pool_repo.get_pool("core")
_etf  = pool_repo.get_pool("etf")
all_pool = pd.concat([_core, _etf], ignore_index=True)
all_codes = all_pool["code"].tolist()
etf_codes_set = set(_etf["code"].tolist()) if not _etf.empty else set()
code_name_map = dict(zip(all_pool["code"], all_pool["name"]))

cal = CalendarRepository(engine)
all_trade_dates = cal.get_trade_dates(TRAIN_START, TRAIN_END)
if all_trade_dates:
    TRAIN_END = all_trade_dates[-1]

print("=" * 70)
print(f"  截面模型训练 [{VERSION}]")
print("=" * 70)
print(f"  训练区间: {TRAIN_START} ~ {TRAIN_END} ({len(all_trade_dates)} 个交易日)")
print(f"  标的池: {len(all_codes)} 只 (core {len(_core)} + etf {len(_etf)})")
for c in all_codes:
    print(f"    {c} {code_name_map.get(c, '')}")

# ════════════════════════════════════════════════
# STEP 1: 构造原始特征矩阵
# ════════════════════════════════════════════════
print(f"\n{'='*70}")
print("  STEP 1: 构造原始特征矩阵 (RawFeatureBuilder)")
print("=" * 70)
t0 = time.time()

builder = RawFeatureBuilder(engine)
daily_repo = StockDailyRepository(engine)
etf_repo = ETFRepository(engine)

stock_data: dict[str, dict] = {}
for code in all_codes:
    t1 = time.time()
    X = builder.build_history(code, TRAIN_START, TRAIN_END)
    if X.empty:
        print(f"  [skip] {code}: 特征矩阵为空")
        continue

    # 加载日线数据用于标签
    daily = daily_repo.get_daily(code, TRAIN_START, TRAIN_END)
    if daily.empty:
        daily = etf_repo.get_daily(code, TRAIN_START, TRAIN_END)
    if daily.empty:
        print(f"  [skip] {code}: 日线数据为空")
        continue

    labels = make_forward_returns(daily, horizons=LABEL_HORIZONS)
    labels = labels.set_index("trade_date")

    aligned = X.join(labels[["fwd_ret_5d"]], how="left")
    y_5d = aligned["fwd_ret_5d"]
    X_clean = aligned[list(X.columns)]

    n_valid = y_5d.notna().sum()
    is_etf = code in etf_codes_set
    print(f"  {code} {'[ETF]' if is_etf else '     '} {code_name_map.get(code, ''):8s} "
          f"| shape={X_clean.shape} | y_5d_valid={n_valid} | {time.time()-t1:.1f}s")

    stock_data[code] = {
        "X": X_clean,
        "y": {5: y_5d},
        "name": code_name_map.get(code, ""),
    }

print(f"\n特征构造完成: {len(stock_data)} 只, 耗时 {time.time()-t0:.1f}s")
if len(stock_data) < 2:
    print("[ERROR] 有效标的不足2只，无法进行截面训练")
    sys.exit(1)

# ════════════════════════════════════════════════
# STEP 2: 截面联合训练
# ════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  STEP 2: 截面联合训练 (XGBoost rank:pairwise, version={VERSION})")
print("=" * 70)
t0 = time.time()

trainer = CrossSectionalTrainer(
    engine=engine,
    n_splits=5,
    train_window=800,
    val_window=80,
    min_stocks_per_date=2,
)

result = trainer.fit(stock_data, version=VERSION, save=SAVE)

print(f"\n训练完成, 耗时 {time.time()-t0:.1f}s")
print(f"  版本:        {result.version}")
print(f"  标的数:      {result.n_stocks}")
print(f"  样本数:      {result.n_samples}")
print(f"  CV折数:      {result.cv_folds}")
print(f"  CV IC均值:   {result.cv_ic_mean:+.4f}")
print(f"  CV IC标准差: {result.cv_ic_std:.4f}")
print(f"  CV Hit Rate: {result.cv_hit_rate:.2%}")
print(f"  训练区间:    {result.train_start} ~ {result.train_end}")

print("\n  特征重要度 Top-10:")
for feat, imp in sorted(result.feature_importance.items(), key=lambda x: -x[1])[:10]:
    bar = "█" * int(imp * 50)
    print(f"    {feat:25s}: {imp:.4f}  {bar}")

# ════════════════════════════════════════════════
# STEP 3: 当日截面打分预测
# ════════════════════════════════════════════════
print(f"\n{'='*70}")
print("  STEP 3: 当日截面打分 (基于最新数据)")
print("=" * 70)

# 获取最新交易日
latest_dates = []
for c in all_codes:
    d = daily_repo.get_latest_date(code=c) or etf_repo.get_latest_date(c)
    if d:
        latest_dates.append(d)
today = max(latest_dates) if latest_dates else TRAIN_END

print(f"  基准日期: {today}")

# 构造今日截面特征
today_features: dict[str, pd.Series] = {}
for code in stock_data.keys():
    x = builder.build_single(code, today)
    if x is not None:
        today_features[code] = x

if today_features:
    from invest_model.ml.cross_sectional_trainer import CrossSectionalPredictor
    predictor = CrossSectionalPredictor(engine, version=VERSION)
    # 如果模型刚训练完，直接用 result.model 预测（不必重新从 DB 加载）
    predictor._model = result.model
    predictor._feat_cols = result.feature_cols

    scores = predictor.predict_today(today_features, today)

    print(f"\n  {'代码':12s} {'名称':10s} {'截面强度':8s} {'排名'}")
    print("  " + "-" * 50)
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    for rank, (code, score) in enumerate(ranked, 1):
        name = code_name_map.get(code, "")
        bar = "█" * int(score * 10)
        print(f"  {code:12s} {name:10s} {score:.3f}    #{rank:2d}  {bar}")
else:
    print("  无今日特征数据")

print(f"\n{'='*70}")
print(f"  ✅ 截面训练完成 (version={VERSION})！")
if result.cv_ic_mean > 0.05:
    print(f"  ✅ IC={result.cv_ic_mean:+.4f} > 0.05，模型质量合格")
elif result.cv_ic_mean > 0.02:
    print(f"  ⚠️  IC={result.cv_ic_mean:+.4f} 偏低，建议扩大标的池或延长训练期")
else:
    print(f"  ❌ IC={result.cv_ic_mean:+.4f} 过低，截面信号不可用，检查特征数据质量")
print("=" * 70)
