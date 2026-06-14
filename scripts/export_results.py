#!/usr/bin/env python3
"""导出结果到 JSON 文件，供其他云端模型读取。"""
import json, os, sys
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from invest_model.config import get_mysql_url
from invest_model.repositories.stock_pool_repo import StockPoolRepository

def get_fresh_engine():
    return create_engine(get_mysql_url(), poolclass=NullPool)

def export():
    engine = get_fresh_engine()
    pool_repo = StockPoolRepository(engine)
    os.makedirs('results', exist_ok=True)

    with engine.connect() as conn:
        # Backtest results
        runs = conn.execute(text(
            'SELECT run_id, name, strategy, start_date, end_date, metrics, created_at '
            'FROM backtest_run ORDER BY run_id DESC LIMIT 6'
        )).fetchall()
        backtest_results = [
            {'run_id': r[0], 'name': r[1], 'strategy': r[2],
             'start_date': r[3], 'end_date': r[4],
             'metrics': json.loads(r[5]) if isinstance(r[5], str) else r[5],
             'created_at': str(r[6])} for r in runs
        ]

        # Stock pool
        pool = pool_repo.get_pool()
        pool_data = pool.to_dict('records')

        # Latest signals
        signals = conn.execute(text(
            'SELECT code, trade_date, action, confidence, position_pct, '
            'composite, target_position, current_position, delta_position, '
            'horizon_score, safety_margin, take_profit, '
            'pred_3d, pred_5d, pred_10d, attribution, created_at '
            'FROM stock_advisor_signal '
            'WHERE trade_date = (SELECT MAX(trade_date) FROM stock_advisor_signal) '
            'ORDER BY horizon_score DESC'
        )).fetchall()
        signal_data = [
            {'code': s[0], 'trade_date': s[1], 'action': s[2],
             'confidence': s[3], 'position_pct': float(s[4] or 0),
             'composite': float(s[5] or 0),
             'target_position': float(s[6] or 0),
             'current_position': float(s[7] or 0),
             'delta_position': float(s[8] or 0),
             'horizon_score': float(s[9] or 0),
             'safety_margin': float(s[10] or 0),
             'take_profit': bool(s[11]),
             'pred_3d': float(s[12] or 0), 'pred_5d': float(s[13] or 0),
             'pred_10d': float(s[14] or 0), 'attribution': s[15],
             'generated_at': str(s[16])} for s in signals
        ]

        # Model registry
        registry = conn.execute(text(
            'SELECT code, horizon, version, cv_avg_ic, cv_avg_rmse, cv_hit_rate, '
            'n_samples, n_features, train_start, train_end '
            'FROM ml_model_registry ORDER BY version, code, horizon'
        )).fetchall()
        model_registry = [
            {'code': m[0], 'horizon': m[1], 'version': m[2],
             'cv_avg_ic': float(m[3] or 0), 'cv_avg_rmse': float(m[4] or 0),
             'cv_hit_rate': float(m[5] or 0),
             'n_samples': m[6], 'n_features': m[7],
             'train_start': m[8], 'train_end': m[9]} for m in registry
        ]

    output = {
        'generated_at': datetime.now().isoformat(),
        'stock_pool': pool_data,
        'backtest_results': backtest_results,
        'latest_signals': signal_data,
        'model_registry': model_registry,
    }
    with open('results/latest.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    # NAV
    with engine.connect() as conn:
        nav = conn.execute(text(
            "SELECT trade_date, nav, ret, turnover, position_count "
            "FROM backtest_nav WHERE run_id = ("
            "  SELECT MAX(run_id) FROM backtest_run WHERE name LIKE '%normal%'"
            ") ORDER BY trade_date"
        )).fetchall()
        with open('results/backtest_nav_latest.json', 'w', encoding='utf-8') as f:
            json.dump(
                [{'trade_date': n[0], 'nav': float(n[1]),
                  'ret': float(n[2] or 0), 'turnover': float(n[3] or 0),
                  'position_count': n[4]} for n in nav],
                f, ensure_ascii=False, indent=2, default=str
            )

    # Trades
    with engine.connect() as conn:
        trades = conn.execute(text(
            "SELECT trade_date, code, action, weight, price "
            "FROM backtest_trades WHERE run_id = ("
            "  SELECT MAX(run_id) FROM backtest_run WHERE name LIKE '%normal%'"
            ") ORDER BY trade_date"
        )).fetchall()
        with open('results/backtest_trades_latest.json', 'w', encoding='utf-8') as f:
            json.dump(
                [{'trade_date': t[0], 'code': t[1], 'action': t[2],
                  'weight': float(t[3] or 0), 'price': float(t[4] or 0)} for t in trades],
                f, ensure_ascii=False, indent=2, default=str
            )

    engine.dispose()
    print(f'✅ results/latest.json — 池{len(pool_data)}/回测{len(backtest_results)}/信号{len(signal_data)}/模型{len(model_registry)}')
    print(f'✅ results/backtest_nav_latest.json — {len(nav)} 天净值')
    print(f'✅ results/backtest_trades_latest.json — {len(trades)} 笔交易')

if __name__ == '__main__':
    export()
