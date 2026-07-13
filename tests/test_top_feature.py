"""顶部特征即时判定（P16 提示版）单测。"""

import numpy as np
import pandas as pd

from invest_model.signals.top_feature import top_feature_now


def _series(n=400, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2022-01-03", periods=n).strftime("%Y%m%d")
    return idx, rng


def test_top_feature_fires_on_vol_and_volume_spike():
    idx, rng = _series()
    # 平缓上涨 +40%，末端 20 日波动骤放大 + 放量
    close = np.linspace(10.0, 14.0, len(idx)) * (1 + rng.normal(0, 0.003, len(idx)))
    close[-20:] *= (1 + np.linspace(0, 0.12, 20) + rng.normal(0, 0.05, 20))  # 末端剧烈波动
    vol = np.full(len(idx), 1e6)
    vol[-5:] *= 3.0                                                          # 末端放量
    c = pd.Series(close, index=idx)
    v = pd.Series(vol, index=idx)
    assert top_feature_now(c, v, cost=10.0, entry_date=idx[0]) is True


def test_top_feature_quiet_market_no_fire():
    idx, rng = _series(seed=1)
    close = np.linspace(10.0, 14.0, len(idx)) * (1 + rng.normal(0, 0.002, len(idx)))  # 平稳
    vol = np.full(len(idx), 1e6)
    c = pd.Series(close, index=idx)
    v = pd.Series(vol, index=idx)
    assert top_feature_now(c, v, cost=10.0, entry_date=idx[0]) is False


def test_top_feature_requires_profit():
    idx, rng = _series(seed=2)
    # 波动+放量都有，但没浮盈（价格没涨过成本 15%）→ 不触发
    close = (10.0 + rng.normal(0, 0.3, len(idx)))
    close[-20:] += np.linspace(0, 1.0, 20)
    vol = np.full(len(idx), 1e6)
    vol[-5:] *= 3.0
    c = pd.Series(close, index=idx)
    v = pd.Series(vol, index=idx)
    assert top_feature_now(c, v, cost=12.0, entry_date=idx[0]) is False


def test_no_volume_data_exempts_volume_condition():
    idx, rng = _series(seed=3)
    close = np.linspace(10.0, 14.0, len(idx)) * (1 + rng.normal(0, 0.003, len(idx)))
    close[-20:] *= (1 + np.linspace(0, 0.12, 20) + rng.normal(0, 0.05, 20))
    c = pd.Series(close, index=idx)
    v = pd.Series([np.nan] * len(idx), index=idx)     # 无量数据 → 放量条件豁免
    assert top_feature_now(c, v, cost=10.0, entry_date=idx[0]) is True
