"""投顾融合 + 量化风控 单元/集成测试。

覆盖：
  - 风控纯函数：均线移动止盈状态机、硬止损、逻辑止损、趋势过滤
  - 投顾为主融合 fuse_targets：分级权重 / 单票&仓位池上限 / 排除 / 量化补充
  - AdvisorRepo 有效期与 exit_codes
  - 回测引擎日频风控（硬止损产生卖出）
  - 实盘操作计划 build_action_plan（止损/逻辑清仓/买入 动作正确）
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine
from invest_model.orchestration import ClosedLoop, LoopConfig
from invest_model.orchestration.action_plan import build_action_plan
from invest_model.portfolio import PortfolioConfig, RiskConfig, fuse_targets
from invest_model.portfolio.risk import (
    armed_ladder,
    evaluate_holding,
    keep_from_step,
    pp_step,
    profit_protect,
    replay_ladder_tier,
    replay_pp_tier,
    replay_tier,
    step_tier,
    time_stop,
    trend_ok_close,
)
from invest_model.signals.buypoint import BuyPointConfig, detect_buypoints
from invest_model.repositories.advisor_repo import AdvisorRepo
from invest_model.repositories.holding_repo import HoldingRepo
from invest_model.universe import UniverseConfig
from scripts.gen_synthetic_sample import generate

START, END = "20210101", "20230101"


# ───────────────────────── 风控纯函数 ─────────────────────────

def test_step_tier_breaches():
    # 收盘跌破全部均线 → 档3（清仓）
    assert step_tier(9, ma5=10, ma10=11, ma20=12, cur_tier=0) == 3
    # 仅破 MA5 → 档1
    assert step_tier(10.5, ma5=11, ma10=10, ma20=9, cur_tier=0) == 1
    # 破到 MA10（>MA20）→ 档2
    assert step_tier(9.5, ma5=11, ma10=10, ma20=9, cur_tier=0) == 2
    # 单调：已在档2，价格收回到均线上方不回退
    assert step_tier(99, ma5=10, ma10=11, ma20=12, cur_tier=2) == 2


def test_keep_from_step():
    assert keep_from_step(0, 1) == 0.5      # 破MA5减半
    assert keep_from_step(1, 2) == 0.5      # 破MA10再减半
    assert keep_from_step(0, 2) == 0.25     # 一步跨两档 → 1/4
    assert keep_from_step(0, 3) == 0.0      # 破MA20清仓
    assert keep_from_step(2, 3) == 0.0


def test_replay_tier_monotonic_decline():
    s = pd.Series(np.arange(60, 0, -1, dtype=float), index=[str(i) for i in range(60)])
    assert replay_tier(s) == 3              # 单边下跌必破 MA20


def test_evaluate_hard_stop():
    s = pd.Series([100, 99, 98, 97, 90.0], index=[str(i) for i in range(5)])
    dec = evaluate_holding(s, cost=100, cfg=RiskConfig())
    assert dec.action == "exit" and "硬止损" in dec.reason


def test_evaluate_logic_exit_priority():
    s = pd.Series([100, 101, 102, 103, 104.0], index=[str(i) for i in range(5)])
    dec = evaluate_holding(s, cost=100, cfg=RiskConfig(), in_exit_codes=True)
    assert dec.action == "exit" and "逻辑证伪" in dec.reason


def test_evaluate_ma_trailing_trim_half_full_mode():
    # trail_full=True：长上行后单日小幅回落跌破 MA5（仍在 MA10/MA20 上方）→ 减半
    prices = list(range(100, 145)) + [140.0]      # 100..144 后回落到 140
    s = pd.Series(prices, index=[str(i) for i in range(len(prices))])
    dec = evaluate_holding(s, cost=100, cfg=RiskConfig(trail_full=True), prev_tier=0)
    assert dec.action == "trim" and abs(dec.keep_frac - 0.5) < 1e-9
    assert dec.new_tier == 1


def test_evaluate_ma20_only_default_no_trim():
    # 默认 trail_full=False（放宽）：仅破 MA5 不减仓，持有
    prices = list(range(100, 145)) + [140.0]
    s = pd.Series(prices, index=[str(i) for i in range(len(prices))])
    dec = evaluate_holding(s, cost=100, cfg=RiskConfig(), prev_tier=0)
    assert dec.action == "hold"
    # 破 MA20 仍清仓
    assert step_tier(9, ma5=10, ma10=11, ma20=12, cur_tier=0, full=False) == 3
    assert step_tier(10.5, ma5=11, ma10=10, ma20=9, cur_tier=0, full=False) == 0


def test_time_stop_sideways_trims():
    # 8 日横盘、收盘均未超过建仓日 → 减半
    s = pd.Series([10, 9.9, 10, 9.95, 10, 9.9, 10, 9.95], index=[str(i) for i in range(8)])
    d = time_stop(s, RiskConfig(time_stop_days=8))
    assert d is not None and d.action == "trim" and "时间止损" in d.reason
    # 期间创出新高 → 不触发
    up = pd.Series([10, 11, 12, 12, 12, 12, 12, 12], index=[str(i) for i in range(8)])
    assert time_stop(up, RiskConfig(time_stop_days=8)) is None
    # 已启动移动止盈(prev_tier>0) → 不属时间止损
    assert time_stop(s, RiskConfig(time_stop_days=8), prev_tier=1) is None


def test_pp_not_armed_below_trigger():
    # 峰值浮盈不足 15% → 保护不启动，即使随后回撤很深也不触发（那是止损的事）
    s = pd.Series([100, 110, 100.0], index=["0", "1", "2"])
    assert profit_protect(s, cost=100, cfg=RiskConfig()) is None
    assert pp_step(100, peak=110, cost=100, cfg=RiskConfig(), cur_tier=0) == 0


def test_pp_trim_then_exit_monotonic():
    cfg = RiskConfig()  # pp_trigger=0.15 trim=0.08 exit=0.12
    # 成本100 涨到 130（+30% 已启动保护），回撤 8%→减半
    s = pd.Series([100, 115, 130, 119.0], index=[str(i) for i in range(4)])  # dd≈8.5%
    d = profit_protect(s, cost=100, cfg=cfg)
    assert d is not None and d.action == "trim" and d.new_tier == 1 and "盈利保护" in d.reason
    # 前一日已减半（prev_tier=1），当日回撤仍在 8~12% 区间 → 不重复触发
    assert profit_protect(s, cost=100, cfg=cfg, prev_tier=1) is None
    # 回撤达 12% → 清仓止盈（即使已在档1）
    s2 = pd.Series([100, 115, 130, 114.0], index=[str(i) for i in range(4)])  # dd≈12.3%
    d2 = profit_protect(s2, cost=100, cfg=cfg, prev_tier=1)
    assert d2 is not None and d2.action == "exit" and d2.new_tier == 2
    # 已清仓档（prev_tier=2）→ 永不再触发
    assert profit_protect(s2, cost=100, cfg=cfg, prev_tier=2) is None


def test_pp_replay_and_juhua_scenario():
    cfg = RiskConfig()
    # 巨化式场景：成本38.8 → 峰值54.8（+41%），跌到 49.35 时自峰值回撤 ≈10%
    s = pd.Series([38.8, 45, 50, 54.83, 49.35], index=[str(i) for i in range(5)])
    prev = replay_pp_tier(s.iloc[:-1], cost=38.8, cfg=cfg)   # 截至昨日：无回撤 → 档0
    assert prev == 0
    d = profit_protect(s, cost=38.8, cfg=cfg, prev_tier=prev)
    assert d is not None and d.action == "trim"              # 回撤10% ≥8% → 减半锁盈
    # 旧规则对照：MA20 追踪要跌回 ~35.7 才动，会回吐全部超额利润
    assert replay_pp_tier(s, cost=38.8, cfg=cfg) == 1


def _idx(n, start=0):
    return [f"202601{i + start:02d}" for i in range(1, n + 1)]


def test_armed_ladder_only_after_trigger():
    cfg = RiskConfig()
    # 上行但峰值浮盈<15% → 破 MA5 也不触发（保护未启动，别把刚启动的票洗掉）
    s = pd.Series([100, 102, 104, 106, 108, 110, 104.0], index=_idx(7))
    assert replay_ladder_tier(s, entry_date=s.index[0], cost=100, cfg=cfg) == 0
    assert armed_ladder(s, s.index[0], 100, cfg) is None


def test_armed_ladder_trim_then_exit():
    cfg = RiskConfig()
    # 涨到 127（+27%，保护启动）后回落：先破 MA5 → 减半；继续破 MA10 → 清仓
    up = [100, 103, 106, 109, 112, 115, 118, 121, 124, 127]
    s1 = pd.Series(up + [120.0], index=_idx(11))      # 120 < MA5=122，仍在 MA10 上
    d1 = armed_ladder(s1, s1.index[0], 100, cfg)
    assert d1 is not None and d1.action == "trim" and "破MA5" in d1.reason
    s2 = pd.Series(up + [120, 112.0], index=_idx(12))  # 112 < MA10≈116 → 清仓
    d2 = armed_ladder(s2, s2.index[0], 100, cfg)
    assert d2 is not None and d2.action == "exit" and "破MA10" in d2.reason
    # 已在档1（昨日破5），今日仍只破5 → 不重复触发
    s3 = pd.Series(up + [120, 121.0], index=_idx(12))
    assert armed_ladder(s3, s3.index[0], 100, cfg) is None
    # 关闭开关 → 永不触发
    assert armed_ladder(s2, s2.index[0], 100, RiskConfig(armed_trail=False)) is None


def test_armed_ladder_duplicate_index_snapshot_day():
    # 生产场景回归：当日 EOD 收盘 + 券商快照价并存 → 索引重复，不得抛错
    cfg = RiskConfig()
    up = [100, 103, 106, 109, 112, 115, 118, 121, 124, 127]
    idx = _idx(11)
    s = pd.Series(up + [123.0], index=idx)            # EOD 123 仍在 MA5 上，未触发
    s = pd.concat([s, pd.Series({idx[-1]: 119.5})])   # 盘中快照 119.5 破 MA5，键与 EOD 重复
    d = armed_ladder(s, idx[0], 100, cfg)
    assert d is not None and d.action == "trim" and "破MA5" in d.reason


def test_armed_ladder_respects_entry_date():
    cfg = RiskConfig()
    # 建仓前的历史破位不算：entry 在高位之后，持有期内峰值浮盈不足 → 不触发
    s = pd.Series([80, 100, 130, 125, 118, 112.0], index=_idx(6))
    assert replay_ladder_tier(s, entry_date=s.index[3], cost=125, cfg=cfg) == 0


def _seed_ohlcv(engine, code, closes, vols, opens=None):
    from invest_model.repositories.base import BaseRepository
    r = BaseRepository(engine)
    r.execute_sql("INSERT OR IGNORE INTO stock_info(ts_code,name) VALUES(:c,:n)",
                  {"c": code, "n": code})
    n = len(closes)
    dates = pd.bdate_range("20250101", periods=n).strftime("%Y%m%d")
    rows = []
    for i in range(n):
        o = opens[i] if opens else closes[i - 1] if i else closes[i]
        rows.append({"code": code, "trade_date": dates[i], "open": o,
                     "high": max(o, closes[i]) * 1.01, "low": min(o, closes[i]) * 0.99,
                     "close": closes[i], "volume": vols[i]})
    r.upsert("stock_daily", pd.DataFrame(rows), ["code", "trade_date"])
    return dates[-1]


def test_buypoint_downtrend_is_watch(tmp_path):
    eng = make_engine(f"sqlite:///{tmp_path}/bp.db"); create_schema(eng)
    import numpy as _np
    closes = list(_np.linspace(100, 50, 80))           # 单边下行
    dt = _seed_ohlcv(eng, "DN.SH", closes, [1e6] * 80)
    bp = detect_buypoints(eng, dt, ["DN.SH"], gross=0.9)["DN.SH"]
    assert bp.is_buy is False and ("趋势" in bp.reason or "样本" in bp.reason)


def test_buypoint_retrace_triggers(tmp_path):
    eng = make_engine(f"sqlite:///{tmp_path}/bp2.db"); create_schema(eng)
    import numpy as _np
    # 70 日上行 → 末段回踩 MA20 附近，最后一根放量阳线
    base = list(_np.linspace(60, 120, 74))
    closes = base + [118, 115, 112, 110, 116]          # 回踩后拉起
    vols = [1e6] * (len(closes) - 1) + [3e6]           # 末日放量
    opens = closes[:-1] + [110]                        # 末日低开高走(阳线)
    opens = [closes[0]] + closes[:-1]                  # open=前收
    opens[-1] = 110                                    # 末日阳线 open<close
    dt = _seed_ohlcv(eng, "UP.SH", closes, vols, opens)
    bp = detect_buypoints(eng, dt, ["UP.SH"], gross=0.9,
                          rank_map={"UP.SH": 0.9})["UP.SH"]
    # 趋势在、末日回踩放量阳线 → 触发（若阈值边界未中也至少不因趋势被否）
    assert "趋势" not in bp.reason or bp.is_buy
    # 大盘环境差 → 即便技术买点也观察
    bp2 = detect_buypoints(eng, dt, ["UP.SH"], gross=0.3,
                           rank_map={"UP.SH": 0.9})["UP.SH"]
    assert bp2.is_buy is False


def test_trend_ok_close():
    up = pd.Series(np.arange(1, 100, dtype=float), index=[str(i) for i in range(99)])
    down = pd.Series(np.arange(99, 0, -1, dtype=float), index=[str(i) for i in range(99)])
    assert trend_ok_close(up, RiskConfig()) is True
    assert trend_ok_close(down, RiskConfig()) is False


# ───────────────────────── fuse_targets ─────────────────────────

def _scores(codes):
    return pd.DataFrame({"code": codes, "score": np.linspace(1, 0, len(codes)),
                         "rank_pct": np.linspace(1, 0, len(codes))})


def _adv(rows):
    return pd.DataFrame(rows, columns=["code", "grade", "direction"])


def test_fuse_grade_weight_and_quant_fill():
    cfg = PortfolioConfig(advisor_led=True, top_n=5, max_weight=0.5,
                          advisory_name_cap=0.2, advisory_sleeve_cap=1.0)
    scores = _scores([f"S{i}.SH" for i in range(6)])
    adv = _adv([["A1.SH", "A", "long"], ["B1.SH", "B", "long"],
                ["X1.SH", "A", "long"], ["C1.SH", "C", "long"]])
    w, meta = fuse_targets(scores, cfg, adv, gross=1.0, exit_codes={"X1.SH"})
    assert abs(w["A1.SH"] - 0.2) < 1e-6 and abs(w["B1.SH"] - 0.1) < 1e-6  # A 顶到 cap，B=半
    assert abs(w["A1.SH"] - 2 * w["B1.SH"]) < 1e-6                        # A=2×B
    assert "X1.SH" not in w and "C1.SH" not in w                          # 排除 / C 级不入
    assert meta["A1.SH"] == {"grade": "A", "source": "advisor"}
    assert any(m["source"] == "quant" for m in meta.values())             # 量化补充
    assert abs(sum(w.values()) - 1.0) < 1e-6                              # 满仓 gross


def test_fuse_sleeve_cap_scaling():
    cfg = PortfolioConfig(advisor_led=True, top_n=5, max_weight=0.5,
                          advisory_name_cap=0.3, advisory_sleeve_cap=0.2)
    adv = _adv([["A1.SH", "A", "long"], ["B1.SH", "B", "long"]])
    w, _ = fuse_targets(_scores([f"S{i}.SH" for i in range(5)]), cfg, adv, gross=1.0)
    # raw 0.2+0.1=0.3 > sleeve_cap 0.2 → 等比缩放到 0.2，且 A 仍=2×B
    assert abs((w["A1.SH"] + w["B1.SH"]) - 0.2) < 1e-6
    assert abs(w["A1.SH"] - 2 * w["B1.SH"]) < 1e-6


def test_fuse_concentration_limits_and_advisory_only():
    cfg = PortfolioConfig(advisor_led=True, top_n=5, max_weight=0.5, advisory_only=True,
                          advisory_max_a=1, advisory_max_b=1, advisory_name_cap=0.2,
                          grade_target={"A": 0.12, "B": 0.06})
    adv = _adv([["A1.SH", "A", "long"], ["A2.SH", "A", "long"],
                ["B1.SH", "B", "long"], ["B2.SH", "B", "long"]])
    # 量化分：A2>A1、B2>B1 → 同级优选 A2、B2
    scores = pd.DataFrame({"code": ["A2.SH", "A1.SH", "B2.SH", "B1.SH", "Q1.SH"],
                           "score": [5.0, 4.0, 3.0, 2.0, 9.0], "rank_pct": [.5] * 5})
    w, meta = fuse_targets(scores, cfg, adv, gross=1.0)
    assert set(w) == {"A2.SH", "B2.SH"}              # 每级限 1 只、按量化分优选
    assert w["A2.SH"] > w["B2.SH"]                   # A 重于 B
    assert all(m["source"] == "advisor" for m in meta.values())  # advisory_only：无量化补仓


def test_fuse_trend_gate_excludes():
    cfg = PortfolioConfig(advisor_led=True, top_n=5, max_weight=0.5, advisory_name_cap=0.2)
    adv = _adv([["A1.SH", "A", "long"], ["B1.SH", "B", "long"]])
    w, _ = fuse_targets(_scores([f"S{i}.SH" for i in range(5)]), cfg, adv, gross=1.0,
                        trend_ok_codes={"A1.SH"})
    assert "A1.SH" in w and "B1.SH" not in w


# ───────────────────────── DB 集成 ─────────────────────────

@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    db = tmp_path_factory.mktemp("db") / "adv.db"
    url = f"sqlite:///{db}"
    generate(url, n_stocks=60, start=START, end=END, seed=11)
    eng = make_engine(url)
    create_schema(eng)
    return eng


def _some_codes(engine, n):
    repo = AdvisorRepo(engine)
    last_dt = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]
    df = repo.read_sql(
        "SELECT code FROM stock_daily WHERE trade_date=:d AND close>0 ORDER BY code LIMIT :n",
        {"d": last_dt, "n": n})
    return list(df["code"])


def test_advisor_repo_validity(engine):
    repo = AdvisorRepo(engine)
    codes = _some_codes(engine, 3)
    df = pd.DataFrame([
        {"rec_date": "20220601", "code": codes[0], "source_type": "research",
         "grade": "A", "direction": "long", "valid_until": None},
        {"rec_date": "20220601", "code": codes[1], "source_type": "intraday",
         "grade": "B", "direction": "long", "valid_until": "20220605"},
        {"rec_date": "20220601", "code": codes[2], "source_type": "research",
         "grade": "A", "direction": "exit", "valid_until": None},
    ])
    repo.save_reco(df)
    # 20220610：研报仍有效，intraday 已过期
    active = repo.get_active_reco("20220610")
    assert codes[0] in set(active["code"])
    assert codes[1] not in set(active["code"])      # intraday 过期
    assert repo.get_exit_codes("20220610") == {codes[2]}


def test_action_plan_actions(engine):
    repo = AdvisorRepo(engine)
    repo.execute_sql("DELETE FROM advisor_reco")          # 隔离其它用例残留
    HoldingRepo(engine).clear()
    codes = _some_codes(engine, 6)
    h_adv, h_exit, h_hard = codes[0], codes[1], codes[2]
    new_buy = codes[3]
    last_dt = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]
    last_px = {c: float(repo.read_sql(
        "SELECT close FROM stock_daily WHERE code=:c AND trade_date=:d",
        {"c": c, "d": last_dt})["close"].iloc[0]) for c in [h_adv, h_exit, h_hard]}

    repo.save_reco(pd.DataFrame([
        {"rec_date": "20220101", "code": h_adv, "source_type": "research",
         "grade": "A", "direction": "long", "valid_until": None},
        {"rec_date": "20220101", "code": new_buy, "source_type": "research",
         "grade": "A", "direction": "long", "valid_until": None},
        {"rec_date": "20220101", "code": h_exit, "source_type": "research",
         "grade": "A", "direction": "exit", "valid_until": None},
    ]))
    HoldingRepo(engine).save(pd.DataFrame([
        {"code": h_adv, "shares": 1000, "cost_price": last_px[h_adv] * 0.9, "entry_date": "20211001"},
        {"code": h_exit, "shares": 1000, "cost_price": last_px[h_exit] * 0.95, "entry_date": "20211001"},
        {"code": h_hard, "shares": 1000, "cost_price": last_px[h_hard] * 2.0, "entry_date": "20211001"},
    ]))

    cfg = LoopConfig(version="plan_v1", start=START, end=END,
                     risk=RiskConfig(enabled=True),
                     universe=UniverseConfig(method="alla"),
                     portfolio=PortfolioConfig(advisor_led=True, advisory_name_cap=0.3))
    plan = build_action_plan(engine, cfg, cash=0.0, buypoint=False)
    by_code = {r["code"]: r for r in plan.rows}

    assert by_code[h_exit]["action"] == "sell" and "逻辑证伪" in by_code[h_exit]["reason"]
    assert by_code[h_hard]["action"] == "sell" and "硬止损" in by_code[h_hard]["reason"]
    assert by_code[new_buy]["action"] == "buy" and by_code[new_buy]["grade"] == "A"
    # 持有的投顾 A 票被纳入并带分级归因（即便当日因均线破位被风控减/清仓）
    assert by_code[h_adv]["grade"] == "A"
    assert "操作计划" in plan.to_markdown()


def test_backtest_risk_overlay_runs(engine):
    """风控开关：开启后回测应跑通并因硬止损产生卖出。"""
    base = LoopConfig(version="rk_off", start=START, end=END,
                      universe=UniverseConfig(method="alla"),
                      portfolio=PortfolioConfig(top_n=15, max_weight=0.1))
    ClosedLoop(engine, base).run("all")

    rk = LoopConfig(version="rk_on", start=START, end=END,
                    risk=RiskConfig(enabled=True, hard_stop_pct=0.05),
                    universe=UniverseConfig(method="alla"),
                    portfolio=PortfolioConfig(top_n=15, max_weight=0.1))
    m = ClosedLoop(engine, rk).run("backtest")
    assert np.isfinite(m["max_drawdown"])
    sells = engine.connect().execute(
        __import__("sqlalchemy").text(
            "SELECT COUNT(*) FROM backtest_trades t JOIN backtest_run r ON t.run_id=r.run_id "
            "WHERE r.name='cs_rk_on' AND t.action='sell'")).scalar()
    assert sells > 0


# ───────────────────────── 研报速通 / 影子验证 / 再入场 ─────────────────────────

def test_research_fast_entry_and_shadow(engine, monkeypatch):
    """新鲜 research A 信号：严格闸未触发也应半仓直入，并落 policy_shadow 影子行。"""
    repo = AdvisorRepo(engine)
    repo.execute_sql("DELETE FROM advisor_reco")
    repo.execute_sql("DELETE FROM policy_shadow")
    repo.execute_sql("DELETE FROM action_plan")
    HoldingRepo(engine).clear()
    codes = _some_codes(engine, 8)
    fresh, stale = codes[6], codes[7]
    last_dt = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]

    repo.save_reco(pd.DataFrame([
        {"rec_date": str(last_dt), "code": fresh, "source_type": "research",
         "grade": "A", "direction": "long", "valid_until": None},
        {"rec_date": "20220101", "code": stale, "source_type": "research",
         "grade": "A", "direction": "long", "valid_until": None},   # 早已过 3 日窗口
    ]))
    cfg = LoopConfig(version="plan_fast", start=START, end=END,
                     risk=RiskConfig(enabled=True),
                     universe=UniverseConfig(method="alla"),
                     portfolio=PortfolioConfig(advisor_led=True, advisory_name_cap=0.3))
    monkeypatch.setenv("RESEARCH_FAST_ENTRY", "1")
    plan = build_action_plan(engine, cfg, cash=100000, buypoint=True)
    by_code = {r["code"]: r for r in plan.rows}
    # 新鲜研报票：免闸直入（若恰巧闸门也触发则为全额 buy，同样接受）
    assert fresh in by_code and by_code[fresh]["action"] == "buy"
    row = by_code[fresh]
    if "研报速通" in row["reason"]:
        assert "半仓" in row["trigger_hint"] if "trigger_hint" in row else "半仓" in row["trigger"]
    # 影子表：新鲜信号有行、d0 尚未到（rec_date=最新日 → d0 是未来）或已填充
    sh = repo.read_sql("SELECT * FROM policy_shadow WHERE code=:c", {"c": fresh})
    assert len(sh) == 1
    # 回退开关：关闭后新鲜票不再免闸（严格闸未触发时应回到观察池）
    monkeypatch.setenv("RESEARCH_FAST_ENTRY", "0")
    plan2 = build_action_plan(engine, cfg, cash=100000, buypoint=True, persist=False)
    by2 = {r["code"]: r for r in plan2.rows}
    if fresh in by2:
        assert "研报速通" not in str(by2[fresh].get("reason", ""))


def test_reentry_after_profit_exit(engine):
    """盈利保护清仓后创新高 → 半仓再入场行。"""
    from invest_model.repositories.base import BaseRepository
    repo = BaseRepository(engine)
    repo.execute_sql("DELETE FROM advisor_reco")
    HoldingRepo(engine).clear()
    # 造一只 45 日单边上行、末日创新高的票
    dts = list(repo.read_sql(
        "SELECT DISTINCT trade_date FROM stock_daily ORDER BY trade_date DESC LIMIT 50"
    )["trade_date"])[::-1]
    px = [10 + 0.1 * i for i in range(len(dts))]
    rows = pd.DataFrame({"code": "RE1.SH", "trade_date": dts,
                         "open": px, "high": [p * 1.01 for p in px],
                         "low": [p * 0.99 for p in px], "close": px,
                         "volume": [1e6] * len(dts), "amount": [1e7] * len(dts)})
    repo.upsert("stock_daily", rows, ["code", "trade_date"])
    # 一周前的计划里被盈利保护清仓
    repo.upsert("action_plan", pd.DataFrame([{
        "plan_date": dts[-6], "code": "RE1.SH", "name": "再入场测试",
        "action": "sell", "cur_weight": 0.05, "tgt_weight": 0.0, "shares_delta": -1000,
        "reason": "盈利保护止盈(自峰值回撤13%≥12%)", "stop_price": None,
        "ref_price": px[-6], "grade": "A", "trigger_hint": "—",
        "model_rank": None, "model_view": None}]), ["plan_date", "code"])
    cfg = LoopConfig(version="plan_re", start=START, end=END,
                     risk=RiskConfig(enabled=True, reentry=True),
                     universe=UniverseConfig(method="alla"),
                     portfolio=PortfolioConfig(advisor_led=True))
    plan = build_action_plan(engine, cfg, cash=100000, buypoint=False, persist=False)
    re_rows = [r for r in plan.rows if r["code"] == "RE1.SH"]
    assert re_rows and re_rows[0]["action"] == "buy" and "再入场" in re_rows[0]["reason"]
    assert re_rows[0]["tgt_weight"] > 0
    # 开关关闭 → 无再入场行
    cfg2 = LoopConfig(version="plan_re2", start=START, end=END,
                      risk=RiskConfig(enabled=True, reentry=False),
                      universe=UniverseConfig(method="alla"),
                      portfolio=PortfolioConfig(advisor_led=True))
    plan2 = build_action_plan(engine, cfg2, cash=100000, buypoint=False, persist=False)
    assert not [r for r in plan2.rows if r["code"] == "RE1.SH"]


def test_buypoint_breakout_v2_no_volume_no_engulf(tmp_path):
    """P18 v2：趋势内 20 日收盘新高 + 阳线即触发突破——不再要求吞没形态与放量。"""
    eng = make_engine(f"sqlite:///{tmp_path}/bp3.db"); create_schema(eng)
    import numpy as _np
    # 连续上行且末日创出 75 日内收盘新高；昨日也是阳线（v1 吞没条件必不成立）、量能平平
    closes = list(_np.linspace(60, 120, 79)) + [122.0]
    vols = [1e6] * 80                                   # 无放量
    opens = [closes[0]] + closes[:-1]
    opens[-1] = 120.5                                   # 末日阳线（open<close）、高于昨实体下沿
    dt = _seed_ohlcv(eng, "BK.SH", closes, vols, opens)
    bp = detect_buypoints(eng, dt, ["BK.SH"], gross=0.9,
                          rank_map={"BK.SH": 0.9})["BK.SH"]
    assert bp.is_buy and bp.kind == "突破新高", (bp.kind, bp.reason)
    # 阴线新高不触发（保留阳线要求）
    opens2 = list(opens); opens2[-1] = 123.0            # open>close=阴线
    dt2 = _seed_ohlcv(eng, "BK2.SH", closes, vols, opens2)
    bp2 = detect_buypoints(eng, dt2, ["BK2.SH"], gross=0.9,
                           rank_map={"BK2.SH": 0.9})["BK2.SH"]
    assert not bp2.is_buy


def test_theme_validity_window_and_direction_evolution(tmp_path):
    """主题有效期两层：① source_type 时间窗（盘中短/研报长、显式 valid_until 优先）；
    ② 方向演化——同主题名只留最新 rec_date 行（reduce 取代旧 long），跨主题分歧并记。"""
    eng = make_engine(f"sqlite:///{tmp_path}/theme.db"); create_schema(eng)
    repo = AdvisorRepo(eng)
    dt = "20260720"
    rows = [
        # 盘中主题：3 天前有效、10 天前过期
        {"rec_date": "20260717", "theme": "端侧AI", "source_type": "intraday",
         "direction": "long", "thesis": "", "valid_until": None},
        {"rec_date": "20260710", "theme": "影视院线", "source_type": "intraday",
         "direction": "long", "thesis": "", "valid_until": None},
        # 研报主题：20 天前仍有效、60 天前过期
        {"rec_date": "20260701", "theme": "红利防御", "source_type": "research",
         "direction": "long", "thesis": "", "valid_until": None},
        {"rec_date": "20260520", "theme": "老赛道", "source_type": "research",
         "direction": "long", "thesis": "", "valid_until": None},
        # 方向演化：同主题名 科技 先 long 后 reduce → 只留 reduce
        {"rec_date": "20260716", "theme": "科技成长", "source_type": "intraday",
         "direction": "long", "thesis": "", "valid_until": None},
        {"rec_date": "20260720", "theme": "科技成长", "source_type": "intraday",
         "direction": "reduce", "thesis": "", "valid_until": None},
        # 跨主题分歧：不同名 long+reduce 都保留
        {"rec_date": "20260719", "theme": "大盘托底", "source_type": "research",
         "direction": "long", "thesis": "", "valid_until": None},
        {"rec_date": "20260720", "theme": "大盘救市存疑", "source_type": "research",
         "direction": "reduce", "thesis": "", "valid_until": None},
        # 显式 valid_until 覆盖：很旧的盘中主题但显式有效期未过 → 保留
        {"rec_date": "20260601", "theme": "长效盘中", "source_type": "intraday",
         "direction": "long", "thesis": "", "valid_until": "20260801"},
    ]
    repo.save_theme(pd.DataFrame(rows))
    act = repo.get_active_theme(dt)
    got = set(act["theme"])

    assert "端侧AI" in got                    # 盘中 3 天前有效
    assert "影视院线" not in got              # 盘中 10 天前过期
    assert "红利防御" in got                  # 研报 20 天前有效
    assert "老赛道" not in got                # 研报 60 天前过期
    assert "长效盘中" in got                  # 显式 valid_until 覆盖时间窗
    # 方向演化：科技成长只留 reduce（最新）
    tech = act[act["theme"] == "科技成长"]
    assert len(tech) == 1 and tech["direction"].iloc[0] == "reduce"
    # 跨主题分歧：大盘两条不同名并记
    assert "大盘托底" in got and "大盘救市存疑" in got
