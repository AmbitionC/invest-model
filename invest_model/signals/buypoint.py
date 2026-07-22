"""复合买点检测：手册技术买点 + 量化分确认 + 大盘环境闸。

把投顾《圈子选股体系执行细则》第 2 步的"精确买点"用日线 OHLCV 做近似，并叠加本量化
系统自有的两个信号，实现"投顾定方向、技术抓时机、量化把质量与环境"：
  - 技术（手册近似+P18 v2）：MA60 走平/上行 且（趋势中继回踩 MA20 放量阳线 ∨ 突破新高阳线）
  - 量化确认：多因子 rank_pct ≥ 阈值（基本面不拖后腿）
  - 环境：大盘择时 gross ≥ 阈值（差行情不追新仓）

只有三层都过，研报观察池里的标的才从"观察"提升为"建议买入"。

逆向叠加：恐慌指数 ≥ 阈值（极度恐慌）时，差行情反而是抄底窗口，故放松环境闸到
``fear_min_gross``，并给触发的买点打 "抄底" 标签。其余两闸（技术/量化）不放松。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from invest_model.repositories.base import BaseRepository


@dataclass
class BuyPointConfig:
    trend_ma: int = 60
    pullback_pct: float = 0.03       # 回踩 MA20 的贴合带（|close/MA20-1| ≤ 此值）
    retrace_vol_mult: float = 1.2    # 趋势中继：放量倍数（vs 20 日均量）
    breakout_vol_mult: float = 2.0   # [已弃用·P18 v2] 保留字段仅为回退兼容，突破分支不再用量能
    breakout_lookback: int = 20      # 突破平台/新高回看天数
    quant_min_rank: float = 0.5      # 量化 rank_pct 下限（中位以上）
    min_gross: float = 0.6           # 大盘环境闸：gross 下限
    use_quant: bool = True
    use_env: bool = True
    use_fear: bool = True            # 逆向：极度恐慌时放松环境闸
    fear_buy: float = 75.0           # 恐慌分 ≥ 此值视为抄底窗口
    fear_min_gross: float = 0.4      # 抄底窗口里环境闸下限（放松后）
    # 下跌二分法（提案 P9）：估值驱动下跌可抄、业绩驱动下跌禁抄——恐慌放松只给
    # 基本面未走坏的票；单季+累计净利同比双负（连续负增长的近似确认）者不放松。
    # 只收紧 fear 特例分支，正常路径行为不变；数据缺失不拦（fail-open）。
    fear_fundamental_gate: bool = True


@dataclass
class BuyPoint:
    code: str
    is_buy: bool
    kind: str        # "趋势中继" | "突破新高" | ""
    reason: str      # 触发或未触发的简述
    last: float = float("nan")        # 最新收盘
    ma20: float = float("nan")        # 回踩买点参考位
    breakout: float = float("nan")    # 突破买点参考位（近 N 日最高收盘）
    limit_up: bool = False            # 信号日涨停收盘：次日按收盘价追=追涨停，计划须改挂回踩位


def _ma(s: pd.Series, n: int) -> float:
    s = s.dropna()
    return float(s.tail(n).mean()) if len(s) >= n else float("nan")


def _slope_up(s: pd.Series, n: int, look: int = 5) -> bool:
    ma = pd.to_numeric(s, errors="coerce").rolling(n).mean().dropna()
    return len(ma) >= look + 1 and float(ma.iloc[-1] - ma.iloc[-1 - look]) >= 0


def detect_buypoints(engine, dt: str, codes: list[str], gross: float,
                     rank_map: dict[str, float] | None = None,
                     cfg: BuyPointConfig | None = None,
                     fear: float | None = None) -> dict[str, BuyPoint]:
    """对 codes 在 dt 检测复合买点。返回 {code: BuyPoint}。

    fear: 当期恐慌分（0–100）。未传且 use_fear 时按 dt 现算一次（慢变量，按日不变）。
    """
    cfg = cfg or BuyPointConfig()
    rank_map = rank_map or {}
    codes = list(dict.fromkeys(codes))
    out: dict[str, BuyPoint] = {}
    if not codes:
        return out
    repo = BaseRepository(engine)
    if cfg.use_fear and fear is None:
        try:
            from invest_model.signals.fear import fear_gauge
            fear = float(fear_gauge(engine, dt)["score"])
        except Exception:  # noqa: BLE001
            fear = None
    start = (pd.to_datetime(dt) - pd.Timedelta(days=cfg.trend_ma * 2 + 40)).strftime("%Y%m%d")
    frames = []
    for i in range(0, len(codes), 600):
        batch = codes[i:i + 600]
        ph = ",".join(f":c{j}" for j in range(len(batch)))
        params = {f"c{j}": c for j, c in enumerate(batch)}
        params.update(s=start, d=dt)
        frames.append(repo.read_sql(
            f"SELECT code, trade_date, open, high, low, close, volume FROM stock_daily "
            f"WHERE trade_date>=:s AND trade_date<=:d AND code IN ({ph})", params))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    panic = cfg.use_fear and fear is not None and fear >= cfg.fear_buy
    # 下跌二分法：恐慌放松只对"估值驱动下跌"的票生效，业绩走坏的票维持原环境闸
    earnings_bad: set[str] = set()
    if panic and cfg.fear_fundamental_gate:
        try:
            earnings_bad = _earnings_deteriorated(repo, dt, codes)
        except Exception:  # noqa: BLE001 — 财务数据缺失不拦（fail-open）
            earnings_bad = set()

    for c in codes:
        g = df[df["code"] == c].sort_values("trade_date") if not df.empty else pd.DataFrame()
        if len(g) < cfg.trend_ma + 5:
            out[c] = BuyPoint(c, False, "", "观察：行情样本不足")
            continue
        o = pd.to_numeric(g["open"], errors="coerce")
        h = pd.to_numeric(g["high"], errors="coerce")
        low = pd.to_numeric(g["low"], errors="coerce")
        cl = pd.to_numeric(g["close"], errors="coerce")
        vol = pd.to_numeric(g["volume"], errors="coerce")
        ma20, ma60 = _ma(cl, 20), _ma(cl, 60)
        c0, o0, v0 = float(cl.iloc[-1]), float(o.iloc[-1]), float(vol.iloc[-1])
        vma20 = float(vol.tail(20).mean())
        platform_high = float(cl.iloc[-(cfg.breakout_lookback + 1):-1].max())
        # 信号日是否涨停收盘（按板块阈值近似：主板10%/创业板科创板20%/北交所30%）。
        # 涨停触发的突破，次日按信号日收盘挂单＝追涨停（0721 长川科技实例：计划给
        # "挂单≈涨停价"、用户自行改挂回踩位才对）——标记后由计划层改挂回踩位并警示。
        prev_c = float(cl.iloc[-2]) if len(cl) >= 2 else float("nan")
        lim_thr = (29.5 if c.endswith(".BJ")
                   else 19.6 if c.startswith(("688", "689", "300", "301", "302")) else 9.8)
        is_limit_up = (np.isfinite(prev_c) and prev_c > 0
                       and (c0 / prev_c - 1) * 100 >= lim_thr)
        px = dict(last=round(c0, 2), ma20=round(ma20, 2) if np.isfinite(ma20) else float("nan"),
                  breakout=round(platform_high, 2), limit_up=is_limit_up)

        # 前置过滤：左侧下降趋势一律不看
        trend_up = np.isfinite(ma60) and c0 >= ma60 and _slope_up(cl, 60)
        if not trend_up:
            out[c] = BuyPoint(c, False, "", "观察：MA60 未走平/上行（左侧趋势，不买）", **px)
            continue

        # 买点2 趋势中继：MA20↑ + 回踩 MA20 贴合 + 阳线 + 放量
        retrace = (_slope_up(cl, 20) and np.isfinite(ma20)
                   and abs(c0 / ma20 - 1.0) <= cfg.pullback_pct
                   and float(low.tail(3).min()) <= ma20 * (1 + cfg.pullback_pct)
                   and c0 > o0 and v0 >= cfg.retrace_vol_mult * vma20)
        # 买点1 突破新高（P18 v2）：趋势内 20 日收盘新高 + 当日阳线。
        # v1 的"阳线吞没+放量2×"确认经全样本回测证伪（run 29399834503：吞没使趋势突破
        # 结构上无法触发、量能确认为负贡献——保留1%信号且胜率反而更低），已删除；
        # 假突破防御交给止损纪律（P10/硬止损），与海龟/52周新高动量的处理一致。
        breakout = c0 > o0 and c0 >= platform_high

        kind = "趋势中继" if retrace else ("突破新高" if breakout else "")
        if not kind:
            out[c] = BuyPoint(c, False, "", "观察：趋势在但未现买点（待回踩 MA20/突破新高）", **px)
            continue

        # 量化确认 + 环境
        rk = rank_map.get(c)
        quant_ok = (not cfg.use_quant) or rk is None or rk >= cfg.quant_min_rank
        if not quant_ok:
            out[c] = BuyPoint(c, False, kind, f"观察：现{kind}买点但量化分偏弱(rank<{cfg.quant_min_rank:.0%})", **px)
            continue
        # 环境闸逐票判定：恐慌放松仅给基本面未走坏的票（估值驱动下跌可抄，
        # 业绩驱动下跌禁抄——下跌二分法，提案 P9）
        relax = panic and c not in earnings_bad
        gross_floor = cfg.fear_min_gross if relax else cfg.min_gross
        env_ok = (not cfg.use_env) or (gross >= gross_floor)
        if not env_ok:
            if panic and c in earnings_bad and gross >= cfg.fear_min_gross:
                msg = f"观察：现{kind}买点但业绩驱动下跌禁抄（单季+累计净利同比双负，不享恐慌放松）"
            else:
                msg = f"观察：现{kind}买点但大盘环境差(gross<{gross_floor:.0%})"
            out[c] = BuyPoint(c, False, kind, msg, **px)
            continue
        tag = f"买点触发：{kind}（技术+量化+环境三重确认）"
        if relax and gross < cfg.min_gross:
            tag = f"抄底买点：{kind}（恐慌{fear:.0f}≥{cfg.fear_buy:.0f}放松环境闸，技术+量化确认，基本面未走坏）"
        out[c] = BuyPoint(c, True, kind, tag, **px)
    return out


def _earnings_deteriorated(repo: BaseRepository, dt: str, codes: list[str]) -> set[str]:
    """业绩驱动下跌的近似确认：最近一期单季净利同比与累计净利同比双负。

    出处：投资篇#3——下跌分估值驱动（系统性恐慌，可抄）与业绩驱动（万不可抄）；
    确认走坏需连续负增长，这里用"单季 + 累计双负"做可取数的近似。
    数据缺失（无 q_profit_yoy 列/无财务数据）时返回空集 = 不拦（fail-open）。
    """
    stale = (pd.to_datetime(dt) - pd.Timedelta(days=540)).strftime("%Y%m%d")
    f = repo.read_sql(
        "SELECT code, ann_date, report_date, profit_yoy, q_profit_yoy "
        "FROM stock_fina_indicator WHERE ann_date<=:d AND ann_date>=:lo",
        {"d": dt, "lo": stale})
    if f.empty:
        return set()
    f = f[f["code"].isin(set(codes))]
    if f.empty:
        return set()
    f = f.sort_values(["code", "ann_date", "report_date"]).groupby("code").tail(1)
    py = pd.to_numeric(f["profit_yoy"], errors="coerce")
    qy = pd.to_numeric(f["q_profit_yoy"], errors="coerce")
    bad = f.loc[(py < 0) & (qy < 0), "code"]
    return set(bad.astype(str))
