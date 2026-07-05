"""三水表自动构建（纯 Tushare 市场资金流代理，替代手工 CSV）。

把文档「读三个真实的资金流」落成每日可查的市场资金流：
  - 政策资本水表 ← 北向持股变化按行业（stock_hk_hold，已每日入库）
  - 信贷水表     ← 两融融资余额变化按行业（stock_margin_detail）
  - 财政水表     ← 暂缺（无干净 API；aggregate_flow 优雅降级，后续可加 LLM 政策挖掘）

均为真实、可查的资金流，天然满足证伪铁律（真金白银、非故事）。每股数据映射到
stock_info.industry（与全系统同一行业口径），跨行业 z-score → [-100,100]。
写入 watermeter_signal（source='auto'），下游 build_flow_scores 聚合不变。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.arb.config import ArbConfig
from invest_model.logger import get_logger
from invest_model.repositories.arb_repo import WaterMeterRepo
from invest_model.repositories.base import BaseRepository

logger = get_logger()


def _two_dates(repo: BaseRepository, table: str, dt: str, window: int) -> tuple[str, str] | None:
    """返回 (now_date, prev_date)：表内 <=dt 的最新日 与 往前 window 个数据日。"""
    df = repo.read_sql(
        f"SELECT DISTINCT trade_date FROM {table} WHERE trade_date<=:d "
        f"ORDER BY trade_date DESC LIMIT :n", {"d": dt, "n": window + 1})
    if df.empty or len(df) < 2:
        return None
    dates = df["trade_date"].tolist()
    return dates[0], dates[-1]


def _sector_delta(repo: BaseRepository, dt: str, table: str, value_col: str,
                  window: int, ind_map: dict[str, str]) -> pd.DataFrame:
    """按行业聚合 value_col 的 window 日净变动，跨行业 z-score → [-100,100]。

    返回 DataFrame[key(行业), flow_score, raw_delta]。数据缺失返回空表。
    """
    if not repo.table_exists(table):
        return pd.DataFrame()
    pair = _two_dates(repo, table, dt, window)
    if not pair:
        return pd.DataFrame()
    now_d, prev_d = pair
    cur = repo.read_sql(
        f"SELECT code, {value_col} AS v FROM {table} WHERE trade_date=:d", {"d": now_d})
    prev = repo.read_sql(
        f"SELECT code, {value_col} AS v FROM {table} WHERE trade_date=:d", {"d": prev_d})
    if cur.empty or prev.empty:
        return pd.DataFrame()
    cur = cur.set_index("code")["v"].apply(pd.to_numeric, errors="coerce")
    prev = prev.set_index("code")["v"].apply(pd.to_numeric, errors="coerce")
    delta = (cur - prev.reindex(cur.index)).dropna()
    if delta.empty:
        return pd.DataFrame()
    df = pd.DataFrame({"code": delta.index, "delta": delta.values})
    df["industry"] = df["code"].map(ind_map)
    df = df[df["industry"].notna() & (df["industry"] != "")]
    if df.empty:
        return pd.DataFrame()
    agg = df.groupby("industry")["delta"].sum()
    std = agg.std(ddof=0)
    if not std or std == 0:
        z = pd.Series(0.0, index=agg.index)
    else:
        z = (agg - agg.mean()) / std
    score = (z * 33.0).clip(-100, 100)
    return pd.DataFrame({"key": agg.index, "flow_score": score.values,
                         "raw_delta": agg.values})


def _direction(score: float) -> str:
    if score >= 10:
        return "in"
    if score <= -10:
        return "out"
    return "flat"


def build_watermeter_auto(engine, dt: str, cfg: ArbConfig | None = None,
                          persist: bool = True) -> int:
    """从市场资金流自动生成三水表信号（写 watermeter_signal，source='auto'）。"""
    cfg = cfg or ArbConfig()
    repo = BaseRepository(engine)
    ind_map: dict[str, str] = {}
    if repo.table_exists("stock_info"):
        si = repo.read_sql("SELECT ts_code AS code, industry FROM stock_info")
        ind_map = dict(zip(si["code"], si["industry"]))
    if not ind_map:
        logger.warning("水表自动构建：stock_info 行业映射为空，跳过")
        return 0

    meters = [
        # (meter, table, value_col, 说明锚点)
        ("policy_capital", "stock_hk_hold", "ratio", "北向持股占比20日净变动"),
        ("credit", "stock_margin_detail", "rzye", "融资余额20日净变动"),
    ]
    rows: list[dict] = []
    for meter, table, col, label in meters:
        sec = _sector_delta(repo, dt, table, col, 20, ind_map)
        if sec.empty:
            logger.info(f"水表自动构建：{meter}（{table}）无数据，降级跳过")
            continue
        for _, r in sec.iterrows():
            sc = round(float(r["flow_score"]), 4)
            rows.append({
                "as_of_date": dt, "meter": meter, "dimension": "sector",
                "key": str(r["key"]), "flow_score": sc, "direction": _direction(sc),
                "evidence": f"{label} z-score（真实资金流·可查，raw={float(r['raw_delta']):.2f}）",
                "source": "auto", "valid_until": None,
                "raw_excerpt": f"auto·{table}·{col}·20d",
            })
    if not rows:
        logger.warning("水表自动构建：两表均无数据，未生成信号")
        return 0
    df = pd.DataFrame(rows)
    if persist:
        WaterMeterRepo(engine).save(df)
    logger.info(f"水表自动构建 {dt}：{len(df)} 条（"
                + "、".join(f"{m}={sum(df['meter']==m)}" for m in df['meter'].unique()) + "）")
    return len(df)
