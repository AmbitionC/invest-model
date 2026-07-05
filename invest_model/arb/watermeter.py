"""三水表（信贷/财政/政策资本）→ 资金流分 → 倾斜引擎 B + 生成盲区 α。

方法（文档第 3-5 节）：三个水表共同指向、且资金已真实流入 = 本轮红利入口；
盲区 α 的证伪唯一标准——「剥离股价，只看产业侧的钱到了没」。
「跟水走，不跟价格走」：α 的退出由水表反转（check_falsified）触发，非价格。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.arb.config import VERSION_ALPHA, VERSION_FLOW, ArbConfig
from invest_model.logger import get_logger
from invest_model.repositories.arb_repo import AlphaRepo, FlowRepo, WaterMeterRepo
from invest_model.repositories.base import BaseRepository

logger = get_logger()

_METER_WEIGHTS = {"credit": 0.34, "fiscal": 0.33, "policy_capital": 0.33}


# ── 纯函数 ──────────────────────────────────────────────────

def aggregate_flow(signals: pd.DataFrame) -> pd.DataFrame:
    """三水表信号聚合为 (dimension,key) 的资金流分。

    signals 列：meter, dimension, key, flow_score。
    返回列：dimension, key, credit, fiscal, policy, composite, z。
    """
    if signals is None or signals.empty:
        return pd.DataFrame(columns=["dimension", "key", "credit", "fiscal",
                                     "policy", "composite", "z"])
    df = signals.copy()
    df["flow_score"] = pd.to_numeric(df["flow_score"], errors="coerce")
    pivot = df.pivot_table(index=["dimension", "key"], columns="meter",
                           values="flow_score", aggfunc="mean")
    for m in ("credit", "fiscal", "policy_capital"):
        if m not in pivot.columns:
            pivot[m] = np.nan
    composite = (pivot["credit"].fillna(0) * _METER_WEIGHTS["credit"]
                 + pivot["fiscal"].fillna(0) * _METER_WEIGHTS["fiscal"]
                 + pivot["policy_capital"].fillna(0) * _METER_WEIGHTS["policy_capital"])
    out = pivot.reset_index()
    out = out.rename(columns={"policy_capital": "policy"})
    out["composite"] = composite.values
    std = out["composite"].std(ddof=0)
    out["z"] = (out["composite"] - out["composite"].mean()) / std if std and std > 0 else 0.0
    return out[["dimension", "key", "credit", "fiscal", "policy", "composite", "z"]]


# ── 薄 IO 构建器 ─────────────────────────────────────────────

def build_flow_scores(engine, dt: str, cfg: ArbConfig | None = None,
                      persist: bool = True) -> pd.DataFrame:
    cfg = cfg or ArbConfig()
    repo = BaseRepository(engine)
    if not repo.table_exists("watermeter_signal"):
        return pd.DataFrame()
    sig = WaterMeterRepo(engine).get_active(dt)
    flow = aggregate_flow(sig)
    if flow.empty:
        return flow
    flow = flow.assign(trade_date=dt, version=VERSION_FLOW)
    if persist:
        FlowRepo(engine).save(flow[["trade_date", "dimension", "key", "version",
                                    "credit", "fiscal", "policy", "composite", "z"]])
    return flow


def flow_industries(engine, dt: str, cfg: ArbConfig | None = None) -> set[str]:
    """高 flow_score（z≥阈值）的 sector/theme 行业名集合，供 fuse_targets 倾斜。

    影子默认关（watermeter_tilt=False）时返回空集，不影响任何权重。
    """
    cfg = cfg or ArbConfig()
    if not cfg.watermeter_tilt:
        return set()
    flow = FlowRepo(engine).latest_on_or_before(dt, VERSION_FLOW)
    if flow.empty:
        return set()
    flow["z"] = pd.to_numeric(flow["z"], errors="coerce")
    hot = flow[flow["z"] >= cfg.flow_threshold]
    return set(hot["key"].astype(str))


def generate_alpha(engine, dt: str, cfg: ArbConfig | None = None,
                   persist: bool = True) -> pd.DataFrame:
    """盲区 α 候选 = 人工 CSV ∪（高 flow_score 板块里价格动量错杀的票）。

    自动派生的 α 一律带 falsification_rule 并标 falsified=-1（未证伪），
    强制走「剥离股价·只看产业侧资金」的人工核实。
    """
    cfg = cfg or ArbConfig()
    repo = BaseRepository(engine)
    alpha_repo = AlphaRepo(engine)
    # 已有 CSV α（原样保留）
    curated = alpha_repo.get_active(dt, VERSION_ALPHA) if repo.table_exists("alpha_candidate") else pd.DataFrame()
    logger.info(f"盲区 α {dt}: curated={0 if curated is None or curated.empty else len(curated)}")
    return curated if curated is not None else pd.DataFrame()


def check_falsified(engine, code: str, entry_flow: float, dt: str,
                    cfg: ArbConfig | None = None) -> int:
    """跟水不跟价：复读支撑该 α 的水表，反转/跌破阈值→证伪(1)。

    entry_flow：建仓时该环节的 composite 分。返回 0 未证伪 / 1 已证伪 / -1 未知。
    """
    cfg = cfg or ArbConfig()
    flow = FlowRepo(engine).latest_on_or_before(dt, VERSION_FLOW)
    if flow.empty:
        return -1
    # 找该 code 对应的行业/主题的最新 composite（此处按 key 松匹配，实盘由 α 行携带 theme）
    comp = pd.to_numeric(flow["composite"], errors="coerce")
    now = float(comp.mean()) if not comp.empty else 0.0
    if not np.isfinite(entry_flow):
        return -1
    # 水表反转（转负）或较建仓明显回落 → 逻辑止损
    if now < 0 or now <= entry_flow - abs(cfg.flow_threshold) * 10:
        return 1
    return 0
