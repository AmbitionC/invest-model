"""财务排雷 7 规则影子打分器（提案 P7，影子观察：只记录、不剔除）。

方法论出处：重远投资观《财报系列#3/#4/#7》（利润表排雷 / 排雷模型 / 商誉），
已在 life-teachers `verification/profit-vs-cash-and-fraud-screen/` 工程验证：
干净公司 0 红旗、问题公司（乐视/獐子岛型）8 红旗清晰分开。

七条规则（每条是"触发深挖"信号，**不是**"确认造假"结论——宁错杀原则只在
晋升为 universe 硬过滤后生效）：
  R1 应收/营收占比过大            —— 收入可能靠赊销堆出来
  R2 商誉/归母净资产过高          —— 虚资产 + 隐形杠杆（减值只减资产端）
  R3 毛利率显著高于行业中位        —— 好得不像真的（行业内校准，验证报告的告诫）
  R4 三费占营收同比骤降            —— 费用腾挪粉饰净利润
  R5 归母净利润 ≫ 净利润 或异号   —— 亏少数股东肥上市公司（乐视探针，红旗≈19 倍）
  R6 经营现金流/净利润长期过低     —— 权责发生制下利润≠现金（利润+1.9 亿 vs 现金−4 亿）
  R7 免税/税优行业放大器           —— 造假税务成本低（獐子岛"扇贝游走"），已有红旗时+1

影子模式：每个调仓日全 universe 打分落 `quality_flag` 表；health 展示分布；
操作计划对持仓命中 ≥2 旗者追加 risk_hint。不改变 universe、打分与组合。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()


@dataclass
class QualityConfig:
    ar_ratio: float = 0.5            # R1 应收/营收 上限
    goodwill_ratio: float = 0.3      # R2 商誉/归母净资产 上限
    gm_excess: float = 15.0          # R3 毛利率超行业中位 上限（百分点）
    fee_drop: float = 5.0            # R4 三费占比同比骤降 阈值（百分点）
    attr_ratio: float = 1.5          # R5 归母/净利润 上限
    ocf_ratio: float = 0.3           # R6 经营现金流/净利润 下限（净利润为正时）
    hint_flags: int = 2              # 操作计划提示阈值（命中 ≥N 旗）
    # R7 免税/税优高发行业（stock_info.industry 关键词匹配）
    tax_free_keywords: tuple[str, ...] = ("农", "渔", "牧", "林", "软件", "半导体",
                                          "元器件", "互联网", "IT")


def screen_cross_section(fina: pd.DataFrame, cfg: QualityConfig | None = None) -> pd.DataFrame:
    """对一个财务截面打红旗。纯函数，供生产与单测共用。

    fina：index=code，列（缺列=对应规则跳过）：
      industry, gross_margin(%),
      accounts_receiv, revenue, goodwill, eq_exc_min,
      n_income, n_income_attr_p, n_cashflow_act,
      fee_ratio(三费/营收，小数), fee_ratio_prev(上年同期)
    返回：index=code，列 n_flags(int) + flags(list[str])。
    """
    cfg = cfg or QualityConfig()
    idx = fina.index
    flags: dict[str, list[str]] = {c: [] for c in idx}

    def _num(col: str) -> pd.Series:
        return pd.to_numeric(fina.get(col, pd.Series(np.nan, index=idx)), errors="coerce")

    ar, rev = _num("accounts_receiv"), _num("revenue")
    gw, eq = _num("goodwill"), _num("eq_exc_min")
    ni, attr = _num("n_income"), _num("n_income_attr_p")
    ocf = _num("n_cashflow_act")
    gm = _num("gross_margin")
    fee, fee_prev = _num("fee_ratio"), _num("fee_ratio_prev")
    industry = fina.get("industry", pd.Series("", index=idx)).fillna("")

    # R1 应收占比
    r1 = ar / rev
    for c in idx[(r1 > cfg.ar_ratio).fillna(False)]:
        flags[c].append(f"应收/营收 {r1[c]:.0%} > {cfg.ar_ratio:.0%}（收入或靠赊销）")

    # R2 商誉占比
    r2 = gw / eq
    for c in idx[((r2 > cfg.goodwill_ratio) & (eq > 0)).fillna(False)]:
        flags[c].append(f"商誉/净资产 {r2[c]:.0%} > {cfg.goodwill_ratio:.0%}（虚资产+隐形杠杆）")

    # R3 毛利率显著高于行业中位（行业内校准）
    if gm.notna().any() and industry.astype(bool).any():
        med = gm.groupby(industry).transform("median")
        cnt = gm.groupby(industry).transform("count")
        r3 = (gm - med > cfg.gm_excess) & (cnt >= 5)   # 行业样本太少不比
        for c in idx[r3.fillna(False)]:
            flags[c].append(
                f"毛利率 {gm[c]:.0f}% 超行业中位 {med[c]:.0f}% 达 {gm[c]-med[c]:.0f}pp（好得不像真的）")

    # R4 三费占比同比骤降
    r4 = (fee - fee_prev) * 100.0 < -cfg.fee_drop
    for c in idx[r4.fillna(False)]:
        flags[c].append(
            f"三费占营收同比骤降 {(fee[c]-fee_prev[c])*100:.1f}pp（费用腾挪粉饰净利润疑点）")

    # R5 归母 ≫ 净利润 或异号（乐视探针）
    r5 = ((ni <= 0) & (attr > 0)) | ((ni > 0) & (attr / ni > cfg.attr_ratio))
    for c in idx[r5.fillna(False)]:
        flags[c].append(
            f"归母 {attr[c]:,.0f} ≫/异号于 净利润 {ni[c]:,.0f}（亏少数股东肥上市公司疑点）")

    # R6 利润-现金背离（净利润为正、经营现金流覆盖不足）
    r6 = (ni > 0) & (ocf / ni < cfg.ocf_ratio)
    for c in idx[r6.fillna(False)]:
        flags[c].append(
            f"经营现金流/净利润 {ocf[c]/ni[c]:.0%} < {cfg.ocf_ratio:.0%}（利润≠现金，权责发生制背离）")

    # R7 免税行业放大器：已有红旗时 +1（避免整行业无差别误伤）
    taxfree = industry.map(lambda s: any(k in str(s) for k in cfg.tax_free_keywords))
    for c in idx[taxfree.fillna(False)]:
        if flags[c]:
            flags[c].append("处免税/税优行业（造假税务成本低），既有疑点加倍警惕")

    out = pd.DataFrame(index=idx)
    out["flags"] = [flags[c] for c in idx]
    out["n_flags"] = [len(flags[c]) for c in idx]
    return out


def _pit_latest(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """每票取 ann_date 最新一行（同日取 report_date 最新）。"""
    df = df.sort_values(["code", "ann_date", "report_date"])
    latest = df.groupby("code").tail(1).set_index("code")
    for c in cols:
        if c in latest.columns:
            latest[c] = pd.to_numeric(latest[c], errors="coerce")
    return latest


def load_quality_inputs(engine, trade_date: str, codes: list[str]) -> pd.DataFrame:
    """组装打分所需财务截面（PIT）。缺表/缺数据的列自然缺省 → 对应规则跳过。"""
    repo = BaseRepository(engine)
    code_set = set(codes)
    out = pd.DataFrame(index=sorted(code_set))
    out.index.name = "code"

    info = repo.read_sql("SELECT ts_code AS code, industry FROM stock_info").set_index("code")
    out = out.join(info["industry"])

    stale = (pd.to_datetime(trade_date) - pd.Timedelta(days=540)).strftime("%Y%m%d")
    fi = repo.read_sql(
        "SELECT code, ann_date, report_date, gross_margin FROM stock_fina_indicator "
        "WHERE ann_date<=:d AND ann_date>=:lo", {"d": trade_date, "lo": stale})
    if not fi.empty:
        fi = fi[fi["code"].isin(code_set)]
        out = out.join(_pit_latest(fi, ["gross_margin"])[["gross_margin"]])

    if repo.table_exists("stock_fina_ext"):
        # 多取一年：R4 需要上年同期三费占比
        lo2 = (pd.to_datetime(trade_date) - pd.Timedelta(days=540 + 380)).strftime("%Y%m%d")
        fe = repo.read_sql(
            "SELECT code, ann_date, report_date, goodwill, eq_exc_min, accounts_receiv, "
            "revenue, n_income, n_income_attr_p, sell_exp, admin_exp, fin_exp, "
            "n_cashflow_act FROM stock_fina_ext WHERE ann_date<=:d AND ann_date>=:lo",
            {"d": trade_date, "lo": lo2})
        if not fe.empty:
            fe = fe[fe["code"].isin(code_set)].copy()
            for c in ("sell_exp", "admin_exp", "fin_exp", "revenue"):
                fe[c] = pd.to_numeric(fe[c], errors="coerce")
            fee = (fe["sell_exp"].fillna(0) + fe["admin_exp"].fillna(0)
                   + fe["fin_exp"].fillna(0))
            fe["fee_ratio"] = (fee / fe["revenue"]).where(fe["revenue"] > 0)
            latest = _pit_latest(
                fe, ["goodwill", "eq_exc_min", "accounts_receiv", "revenue",
                     "n_income", "n_income_attr_p", "n_cashflow_act", "fee_ratio"])
            # 上年同期三费占比（report_date 年份-1、月日相同）
            latest["_prev_period"] = latest["report_date"].map(
                lambda p: f"{int(str(p)[:4]) - 1}{str(p)[4:]}" if p and len(str(p)) == 8 else None)
            prev = fe.set_index(["code", "report_date"])["fee_ratio"]
            latest["fee_ratio_prev"] = [
                prev.get((c, latest.loc[c, "_prev_period"]), np.nan) for c in latest.index]
            out = out.join(latest[["goodwill", "eq_exc_min", "accounts_receiv", "revenue",
                                   "n_income", "n_income_attr_p", "n_cashflow_act",
                                   "fee_ratio", "fee_ratio_prev"]])
    return out


def build_quality_flags(engine, trade_date: str, codes: list[str],
                        cfg: QualityConfig | None = None, persist: bool = True) -> pd.DataFrame:
    """对某调仓日 universe 全量打分并落 quality_flag（影子，不剔除任何票）。"""
    if not codes:
        return pd.DataFrame()
    fina = load_quality_inputs(engine, trade_date, codes)
    res = screen_cross_section(fina, cfg)
    if persist and not res.empty:
        rows = pd.DataFrame({
            "trade_date": trade_date,
            "code": res.index,
            "n_flags": res["n_flags"].astype(int).values,
            "flags": [json.dumps(f, ensure_ascii=False) for f in res["flags"]],
        })
        BaseRepository(engine).upsert("quality_flag", rows, ["trade_date", "code"])
    return res


def latest_flags(engine, dt: str, codes: list[str],
                 min_flags: int = 2) -> dict[str, tuple[int, list[str]]]:
    """取 dt 之前最近一次打分中命中 ≥min_flags 的票。操作计划 risk_hint 用。"""
    repo = BaseRepository(engine)
    if not codes or not repo.table_exists("quality_flag"):
        return {}
    df = repo.read_sql(
        "SELECT code, n_flags, flags FROM quality_flag "
        "WHERE trade_date=(SELECT MAX(trade_date) FROM quality_flag WHERE trade_date<=:d) "
        "AND n_flags>=:n", {"d": dt, "n": min_flags})
    out: dict[str, tuple[int, list[str]]] = {}
    for _, r in df.iterrows():
        if r["code"] in codes:
            try:
                fl = json.loads(r["flags"]) if r["flags"] else []
            except Exception:  # noqa: BLE001
                fl = []
            out[str(r["code"])] = (int(r["n_flags"]), fl)
    return out
