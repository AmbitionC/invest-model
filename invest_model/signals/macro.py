"""宏观读数层（P54，2026-08-04）——把 macro_series 长表还原成陈老师的固定读数。

**本模块只算读数，不产生任何买卖判断。** 宏观要影响决策必须先过 E47 预登记判据。
这条边界是刻意的：宏观择时在本系统里一次都没验过，而他自己的用法也不是"数据变差就减仓"
——他 2025-11-14 那期四项全部走弱，操作仍是"不增不减"，理由写得很清楚：
**"仓位调整的触发条件是判断改变，不是数据变差。"**

他每期固定读五项：
  ① 居民新增贷款（放款口径）→ 楼市（"房价周期就是居民债务周期"）
  ② M1 → 企业资金活跃度        ③ M2 → 货币总量松紧
  ④ 总贷款余额增速 → 信用扩张强度
  ⑤ 社融结构 → 拉动来自政府部门还是企业/居民
三条分析纪律：
  · **基数校正**——疫情年（2020/2022）不能直接进累计比较，须换基数可比的单月窗口；
    对应 `seasonality()`：先看这个月的环比方向历史上是增还是减，再谈同比。
  · **政策反证法**——政策已放松而数据仍恶化 ⟹ 约束不在政策端而在需求端。属论证结构，
    不可机械化，留在文档层。
  · **看政策文本措辞变化**——需要文本挖掘，当前不实现（见 P54「已知缺口」）。
"""

from __future__ import annotations

import pandas as pd

# 指标别名表：tushare 列名可能变，取第一个命中的。找不到返回 None，调用方须容忍。
_ALIAS: dict[str, tuple[str, ...]] = {
    "m1_yoy": ("cn_m.m1_yoy",),
    "m2_yoy": ("cn_m.m2_yoy",),
    "m0_yoy": ("cn_m.m0_yoy",),
    "cpi_yoy": ("cn_cpi.nt_yoy", "cn_cpi.cpi_yoy"),
    "ppi_yoy": ("cn_ppi.ppi_yoy", "cn_ppi.nt_yoy"),
    "pmi": ("cn_pmi.pmi010000", "cn_pmi.pmi"),
    "sf_stock": ("cn_sf.stk_endperiod",),              # 社融存量（月末）
    "sf_inc": ("cn_sf.inc_month", "sf_month.inc_month"),
    "gdp_nominal": ("cn_gdp.gdp",),                    # 名义 GDP（亿元，季度）
    "gdp_real_yoy": ("cn_gdp.gdp_yoy",),               # 实际 GDP 同比（%）
    "lpr_1y": ("shibor_lpr.1y", "shibor_lpr.lpr_1y"),
    "lpr_5y": ("shibor_lpr.5y", "shibor_lpr.lpr_5y"),
    "usdcny": ("fx_daily.bid_close", "fx_daily.close"),
}


def load_panel(repo, freq: str | None = None) -> pd.DataFrame:
    """macro_series → 宽表（index=period，columns=series）。库里没有该表时返回空帧。"""
    sql = "SELECT period, series, value FROM macro_series"
    params: dict = {}
    if freq:
        sql += " WHERE freq=:f"
        params["f"] = freq
    try:
        df = repo.read_sql(sql + " ORDER BY period", params)
    except Exception:  # noqa: BLE001 — 表不存在/无权限时静默降级
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.pivot_table(index="period", columns="series", values="value",
                          aggfunc="last").sort_index()


def pick(panel: pd.DataFrame, name: str) -> pd.Series | None:
    """按别名表取一条指标序列；全都不在返回 None。"""
    for col in _ALIAS.get(name, ()):
        if col in panel.columns:
            s = panel[col].dropna()
            if len(s):
                return s
    return None


def seasonality(s: pd.Series, month: int) -> dict | None:
    """基数校正的第一步：这个月的环比方向，历史上是增还是减？

    他 2024-02 判"假开门红"的全部依据就是这一步——"过去十年除 2016 外，1 月 M1 环比均减少"，
    所以那年 1 月 M1 反季节回升不能直接读同比。
    返回该自然月历史环比的 上升次数/样本数/中位环比。
    """
    if s is None or len(s) < 24:
        return None
    d = s.copy()
    d.index = pd.Index([str(i) for i in d.index])
    d = d[d.index.str.len() >= 6]
    mom = d.diff()
    m = pd.Index(d.index).str[4:6].astype(int)
    sel = mom[(m == month) & mom.notna()]
    if len(sel) < 5:
        return None
    return {"n": int(len(sel)), "up": int((sel > 0).sum()),
            "median_mom": float(sel.median()),
            "up_rate": float((sel > 0).mean())}


def yoy_from_level(s: pd.Series, periods: int = 12) -> pd.Series | None:
    """由存量水平序列自算同比（社融存量增速、名义 GDP 增速都要这一步）。"""
    if s is None or len(s) <= periods:
        return None
    return (s / s.shift(periods) - 1.0).dropna() * 100.0


def inflation_gauge(panel: pd.DataFrame) -> dict | None:
    """宏观通胀计（源自《通胀的玩笑这次开大了》2026-07-16）：
    **名义 GDP 增速 与 实际 GDP 增速 的金叉 ＝ 通胀技术性回归。**

    他自己在同一篇里就做了自我证伪（产能利用率 73% 六年最低、居民消费近零 ⟹
    归因输入性而非需求复苏），所以这条**是一个读数，不是一个信号**——
    金叉本身不构成任何仓位主张。
    """
    lvl = pick(panel, "gdp_nominal")
    real = pick(panel, "gdp_real_yoy")
    if lvl is None or real is None:
        return None
    nom = yoy_from_level(lvl, periods=4)          # 季度序列，4 期 = 同比
    if nom is None or nom.empty:
        return None
    idx = nom.index.intersection(real.index)
    if len(idx) < 4:
        return None
    n, r = float(nom[idx[-1]]), float(real[idx[-1]])
    prev = [(float(nom[i]), float(real[i])) for i in idx[-5:-1]]
    return {"period": str(idx[-1]), "nominal_yoy": n, "real_yoy": r,
            "gap": n - r, "cross": bool(n > r),
            "cross_new": bool(n > r and all(a <= b for a, b in prev))}


def chen_readings(repo) -> dict:
    """他的固定读数一次算齐。缺哪项就是哪项 None——不猜、不填充。"""
    panel = load_panel(repo)
    if panel.empty:
        return {"available": False, "reason": "macro_series 无数据（先跑 ingest_macro）"}
    out: dict = {"available": True, "latest_period": str(panel.index[-1])}
    for k in ("m1_yoy", "m2_yoy", "cpi_yoy", "ppi_yoy", "pmi", "lpr_1y", "lpr_5y"):
        s = pick(panel, k)
        out[k] = None if s is None else {"period": str(s.index[-1]), "value": float(s.iloc[-1])}
    # ④ 信用扩张强度：社融存量同比（由存量水平自算，口径透明）
    sf = yoy_from_level(pick(panel, "sf_stock"))
    out["sf_stock_yoy"] = (None if sf is None or sf.empty
                           else {"period": str(sf.index[-1]), "value": float(sf.iloc[-1])})
    # M1−M2 剪刀差：他读 M1 拐点时实际看的是企业活期相对总量的位置
    m1, m2 = pick(panel, "m1_yoy"), pick(panel, "m2_yoy")
    if m1 is not None and m2 is not None:
        idx = m1.index.intersection(m2.index)
        if len(idx):
            out["m1_m2_scissors"] = {"period": str(idx[-1]),
                                     "value": float(m1[idx[-1]] - m2[idx[-1]])}
    out["inflation_gauge"] = inflation_gauge(panel)
    # 基数校正：对最新月的 M1 给出该自然月的历史环比季节性
    if m1 is not None and len(m1):
        out["m1_seasonality"] = seasonality(m1, int(str(m1.index[-1])[4:6]))
    return out
