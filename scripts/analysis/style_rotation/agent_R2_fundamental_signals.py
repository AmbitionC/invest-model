#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
E34 / R2（盈利与景气驱动）—— 候选信号实现（**预登记版，判据与定义跑数前写死**）

状态：2026-08-02 本次执行 **DATA_UNAVAILABLE**。
      TUSHARE_TOKEN 已过期（minitick.top 与 api.tushare.pro 双端确认，
      index_dailybasic / fina_indicator / index_weight / daily_batch / cn_pmi / forecast
      全部返回 code=2002「token已过期」），其余公开数据源被出口代理 403 拒绝。
      本文件是**跑数前写死的信号定义与评估流程**，token 恢复后原样执行即可，
      不得在看到结果后回头改定义（防 p-hacking，遵治理惯例）。

命题：风格轮动的根因是成长腿与价值腿的**盈利增速差**变化。
被解释变量与判据严格照 BRIEF：未来 60 交易日「成长 − 价值」相对收益。

用法：
    python3 agent_R2_fundamental_signals.py            # 全量（需 tushare）
    python3 agent_R2_fundamental_signals.py --probe    # 只探测数据可得性与覆盖度
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "_r2_cache")
os.makedirs(CACHE, exist_ok=True)

# ===================================================================== 写死参数
END_DATE = "20260729"
START_DATE = "20100101"
HORIZON = 60                    # 未来 60 交易日
YOY_TD = 252                    # YoY 用 252 交易日（≈4 季度）
EXTREME_LO, EXTREME_HI = 0.20, 0.80
RHO_MIN, ACC_MIN, EPISODE_MIN = 0.15, 0.58, 30
RF = 0.02

# 腿定义（写死）
VALUE_IDX = "000922.CSI"        # 中证红利 = 价值腿
GROWTH_IDX = {"chinext": "399006.SZ", "star50": "000688.SH"}
MKT_IDX = "000300.SH"

# 公告可得日滞后（写死；仅用于自建成分股聚合路径，指数官方 PE 已是 PIT 则不重复施加）
# 报告期 -> 该期数据「最晚必然公开」的日期。按交易所法定披露截止日取，宁可保守。
ANN_LAG = {"0331": ("0430", 0),   # 一季报：当年 4/30
           "0630": ("0831", 0),   # 中报：当年 8/31
           "0930": ("1031", 0),   # 三季报：当年 10/31
           "1231": ("0430", 1)}   # 年报：**次年** 4/30


# ===================================================================== 数据层
def get_client():
    """返回 tushare pro 句柄；不可用则抛出带明确原因的异常。"""
    sys.path.insert(0, "/home/user/invest-model")
    from invest_model.sources.tushare_client import TushareClient
    return TushareClient().pro


def probe() -> dict:
    """探测 R2 所需每个接口 + 每个指数的可得性与覆盖度。结论写入 _r2_probe.json。"""
    res = {"ts": datetime.now().isoformat(), "endpoints": {}, "index_coverage": {}}
    try:
        pro = get_client()
    except Exception as e:                                   # noqa: BLE001
        res["fatal"] = f"{type(e).__name__}: {e}"
        _dump(res)
        return res
    for api, params in [
        ("index_dailybasic", {"ts_code": MKT_IDX, "start_date": "20260701", "end_date": END_DATE}),
        ("index_weight", {"index_code": MKT_IDX, "start_date": "20260601", "end_date": END_DATE}),
        ("daily_basic", {"trade_date": "20260729"}),
        ("fina_indicator_vip", {"period": "20260331"}),
        ("income_vip", {"period": "20260331"}),
        ("forecast_vip", {"period": "20260331"}),
        ("report_rc", {"start_date": "20260701", "end_date": END_DATE}),   # 分析师预期（多数版本无权限）
        ("cn_pmi", {"start_m": "202601", "end_m": "202607"}),
    ]:
        try:
            df = getattr(pro, api)(**params)
            res["endpoints"][api] = {"ok": True, "rows": len(df),
                                     "cols": list(df.columns)[:25]}
        except Exception as e:                               # noqa: BLE001
            res["endpoints"][api] = {"ok": False, "err": str(e)[:200]}
    for name, code in {**GROWTH_IDX, "dividend": VALUE_IDX, "hs300": MKT_IDX}.items():
        try:
            df = pro.index_dailybasic(ts_code=code, start_date="20050101", end_date=END_DATE)
            if df is None or df.empty:
                res["index_coverage"][name] = {"code": code, "ok": False, "err": "空数据（该指数不在 index_dailybasic 覆盖内）"}
            else:
                res["index_coverage"][name] = {
                    "code": code, "ok": True, "rows": len(df),
                    "first": str(df["trade_date"].min()), "last": str(df["trade_date"].max()),
                    "has_pe_ttm": bool(df["pe_ttm"].notna().any()) if "pe_ttm" in df else False,
                    "has_pb": bool(df["pb"].notna().any()) if "pb" in df else False,
                    "pe_ttm_first_valid": str(df.loc[df["pe_ttm"].notna(), "trade_date"].min())
                    if "pe_ttm" in df and df["pe_ttm"].notna().any() else None,
                }
        except Exception as e:                               # noqa: BLE001
            res["index_coverage"][name] = {"code": code, "ok": False, "err": str(e)[:200]}
    _dump(res)
    return res


def _dump(res: dict) -> None:
    with open(os.path.join(HERE, "_r2_probe.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)


def fetch_index_basic(pro, code: str) -> pd.DataFrame:
    """index_dailybasic 缓存拉取。返回 trade_date / pe_ttm / pb / total_mv。"""
    fp = os.path.join(CACHE, f"idb_{code.replace('.', '_')}.csv")
    if os.path.exists(fp):
        return pd.read_csv(fp)
    df = pro.index_dailybasic(ts_code=code, start_date=START_DATE, end_date=END_DATE)
    if df is None or df.empty:
        raise RuntimeError(f"index_dailybasic 无 {code} 数据（覆盖不足，禁止用代理硬凑）")
    df = df[["trade_date", "pe_ttm", "pb", "total_mv"]].sort_values("trade_date")
    df.to_csv(fp, index=False)
    return df


# ===================================================================== 前视对齐
def ann_available_date(period: str) -> str:
    """报告期 -> 实际公告可得日（法定披露截止日，保守）。

    这是本路线**最大的前视风险点**：绝不用报告期(end_date)直接对齐日频行情。
    2026Q1 -> 20260430；2025Q4(年报) -> 20260430；2025Q3 -> 20251031 …
    """
    y, md = period[:4], period[4:]
    tgt_md, yr_off = ANN_LAG[md]
    return f"{int(y) + yr_off}{tgt_md}"


def pit_align(fund: pd.DataFrame, dates: pd.Series, period_col="end_date",
              val_col="value") -> pd.Series:
    """把报告期财务数据按**公告可得日**前向填充到日频交易日上（point-in-time）。"""
    f = fund.copy()
    f["avail"] = f[period_col].astype(str).map(ann_available_date)
    f = f.sort_values("avail").drop_duplicates("avail", keep="last")
    s = pd.Series(f[val_col].values, index=f["avail"].astype(str))
    idx = pd.Index(dates.astype(str))
    return s.reindex(s.index.union(idx)).sort_index().ffill().reindex(idx)


# ===================================================================== 候选信号（写死定义）
def build_signals(idb: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """由 index_dailybasic 反推的指数层盈利与估值信号。

    EPS 代理：指数点位不直接给，但 pe_ttm 已含价格，故用
        earnings_yield = 1 / pe_ttm            （盈利收益率，规模无关）
        implied_roe    = pb / pe_ttm           （隐含 ROE_ttm）
        eps_index      = total_mv / pe_ttm     （指数总盈利，规模量纲，用于算增速）
    盈利增速用 eps_index 的 252 交易日 YoY —— 这是**盈利**的变换，不是价格或价格比值，
    符合 BRIEF 前置约束 1（价格在 total_mv 与 pe_ttm 中同分子分母抵消）。
    """
    out = {}
    for name, df in idb.items():
        d = df.copy()
        d["earn_yield"] = 1.0 / d["pe_ttm"]
        d["implied_roe"] = d["pb"] / d["pe_ttm"]
        d["eps_index"] = d["total_mv"] / d["pe_ttm"]
        d["earn_yoy"] = d["eps_index"] / d["eps_index"].shift(YOY_TD) - 1.0
        d["roe_chg"] = d["implied_roe"] - d["implied_roe"].shift(YOY_TD)
        out[name] = d.set_index("trade_date")
    return out


SIGNAL_DEFS = {
    # 名称: (说明, 计算函数(growth_df, value_df) -> Series, 预期符号)
    "S1_盈利增速差": ("成长腿盈利 YoY − 价值腿盈利 YoY（核心假设）",
                    lambda g, v: g["earn_yoy"] - v["earn_yoy"], "+"),
    "S2_盈利加速度差": ("S1 的 4 季度变化（增速差的二阶差分）",
                     lambda g, v: (g["earn_yoy"] - v["earn_yoy"]).diff(YOY_TD), "+"),
    "S3_隐含ROE差变化": ("(pb/pe_ttm) 成长 − 价值 的 4 季度变化",
                      lambda g, v: g["roe_chg"] - v["roe_chg"], "+"),
    "S4_盈利收益率差": ("价值腿盈利收益率 − 成长腿盈利收益率（估值差，越大成长越贵）",
                    lambda g, v: v["earn_yield"] - g["earn_yield"], "−"),
    "S5_盈利收益率差分位": ("S4 的 750 交易日滚动分位（**未处理成分股漂移，见报告 §4**）",
                      lambda g, v: (v["earn_yield"] - g["earn_yield"])
                      .rolling(750).rank(pct=True), "−"),
}


# ===================================================================== 评估层（与 power_audit 同口径）
def forward_rel(growth_px: pd.Series, value_px: pd.Series) -> pd.Series:
    sub = pd.concat([growth_px.rename("g"), value_px.rename("v")], axis=1).dropna()
    lg, lv = np.log(sub["g"]), np.log(sub["v"])
    return (lg.shift(-HORIZON) - lg) - (lv.shift(-HORIZON) - lv)


def evaluate(sig: pd.Series, fwd: pd.Series, label: str) -> dict:
    df = pd.concat([sig.rename("sig"), fwd.rename("y")], axis=1).dropna()
    if len(df) < 100:
        return {"信号": label, "备注": f"样本不足 n={len(df)}"}
    rho, p = stats.spearmanr(df["sig"], df["y"])
    mid = len(df) // 2
    r1, _ = stats.spearmanr(df["sig"].iloc[:mid], df["y"].iloc[:mid])
    r2, _ = stats.spearmanr(df["sig"].iloc[mid:], df["y"].iloc[mid:])
    lo, hi = df["sig"].quantile([EXTREME_LO, EXTREME_HI])
    hi_m, lo_m = df["sig"] >= hi, df["sig"] <= lo
    ext = df[hi_m | lo_m].copy()
    ext["ok"] = np.where(ext["sig"] >= hi, ext["y"] > 0, ext["y"] < 0)
    flag = hi_m.astype(int) - lo_m.astype(int)
    epi = int((flag.ne(flag.shift()) & flag.ne(0)).sum())
    acc = ext["ok"].mean()
    return {"信号": label, "n": len(df), "ρ": round(rho, 3), "p": round(p, 4),
            "ρ前半": round(r1, 3), "ρ后半": round(r2, 3),
            "分半同号": bool(np.sign(r1) == np.sign(r2)),
            "方向准确率": round(acc, 4), "极端档观测": len(ext), "独立episode": epi,
            "判据①": abs(rho) >= RHO_MIN and np.sign(r1) == np.sign(r2),
            "判据②": acc >= ACC_MIN and epi >= EPISODE_MIN}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true")
    args = ap.parse_args()

    print("=" * 78)
    print("E34 / R2 盈利与景气驱动 —— 预登记信号脚本")
    print("=" * 78)

    r = probe()
    if r.get("fatal") or not any(v.get("ok") for v in r.get("endpoints", {}).values()):
        print("\n【DATA_UNAVAILABLE】tushare 不可用，R2 路线无法执行。")
        print("  原因：", r.get("fatal", "全部接口返回错误"))
        print("  探测明细已写入 _r2_probe.json。")
        print("  本脚本的信号定义与判据均已跑数前写死；token 恢复后原样重跑即可。")
        return 2
    if args.probe:
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0

    pro = get_client()
    px = {}
    for name, f, c in [("dividend", "000922_csi.csv", "close"),
                       ("chinext", "spread_full.csv", "chinext"),
                       ("star50", "star50.csv", "close")]:
        d = pd.read_csv(os.path.join(HERE, f))
        px[name] = d.set_index("trade_date")[c].astype(float)

    idb = {"dividend": fetch_index_basic(pro, VALUE_IDX)}
    for n, c in GROWTH_IDX.items():
        idb[n] = fetch_index_basic(pro, c)
    feat = build_signals(idb)

    rows = []
    for gname in GROWTH_IDX:
        fwd = forward_rel(px[gname], px["dividend"])
        fwd.index = fwd.index.astype(int)
        for sname, (_desc, fn, _sign) in SIGNAL_DEFS.items():
            s = fn(feat[gname], feat["dividend"])
            s.index = s.index.astype(int)
            rows.append(evaluate(s, fwd, f"{gname} / {sname}"))
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n判据③（可交易性）与④（稳健性）仅对①②同时通过的信号计算——见报告。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
