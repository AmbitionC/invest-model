"""宏观数据层入库（P54）：把 tushare 宏观/利率/汇率接口拉成长表 macro_series。

源自陈老师的月度金融数据框架。他每期固定读五项：
  ① 居民新增贷款（放款口径）→ 楼市（"房价周期就是居民债务周期"）
  ② M1 → 企业资金活跃度      ③ M2 → 货币总量松紧
  ④ 总贷款余额增速 → 信用扩张强度
  ⑤ 社融结构 → 拉动来自政府部门还是企业/居民
配三条分析纪律：基数校正（疫情年不进累计比较）/ 政策反证法 / 看政策文本措辞变化。

**本脚本只负责把数拉进来，不做任何择时判断。**宏观要影响买卖决策必须先过 E47
预登记判据（docs/model_change_proposals.md P54 段）。

设计要点——**不写死 tushare 的列名**：
    各接口返回什么列我们并不预先知道（且 tushare 会改）。这里统一把返回帧的
    时间键识别出来，其余数值列全部 melt 成 (period, series="接口.列", value)。
    新增指标、接口改列名，都不用改表结构、不用改本脚本。

用法：
    python scripts/ingest_macro.py                      # 全量（首次）
    python scripts/ingest_macro.py --start-m 202401     # 增量
    python scripts/ingest_macro.py --probe              # 只探测可得性与列名，不落库
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.logger import get_logger  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402

logger = get_logger()

# 时间键候选（按优先级）：不同接口叫法不一，命中第一个即用
_PERIOD_KEYS = ("month", "quarter", "trade_date", "date", "cal_date", "end_date")

# 接口注册表：(名称, freq, 调用方式)
# freq: M 月度 / Q 季度（统一折算到季末月）/ D 日度
# 每条都 best-effort——某个接口无权限或改签名不阻断其余接口。
_MONTHLY = ("cn_m", "cn_cpi", "cn_ppi", "cn_pmi", "cn_sf", "sf_month")
_QUARTERLY = ("cn_gdp",)
# 日度：利率与汇率。他的宏观框架里 i 端（利率）与汇率各自成篇
# （《人民币汇率驱动因素》2024-04-09、《不赌降息》2024-04-25）。
_DAILY = (
    ("shibor_lpr", dict()),
    ("yc_cb", dict(ts_code="1001.CB", curve_type="0")),          # 中债国债收益率曲线
    ("fx_daily", dict(ts_code="USDCNY.FXCM")),
    ("us_tycr", dict()),                                         # 美债收益率
)


def _q_to_month(q: str) -> str:
    """季度键 → 季末月（2024Q1 → 202403）。无法解析时原样返回前 6 位。"""
    s = str(q).upper().replace("-", "")
    if "Q" in s:
        y, _, n = s.partition("Q")
        try:
            return f"{int(y):04d}{int(n) * 3:02d}"
        except ValueError:
            return s[:6]
    return s[:8]


def melt_frame(df: pd.DataFrame, iface: str, freq: str) -> pd.DataFrame:
    """把一个接口的返回帧摊平成长表 (period, series, value, freq, source)。"""
    if df is None or df.empty:
        return pd.DataFrame()
    key = next((k for k in _PERIOD_KEYS if k in df.columns), None)
    if key is None:
        logger.warning(f"{iface}: 找不到时间键（列={list(df.columns)}），跳过")
        return pd.DataFrame()
    d = df.copy()
    per = d[key].astype(str)
    d["_period"] = per.map(_q_to_month) if freq == "Q" else per.str.replace("-", "", regex=False)
    rows = []
    for col in d.columns:
        if col in (key, "_period") or col in _PERIOD_KEYS:
            continue
        v = pd.to_numeric(d[col], errors="coerce")
        if v.notna().sum() == 0:          # 纯文本列（如 ts_code）不入库
            continue
        rows.append(pd.DataFrame({"period": d["_period"], "series": f"{iface}.{col}",
                                  "value": v}))
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True).dropna(subset=["value"])
    out["freq"] = freq
    out["source"] = iface
    # 同一 (period, series) 去重保最后一条（tushare 偶有重复行）
    return out.drop_duplicates(subset=["period", "series"], keep="last")


def _call(pro, iface: str, **kw) -> pd.DataFrame:
    fn = getattr(pro, iface, None)
    if fn is None:
        logger.warning(f"{iface}: 客户端无此接口，跳过")
        return pd.DataFrame()
    return fn(**kw)


def collect(client: TushareClient, start_m: str, end_m: str,
            start_d: str, end_d: str, probe: bool = False) -> pd.DataFrame:
    pro = client.pro
    parts = []
    for iface in _MONTHLY:
        try:
            df = _call(pro, iface, start_m=start_m, end_m=end_m)
            logger.info(f"{iface}: rows={0 if df is None else len(df)} "
                        f"cols={[] if df is None or df.empty else list(df.columns)}")
            if not probe:
                parts.append(melt_frame(df, iface, "M"))
        except Exception as e:  # noqa: BLE001 — 单接口失败不阻断其余
            logger.warning(f"{iface} 拉取失败：{type(e).__name__}: {e}")
    for iface in _QUARTERLY:
        try:
            sq = f"{start_m[:4]}Q1"
            eq = f"{end_m[:4]}Q{max(1, min(4, (int(end_m[4:6]) + 2) // 3))}"
            df = _call(pro, iface, start_q=sq, end_q=eq)
            logger.info(f"{iface}: rows={0 if df is None else len(df)} "
                        f"cols={[] if df is None or df.empty else list(df.columns)}")
            if not probe:
                parts.append(melt_frame(df, iface, "Q"))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{iface} 拉取失败：{type(e).__name__}: {e}")
    for iface, kw in _DAILY:
        try:
            df = _call(pro, iface, start_date=start_d, end_date=end_d, **kw)
            logger.info(f"{iface}: rows={0 if df is None else len(df)} "
                        f"cols={[] if df is None or df.empty else list(df.columns)}")
            if not probe:
                parts.append(melt_frame(df, iface, "D"))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{iface} 拉取失败：{type(e).__name__}: {e}")
    parts = [p for p in parts if p is not None and not p.empty]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--start-m", default="200501")
    ap.add_argument("--end-m", default=None)
    ap.add_argument("--start-d", default="20050101")
    ap.add_argument("--end-d", default=None)
    ap.add_argument("--probe", action="store_true", help="只探测可得性与列名，不落库")
    args = ap.parse_args()
    today = pd.Timestamp.today()
    end_m = args.end_m or today.strftime("%Y%m")
    end_d = args.end_d or today.strftime("%Y%m%d")

    client = TushareClient()
    df = collect(client, args.start_m, end_m, args.start_d, end_d, probe=args.probe)
    if args.probe:
        print("probe 完成（未落库）")
        return
    if df.empty:
        raise SystemExit("宏观数据一条也没拉到——检查 TUSHARE_TOKEN 与接口权限")

    engine = make_engine(args.db) if args.db else make_engine()
    create_schema(engine)
    repo = BaseRepository(engine)
    n = repo.upsert("macro_series", df[["period", "series", "value", "freq", "source"]],
                    ["period", "series"])
    per_src = df.groupby("source").agg(行数=("value", "size"),
                                       指标数=("series", "nunique"),
                                       起=("period", "min"), 止=("period", "max"))
    print(per_src.to_string())
    print(f"\nupsert macro_series {n} 行（共 {df['series'].nunique()} 条指标）")


if __name__ == "__main__":
    main()
