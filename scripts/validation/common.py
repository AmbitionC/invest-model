"""验证 harness 共享工具（只读 DB）。

设计原则：
  - **只读**：仅 SELECT，绝不写库。
  - **容错降级**：表/列/数据缺失时返回空结果并附原因，不抛异常中断报告。
  - **口径统一**：基准相对超额、固定前瞻窗口（去 rec_date 混淆）、扎堆聚类稳健显著性。

被 e1_risk / e2_advisor / e3_validate / e4_alpha 复用。
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


def get_repo(db_url: str | None = None) -> BaseRepository:
    """连库（默认走 .env / INVEST_DB_URL）。db_url 可传 sqlite:///path 覆盖。"""
    return BaseRepository(make_engine(db_url) if db_url else make_engine())


# ── 交易日历 / 行情面板 ──────────────────────────────────────────
def trade_calendar(repo: BaseRepository, start: str | None = None,
                   end: str | None = None) -> list[str]:
    """从 stock_daily 取升序去重交易日（可选区间）。表缺失返回空。"""
    if not repo.table_exists("stock_daily"):
        return []
    where, params = [], {}
    if start:
        where.append("trade_date >= :s"); params["s"] = start
    if end:
        where.append("trade_date <= :e"); params["e"] = end
    w = (" WHERE " + " AND ".join(where)) if where else ""
    df = repo.read_sql(f"SELECT DISTINCT trade_date FROM stock_daily{w} ORDER BY trade_date", params)
    return [str(x) for x in df["trade_date"].tolist()] if not df.empty else []


def close_panel(repo: BaseRepository, codes: list[str], dates: list[str]) -> dict:
    """{code: {date: close}}，只取给定 codes×dates。分批避免 IN 过长。"""
    panel: dict[str, dict[str, float]] = {}
    if not codes or not dates:
        return panel
    dph = ",".join(f":d{i}" for i in range(len(dates)))
    dparams = {f"d{i}": d for i, d in enumerate(dates)}
    B = 400
    for i in range(0, len(codes), B):
        chunk = codes[i:i + B]
        cph = ",".join(f":c{j}" for j in range(len(chunk)))
        params = {**dparams, **{f"c{j}": c for j, c in enumerate(chunk)}}
        df = repo.read_sql(
            f"SELECT code, trade_date, close FROM stock_daily "
            f"WHERE code IN ({cph}) AND trade_date IN ({dph})", params)
        if df.empty:
            continue
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        for c, t, cl in zip(df["code"], df["trade_date"].astype(str), df["close"]):
            if pd.notna(cl):
                panel.setdefault(c, {})[t] = float(cl)
    return panel


def index_returns(repo: BaseRepository, codes: list[str], dates: list[str]) -> dict:
    """基准指数 {code: {date: close}}（index_daily）。"""
    out: dict[str, dict[str, float]] = {}
    if not repo.table_exists("index_daily"):
        return out
    for bc in codes:
        df = repo.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE code=:c ORDER BY trade_date",
            {"c": bc})
        if not df.empty:
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            out[bc] = {str(t): float(c) for t, c in zip(df["trade_date"], df["close"]) if pd.notna(c)}
    return out


def industry_map(repo: BaseRepository, codes: list[str] | None = None) -> dict:
    """{code: industry}（stock_info）。"""
    if not repo.table_exists("stock_info"):
        return {}
    if codes:
        ph = ",".join(f":c{i}" for i in range(len(codes)))
        df = repo.read_sql(
            f"SELECT ts_code, industry FROM stock_info WHERE ts_code IN ({ph})",
            {f"c{i}": c for i, c in enumerate(codes)})
    else:
        df = repo.read_sql("SELECT ts_code, industry FROM stock_info")
    return dict(zip(df["ts_code"], df["industry"])) if not df.empty else {}


def fwd_return(panel_code: dict, entry_date: str, exit_date: str) -> float | None:
    """单票 entry→exit 收益。"""
    if not panel_code:
        return None
    e = panel_code.get(entry_date); x = panel_code.get(exit_date)
    if e is None or x is None or e <= 0:
        return None
    return x / e - 1.0


def first_on_or_after(dates_sorted: list[str], d: str) -> str | None:
    import bisect
    i = bisect.bisect_left(dates_sorted, d)
    return dates_sorted[i] if i < len(dates_sorted) else None


# ── 统计：扎堆聚类稳健 t ────────────────────────────────────────
def cluster_robust_tstat(values: list[float], clusters: list) -> float:
    """按 cluster 分组的稳健 t 统计（均值/聚类稳健标准误）。

    投顾推荐高度扎堆同主题/同日 → 普通 t 把相关样本当独立、严重高估显著性。
    这里按 cluster 聚合（每簇取均值作为一个有效观测）再算 t，得到保守的有效 t。
    """
    v = np.asarray([x for x in values if x is not None and np.isfinite(x)], dtype=float)
    if len(v) < 2:
        return float("nan")
    if clusters is None or len(clusters) != len(values):
        # 无聚类信息：退回普通 t（并在报告里注明）
        sd = v.std(ddof=1)
        return float(v.mean() / (sd / math.sqrt(len(v)))) if sd > 0 else float("nan")
    # 按簇取均值 → 每簇 1 个有效观测
    df = pd.DataFrame({"v": values, "cl": clusters}).dropna()
    g = df.groupby("cl")["v"].mean().to_numpy()
    if len(g) < 2:
        return float("nan")
    sd = g.std(ddof=1)
    return float(g.mean() / (sd / math.sqrt(len(g)))) if sd > 0 else float("nan")


def summ(vals: list[float]) -> dict:
    """基础统计摘要。"""
    v = np.asarray([x for x in vals if x is not None and np.isfinite(x)], dtype=float)
    if len(v) == 0:
        return {"n": 0, "mean": float("nan"), "median": float("nan"), "win": float("nan")}
    return {"n": int(len(v)), "mean": float(v.mean()), "median": float(np.median(v)),
            "win": float((v > 0).mean())}


def pct(x: float, nd: int = 2) -> str:
    return "NA" if x is None or not np.isfinite(x) else f"{x*100:+.{nd}f}%"


def md_table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


# 催化剂关键词 → 主题（用于扎堆聚类与分桶；可扩充）
THEME_KEYWORDS = {
    "半导体": ["半导体", "芯片", "存储", "晶圆", "封测", "先进制程", "光刻"],
    "算力/AI": ["算力", "AI", "光模块", "CPO", "英伟达", "液冷", "服务器", "大模型"],
    "机器人": ["机器人", "减速器", "谐波", "丝杠", "Optimus", "人形", "灵巧手"],
    "黄金/有色": ["黄金", "贵金属", "金价", "白银", "有色", "铜", "稀土"],
    "医药": ["创新药", "医药", "CXO", "GLP", "减肥", "疫苗", "器械"],
    "军工": ["军工", "国防", "导弹", "航空", "航天", "船舶"],
    "新能源": ["锂电", "光伏", "储能", "电池", "固态", "风电"],
    "消费": ["白酒", "消费", "食品", "啤酒", "免税", "零售"],
}


def theme_of(catalyst: str) -> str:
    t = catalyst or ""
    for th, kws in THEME_KEYWORDS.items():
        if any(k in t for k in kws):
            return th
    return "其他"
