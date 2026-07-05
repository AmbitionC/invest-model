"""题材/行业拥挤度（HHI + Top-3 集中）——只读风控度量。

依据（验证 harness E7）：拥挤是量化/题材策略的核心失效模式——共享输入 → 组合雷同 →
一起踩踏。实盘投顾信号高度扎堆少数题材（半导体+算力等），同涨同跌、非独立样本，聚类稳健 t
一压就掉。故把「当前可动用标的的题材/行业集中度」做成常态度量，接进每日操作计划亮红。

本模块是**唯一真源**：`scripts/validation/common.py` 的 `theme_of`/`THEME_KEYWORDS` 从此处再导出，
E7 harness 与实盘 action_plan 用同一套口径。纯函数、无 DB 依赖、易测。

指标（越高越危险）：HHI（Σ 份额²，1/N~1）与 Top-3 份额。
预警线（经验，可调）：Top-3 份额 > 60% 或 HHI > 0.20 → ⚠️ 拥挤。
**只读**：不改组合、不下单，只出提示；真正限仓由 fuse_targets 的 industry_cap/sleeve_cap 执行。
"""

from __future__ import annotations

import pandas as pd

# 催化剂/主题关键词 → 归一化主题（用于扎堆聚类与拥挤度分桶；可扩充）
THEME_KEYWORDS: dict[str, list[str]] = {
    "半导体": ["半导体", "芯片", "存储", "晶圆", "封测", "先进制程", "光刻", "HBM", "DRAM"],
    "算力/AI": ["算力", "AI", "光模块", "CPO", "英伟达", "液冷", "服务器", "大模型", "PCB", "交换机"],
    "机器人": ["机器人", "减速器", "谐波", "丝杠", "Optimus", "人形", "灵巧手"],
    "黄金/有色": ["黄金", "贵金属", "金价", "白银", "有色", "铜", "稀土"],
    "医药": ["创新药", "医药", "CXO", "GLP", "减肥", "疫苗", "器械"],
    "军工": ["军工", "国防", "导弹", "航空", "航天", "船舶"],
    "新能源": ["锂电", "光伏", "储能", "电池", "固态", "风电"],
    "消费": ["白酒", "消费", "食品", "啤酒", "免税", "零售"],
    "券商/保险": ["券商", "保险", "证券", "非银"],
}

# 预警阈值
TOP3_THRESHOLD = 0.60
HHI_THRESHOLD = 0.20


def theme_of(catalyst: str) -> str:
    """催化剂文本 → 归一化主题（命中关键词优先，未命中记「其他」）。"""
    t = catalyst or ""
    for th, kws in THEME_KEYWORDS.items():
        if any(k in t for k in kws):
            return th
    return "其他"


def hhi(shares: pd.Series) -> float:
    """Herfindahl 集中度指数 Σ 份额²（1/N~1）。空/全零返回 nan。"""
    s = pd.to_numeric(shares, errors="coerce")
    s = s[s > 0]
    if s.empty:
        return float("nan")
    p = s / s.sum()
    return float((p ** 2).sum())


def concentration(shares: pd.Series) -> dict:
    """按分组份额（可为计数或权重）算集中度画像。

    返回 {n, hhi, top3_share, top3_detail:[(组名,份额)...], crowded:bool}。
    """
    s = pd.to_numeric(shares, errors="coerce")
    s = s[s.notna() & (s > 0)]
    tot = float(s.sum())
    if tot <= 0 or s.empty:
        return {"n": 0, "hhi": float("nan"), "top3_share": float("nan"),
                "top3_detail": [], "crowded": False}
    p = (s / tot).sort_values(ascending=False)
    top3 = p.head(3)
    h = float((p ** 2).sum())
    top3_share = float(top3.sum())
    # +eps 容忍浮点噪声：恰好等于阈值（如 5 主题均分 HHI=0.20）不算拥挤
    crowded = (top3_share > TOP3_THRESHOLD + 1e-9) or (h > HHI_THRESHOLD + 1e-9)
    return {"n": int(s.size), "hhi": h, "top3_share": top3_share,
            "top3_detail": [(str(k), float(v)) for k, v in top3.items()],
            "crowded": crowded}


def _detail_str(detail: list) -> str:
    return "、".join(f"{k} {v*100:.0f}%" for k, v in detail)


def crowding_hints(held_weights: dict[str, float], industry_map: dict[str, str],
                   advisor_catalysts: list[str] | None = None) -> list[str]:
    """给每日操作计划生成拥挤预警文案（触阈值才产出，否则空）。

    held_weights：{code: 组合权重}（当前持仓的真实敞口，按权重看行业拥挤）；
    industry_map：{code: 行业}；
    advisor_catalysts：当期有效投顾 long 推荐的催化剂文本列表（按主题看信号池拥挤）。

    两个口径互补：持仓行业 HHI 度量「钱压在哪」，投顾主题 HHI 度量「信号扎堆在哪」。
    """
    hints: list[str] = []

    # 1) 持仓行业拥挤（按权重）——补单行业>35% 阈值的盲区（多个中等行业同题材共振）
    if held_weights:
        ser = pd.Series({industry_map.get(c) or "未知": 0.0 for c in held_weights})
        for c, w in held_weights.items():
            ind = industry_map.get(c) or "未知"
            ser[ind] = ser.get(ind, 0.0) + float(w or 0.0)
        ser = ser[ser.index != "未知"]
        if len(ser) >= 2:
            info = concentration(ser)
            if info["crowded"]:
                hints.append(
                    f"⚠️ 持仓行业拥挤：HHI {info['hhi']:.2f}、Top-3 合计 {info['top3_share']*100:.0f}%"
                    f"（{_detail_str(info['top3_detail'])}）超阈值(Top3>60% 或 HHI>0.20)，"
                    f"同题材易踩踏，建议分散或限单行业仓位")

    # 2) 投顾 long 信号池主题拥挤（按条数）——你实测的「投顾扎堆半导体+算力」正是此项
    if advisor_catalysts:
        themes = pd.Series([theme_of(x) for x in advisor_catalysts])
        themes = themes[themes != "其他"]
        counts = themes.value_counts()
        if int(counts.sum()) >= 3 and len(counts) >= 2:
            info = concentration(counts)
            if info["crowded"]:
                hints.append(
                    f"⚠️ 投顾信号扎堆：{int(counts.sum())} 条 long 推荐集中于 "
                    f"Top-3 主题 {info['top3_share']*100:.0f}%"
                    f"（{_detail_str(info['top3_detail'])}）、HHI {info['hhi']:.2f}，"
                    f"非独立样本、同涨同跌，控制单题材总仓位")

    return hints
