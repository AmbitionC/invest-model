"""验证 harness 主入口：跑 E0–E4，产出 go/no-go 报告（只读 DB）。

  python scripts/validation/run_all.py                 # 走 .env / INVEST_DB_URL
  python scripts/validation/run_all.py --out results/validation_report.md

每个实验独立容错：单个失败不阻断整份报告，如实标注「数据不足/异常」。
本 harness 全程只读，不写库、不改业务逻辑。
"""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import BaseRepository, get_repo, trade_calendar  # noqa: E402


def e0_baseline(repo: BaseRepository) -> str:
    L = ["## E0 —— 数据可信基线（前置）", ""]
    tables = ["stock_daily", "stock_info", "index_daily", "factor_exposure",
              "model_prediction", "advisor_reco", "advisor_theme"]
    rows = []
    for t in tables:
        if repo.table_exists(t):
            try:
                n = repo.get_row_count(t)
            except Exception:
                n = "?"
            rows.append(f"{t}={n}")
        else:
            rows.append(f"{t}=缺")
    L.append("- 表行数：" + "，".join(rows))
    cal = trade_calendar(repo)
    if cal:
        L.append(f"- 行情跨度：{cal[0]}~{cal[-1]}（{len(cal)} 交易日）。")
        # 截面规模：最新交易日的股票数
        try:
            last = cal[-1]
            df = repo.read_sql("SELECT COUNT(DISTINCT code) n FROM stock_daily WHERE trade_date=:d",
                               {"d": last})
            usz = int(df["n"].iloc[0]) if not df.empty else 0
            trust = usz >= 1500
            L.append(f"- 最新截面标的数 ≈ {usz} → {'✅ 可信(≥1500)' if trust else '⚠️ 偏小(<1500)，IC/回测慎用'}。")
        except Exception as e:
            L.append(f"- 截面规模计算异常：{e}")
    else:
        L.append("- ⚠️ stock_daily 无数据，后续实验多半降级。")
    return "\n".join(L)


def _power_note(repo: BaseRepository) -> str:
    """统计功效声明：投顾历史越短、越扎堆，E1/E2/E3 结论越弱。"""
    import pandas as pd
    if not repo.table_exists("advisor_reco"):
        return "- advisor_reco 缺失，E1/E2/E3 无法评估。"
    df = repo.read_sql("SELECT rec_date FROM advisor_reco WHERE direction='long'")
    if df.empty:
        return "- advisor_reco 无 long 记录，E1/E2/E3 无法评估。"
    d = df["rec_date"].astype(str)
    span_days = 0
    try:
        dt = pd.to_datetime(d, format="%Y%m%d", errors="coerce").dropna()
        span_days = int((dt.max() - dt.min()).days)
    except Exception:
        pass
    weak = span_days < 180 or len(d) < 300
    return (
        f"- **投顾历史：{len(d)} 条 long / 跨度 {d.min()}~{d.max()}（约 {span_days} 天）。**\n"
        f"- E1/E2/E3 全部基于这段投顾持仓/推荐，"
        f"{'⚠️ **功效偏弱**：' if weak else ''}单一/少数题材窗口 → **仅聚类稳健 t 算数**，"
        f"且即便显著也难与「踩对一波题材 beta」完全区分。\n"
        f"- 主证据取 **最短等窗口（5 交易日）**（最少混淆）；长窗口高样本档多为「早批已涨」"
        f"确认样本（混淆 rec_date/题材），**不作主证据**。E4 用 model_prediction 多年月度，功效较足。"
    )


def safe(name: str, fn, repo) -> str:
    try:
        return fn(repo)
    except Exception:
        return (f"## {name}\n\n**运行异常（已捕获，不阻断报告）**：\n```\n"
                + traceback.format_exc()[-1500:] + "\n```")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="sqlite:///path 覆盖；留空走 .env/INVEST_DB_URL")
    ap.add_argument("--out", default="results/validation_report.md")
    args = ap.parse_args()

    repo = get_repo(args.db)

    import e1_risk
    import e2_advisor
    import e3_validate
    import e4_alpha
    import e5_promotion
    import e6_advisor_factor

    parts = [
        "# 验证报告（go/no-go）：投顾主导 + 量化辅助 各杠杆实证",
        "",
        "> 口径：净额/基准或行业相对超额/walk-forward；投顾推荐扎堆 → 用**聚类稳健 t**。",
        "> 全程只读 DB、不改业务逻辑。每个实验附预登记 H0 与过关判据。",
        "",
        safe("E0", e0_baseline, repo),
        "",
        safe("E1", e1_risk.run, repo),
        "",
        safe("E2", e2_advisor.run, repo),
        "",
        safe("E3", e3_validate.run, repo),
        "",
        safe("E4", e4_alpha.run, repo),
        "",
        safe("E5", e5_promotion.run, repo),
        "",
        safe("E6", e6_advisor_factor.run, repo),
        "",
        "## 数据范围与功效声明（务必先读）",
        safe("power", _power_note, repo),
        "",
        "## go/no-go 汇总",
        "见各节「结论」。**只有过关的杠杆进入实现阶段**（届时单独提计划、走 policy_shadow 影子 + kill-switch）。",
        "本 harness 在 master，push `ops/validation.trigger` 即自动重跑；投顾历史随时间累积，"
        "E1/E2/E3 功效逐轮增强，结论更硬。",
    ]
    report = "\n".join(parts)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n[written] {out}")


if __name__ == "__main__":
    main()
