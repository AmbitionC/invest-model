"""一次性维护：删除 action_plan 里套利模块的防御 sleeve 行（defense_A/alpha）。

背景：ARB_ENABLED=1"整体测"期间（arb 1064 修复后）生成的红利carry/可转债双低/盲区α
行混入了用户面向的每日计划。ARB_ENABLED 已改回默认 0，但历史 plan_date 的这些行留在库里，
前端仍显示。本脚本按 sleeve 精准删除（只删套利行，绝不碰投顾主导的持仓/买入/观察池行）。

安全：
  - 只 DELETE `sleeve IN ('defense_A','alpha')`，不按 code/plan_date 猜；
  - 先 SELECT 计数并打印将删的 (plan_date, sleeve, code, name)，再执行；
  - 幂等：删完再跑就是 0 行。

用法（GitHub Actions，带 INVEST_DB_URL）：
  python scripts/purge_arb_plan_rows.py            # 删所有 plan_date 的套利行
  python scripts/purge_arb_plan_rows.py --date 20260713   # 只删某决策日
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402

ARB_SLEEVES = ("defense_A", "alpha")


def main() -> None:
    ap = argparse.ArgumentParser(description="删除 action_plan 套利 sleeve 行")
    # 缺省 None → make_engine 走 resolve_db_url：INVEST_DB_URL 空则回退 MYSQL_*/DB_* env（生产）。
    ap.add_argument("--db", default=os.getenv("INVEST_DB_URL") or None)
    ap.add_argument("--date", default=None, help="仅删某 plan_date（YYYYMMDD）；缺省删全部")
    args = ap.parse_args()

    engine = make_engine(args.db)
    placeholders = ", ".join([f":s{i}" for i in range(len(ARB_SLEEVES))])
    params: dict = {f"s{i}": s for i, s in enumerate(ARB_SLEEVES)}
    where = f"sleeve IN ({placeholders})"
    if args.date:
        where += " AND plan_date = :d"
        params["d"] = args.date

    with engine.connect() as conn:
        preview = conn.execute(
            text(f"SELECT plan_date, sleeve, code, name FROM action_plan WHERE {where} "
                 f"ORDER BY plan_date, sleeve, code"), params).fetchall()

    print(f"将删除 {len(preview)} 行套利计划行（sleeve∈{ARB_SLEEVES}）：")
    for r in preview:
        print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]}")
    if not preview:
        print("无套利行，无需清理（幂等）。")
        return

    with engine.begin() as conn:
        res = conn.execute(text(f"DELETE FROM action_plan WHERE {where}"), params)
    print(f"已删除 {res.rowcount} 行。前端下次读取即恢复纯投顾主导计划。")


if __name__ == "__main__":
    main()
