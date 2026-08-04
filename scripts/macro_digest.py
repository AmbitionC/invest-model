"""宏观月报读数（P54）：把 macro_series 还原成陈老师的固定读数并打印。

**只读、只打印，不落库、不产生任何仓位主张。**
他自己的用法就不是"数据变差就减仓"——2025-11-14 那期四项全部走弱、M1 拐点向下，
操作仍是"不增不减"，理由写死在那篇里：**仓位调整的触发条件是判断改变，不是数据变差。**

用法：python scripts/macro_digest.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.macro import chen_readings, load_panel  # noqa: E402


def _fmt(x: dict | None, unit: str = "%") -> str:
    return "—" if not x else f"{x['value']:+.2f}{unit}（{x['period']}）"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    repo = BaseRepository(make_engine(args.db) if args.db else make_engine())

    panel = load_panel(repo)
    r = chen_readings(repo)
    print("=" * 78)
    print("宏观读数（P54·数据层·不构成任何仓位主张）")
    print("=" * 78)
    if not r.get("available"):
        print(f"  {r.get('reason')}")
        return
    print(f"  库内指标 {panel.shape[1]} 条 · 期数 {panel.shape[0]} · 最新期 {r['latest_period']}")
    print("\n  ── 他的固定读数 ──")
    print(f"  ② M1 同比        {_fmt(r.get('m1_yoy'))}        ← 企业资金活跃度")
    print(f"  ③ M2 同比        {_fmt(r.get('m2_yoy'))}        ← 货币总量松紧")
    print(f"     M1−M2 剪刀差  {_fmt(r.get('m1_m2_scissors'), 'pp')}")
    print(f"  ④ 社融存量同比    {_fmt(r.get('sf_stock_yoy'))}        ← 信用扩张强度")
    print(f"     CPI / PPI     {_fmt(r.get('cpi_yoy'))} / {_fmt(r.get('ppi_yoy'))}")
    print(f"     PMI           {_fmt(r.get('pmi'), '')}")
    print(f"     LPR 1Y / 5Y   {_fmt(r.get('lpr_1y'), '')} / {_fmt(r.get('lpr_5y'), '')}   ← i 端")

    g = r.get("inflation_gauge")
    print("\n  ── 宏观通胀计（名义 GDP vs 实际 GDP）──")
    if not g:
        print("     数据不足（需 cn_gdp 的 gdp 与 gdp_yoy）")
    else:
        print(f"     {g['period']}：名义 {g['nominal_yoy']:+.2f}% vs 实际 {g['real_yoy']:+.2f}%"
              f"｜差 {g['gap']:+.2f}pp｜{'金叉（平减指数为正）' if g['cross'] else '死叉（平减指数为负）'}"
              f"{'·本期新金叉' if g.get('cross_new') else ''}")
        print("     注：这是读数不是信号。他 2026-07-16 提出金叉的同一篇里就自我证伪了"
              "（产能利用率 73% 六年最低 ⟹ 输入性而非需求复苏）。")

    s = r.get("m1_seasonality")
    print("\n  ── 基数校正（先看季节性，再读同比）──")
    if not s:
        print("     样本不足")
    else:
        m = int(str(r["m1_yoy"]["period"])[4:6]) if r.get("m1_yoy") else 0
        print(f"     {m} 月 M1 的历史环比：{s['n']} 个样本中 {s['up']} 次上升"
              f"（上升率 {s['up_rate']:.0%}、中位环比 {s['median_mom']:+.2f}）")
        print("     他判 2024「假开门红」的全部依据就是这一步——"
              "反季节的月份不能直接读同比。")

    print("\n  ── 已知缺口（tushare 拿不到，他用得上）──")
    print("     ① 居民新增贷款分项（居民中长贷/短贷）：他的第①项读数，社融只给总量")
    print("     ② 失业保险领取金额（他的「打工人体温计」）  ③ RMBS 条件早偿率")
    print("     ④ 工资增速 g（决定其负债模型 g>i 是否成立，本库已有 verification 但无月频源）")
    print("     ⑤ 政策文本措辞变化（需文本挖掘）")


if __name__ == "__main__":
    main()
