"""复盘引擎自审计：用内存 SQLite 构造最小数据集，直接跑生产代码验证复盘口径。

背景：2026-08-02 周复盘复核发现两处口径缺陷，本脚本当时把结论落成可复跑的实证
（直接调用生产函数 `invest_model.review.execution.reconcile`，不 mock、只喂数据看输出）。

**2026-08-08 两项缺陷已修复（handoff §1.1/§1.4），本脚本随之转为回归护栏**：
断言从「缺陷复现」翻转为「修复行为在位」。退出码 0=修复在位，1=回归
（缺陷重现，须回查 execution.py 的 no_op / alert_state 逻辑）。
缺陷原始复现版见 git 历史（2026-08-08 前）。

  python scripts/analysis/review_meta_audit.py

纯内存 SQLite，零外部依赖，不读写任何生产数据、不落库。

审计项（修复后的预期行为）：
  A. 计划股数=0 的 buy 指令判 no_op，不进 buy_fill_rate 分子分母
     （修复前：被 1 手容忍带判「已执行 100%」，兑现率 1/2 虚报成 8/9）
  B. 强风控卖出未执行、价格回升时告警降级为 lapsed 留痕，不静默消失
     （修复前：熄火由「价格回升」驱动，反弹一次计数即归零）
"""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.review.execution import reconcile  # noqa: E402

CAL = [f"2026070{d}" for d in range(1, 10)] + ["20260710"]
ASOF = "20260710"


def _base_tables(cx) -> None:
    """交易日历 + 空的价格/计划骨架（各审计项自行填充业务行）。"""
    cx.execute(text("CREATE TABLE index_daily (code TEXT, trade_date TEXT, close REAL)"))
    for d in CAL:
        cx.execute(text("INSERT INTO index_daily VALUES ('000300.SH', :d, 4000)"), {"d": d})
    cx.execute(text("CREATE TABLE holding_snapshot (snapshot_date TEXT, code TEXT, "
                    "shares REAL, asset_type TEXT)"))
    cx.execute(text("CREATE TABLE action_plan (plan_date TEXT, code TEXT, name TEXT, "
                    "action TEXT, shares_delta REAL, reason TEXT, stop_price REAL, "
                    "ref_price REAL, trigger_hint TEXT)"))
    cx.execute(text("CREATE TABLE stock_daily (code TEXT, trade_date TEXT, close REAL, "
                    "low REAL, high REAL)"))


def audit_a() -> tuple[bool, list[str]]:
    """A：空指令（计划股数=0）应判 no_op、不进买点兑现率。

    对照组同一批数据里放一条真买入指令（计划 100 股、实际没买），看两者被判成什么。
    """
    eng = create_engine("sqlite://")
    with eng.begin() as cx:
        _base_tables(cx)
        for d in CAL:                      # 持仓全程不动 = 计划完全没执行
            for c, sh in (("AAA.SZ", 500.0), ("BBB.SZ", 500.0)):
                cx.execute(text("INSERT INTO holding_snapshot VALUES (:d,:c,:s,'stock')"),
                           {"d": d, "c": c, "s": sh})
            for c in ("AAA.SZ", "BBB.SZ"):
                cx.execute(text("INSERT INTO stock_daily VALUES (:c,:d,10,9.5,10.5)"),
                           {"c": c, "d": d})
        # 空指令：计划买 0 股（线上 0703-0707 计划里共 7 条同款）
        cx.execute(text("INSERT INTO action_plan VALUES ('20260702','AAA.SZ','空指令买入',"
                        "'buy',0,'研报速通',NULL,10.0,NULL)"))
        # 真指令：计划买 100 股
        cx.execute(text("INSERT INTO action_plan VALUES ('20260702','BBB.SZ','真买入',"
                        "'buy',100,'研报速通',NULL,10.0,NULL)"))

    rec = reconcile(BaseRepository(eng), ASOF)
    by = {o["code"]: o for o in rec["orders"]}
    bf = rec["metrics"]["buy_fill_rate"]
    empty, real = by["AAA.SZ"], by["BBB.SZ"]

    checks = [
        ("空指令被判 no_op（不再走 1 手容忍带）",
         empty["status"] == "no_op",
         f"status={empty['status']} ratio={empty['executed_ratio']}"),
        ("真指令未执行仍被正确判 not_executed",
         real["status"] == "not_executed", f"status={real['status']}"),
        ("buy_fill_rate 分子分母剔除空指令",
         (bf["num"], bf["den"]) == (0, 1),
         f"buy_fill_rate={bf['num']}/{bf['den']}（修复前为 1/2）"),
        ("no_op 计数进 metrics（报表可见不隐身）",
         rec["metrics"].get("n_no_op") == 1, f"n_no_op={rec['metrics'].get('n_no_op')}"),
    ]
    log = [f"    {'✓' if ok else '✗'} {name} —— {detail}" for name, ok, detail in checks]
    log.append(f"    → 报表口径 = 真实兑现 {bf['num']}/{bf['den']}")
    return all(ok for _, ok, _ in checks), log


def audit_b() -> tuple[bool, list[str]]:
    """B：价格回升熄火应降级为 lapsed 留痕，不静默。

    两组唯一差别是窗口末价格；执行事实（未执行、仍持有）完全相同。
    阈值见 execution.py：condition_still_valid = last_close <= stop_price * 1.02；
    修复后 alert_state：条件仍成立=active（滚动告警）、价格回升=lapsed（历史留痕）。
    """
    stop = 9.0

    def run(last_px: float) -> dict:
        eng = create_engine("sqlite://")
        with eng.begin() as cx:
            _base_tables(cx)
            for d in CAL:
                cx.execute(text("INSERT INTO holding_snapshot VALUES (:d,'CCC.SZ',1000,'stock')"),
                           {"d": d})
                px = 8.5 if d < CAL[-1] else last_px
                cx.execute(text("INSERT INTO stock_daily VALUES ('CCC.SZ',:d,:p,:l,:h)"),
                           {"d": d, "p": px, "l": px - 0.2, "h": px + 0.2})
            cx.execute(text("INSERT INTO action_plan VALUES ('20260702','CCC.SZ','某持仓',"
                            "'sell',-1000,'硬止损(-8%)',:s,10.0,NULL)"), {"s": stop})
        return reconcile(BaseRepository(eng), ASOF)["orders"][0]

    lo, hi = run(8.5), run(9.5)      # 阈值 = 9.0 * 1.02 = 9.18
    checks = [
        ("两组执行事实完全相同（均 not_executed 且仍持有）",
         lo["status"] == hi["status"] == "not_executed"
         and lo["still_held"] and hi["still_held"],
         f"低价组={lo['status']} 高价组={hi['status']}"),
        ("条件仍成立时告警 active（滚动告警）",
         lo.get("alert_state") == "active", f"末价8.5→alert_state={lo.get('alert_state')}"),
        ("价格回升时降级 lapsed 留痕（不再静默消失）",
         hi.get("alert_state") == "lapsed", f"末价9.5→alert_state={hi.get('alert_state')}"),
        ("未执行成本仍如实计算（价格回升时为正=不卖少亏的事实保留）",
         (hi["nonexec_cost"] or 0) > 0, f"nonexec_cost={hi['nonexec_cost']:+,.0f} 元"),
    ]
    log = [f"    {'✓' if ok else '✗'} {name} —— {detail}" for name, ok, detail in checks]
    return all(ok for _, ok, _ in checks), log


def main() -> None:
    audits = [
        ("A", "空指令判 no_op、不进买点兑现率（修 2026-08-08）", audit_a),
        ("B", "价格回升熄火降级 lapsed 留痕、不静默（修 2026-08-08）", audit_b),
    ]
    print("=" * 78)
    print("复盘引擎自审计·回归护栏（内存 SQLite·直连生产函数 reconcile·不碰生产数据）")
    print("=" * 78)
    allok = True
    for tag, title, fn in audits:
        ok, log = fn()
        allok &= ok
        print(f"\n[{tag}] {title}：{'修复在位' if ok else '❌回归'}")
        print("\n".join(log))
    print()
    print("=" * 78)
    print("两项修复均在位" if allok else
          "有项目回归——原缺陷重现，回查 execution.py 的 no_op / alert_state 逻辑")
    print("=" * 78)
    sys.exit(0 if allok else 1)


if __name__ == "__main__":
    main()
