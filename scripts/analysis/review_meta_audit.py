"""复盘引擎自审计：用内存 SQLite 构造最小数据集，直接跑生产代码验证复盘口径缺陷。

背景：2026-08-02 周复盘复核发现两处口径问题。为避免"靠读报表推断"式的误判把系统改坏，
本脚本把结论落成**可复跑的实证**——直接调用生产函数 `invest_model.review.execution.reconcile`，
不 mock、不改逻辑，只喂数据看输出。

  python scripts/analysis/review_meta_audit.py

纯内存 SQLite，零外部依赖，不读写任何生产数据、不落库。退出码 0=全部复现，1=有项目未复现
（未复现即说明该缺陷已被修复或原判断有误，应回头改文档而不是改代码）。

审计项：
  A. 计划股数=0 的 buy 指令被判「已执行 100%」并计入 buy_fill_rate 分子分母
  B. 强风控卖出未执行时，告警熄火由「价格回升」驱动而非「执行行为」驱动
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
    """A：空指令（计划股数=0）污染买点兑现率。

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
        ("空指令被判 executed 且执行率 100%",
         empty["status"] == "executed" and empty["executed_ratio"] == 1.0,
         f"status={empty['status']} ratio={empty['executed_ratio']}"),
        ("真指令未执行被正确判 not_executed",
         real["status"] == "not_executed", f"status={real['status']}"),
        ("空指令同时进入 buy_fill_rate 分子与分母",
         (bf["num"], bf["den"]) == (1, 2), f"buy_fill_rate={bf['num']}/{bf['den']}（剔除空指令应为 0/1）"),
    ]
    log = [f"    {'✓' if ok else '✗'} {name} —— {detail}" for name, ok, detail in checks]
    log.append(f"    → 真实兑现 0/1 = 0%，报表口径 {bf['num']}/{bf['den']} = "
               f"{bf['num'] / bf['den']:.0%}")
    return all(ok for _, ok, _ in checks), log


def audit_b() -> tuple[bool, list[str]]:
    """B：告警熄火由价格驱动。

    两组唯一差别是窗口末价格；执行事实（未执行、仍持有）完全相同。
    熄火阈值见 execution.py：condition_still_valid = last_close <= stop_price * 1.02
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

    def alerted(o: dict) -> bool:
        """复盘第六段 ⚠️ 纪律告警的四条件（见 review.py review_execution）。"""
        return bool(o["strong_risk"] and o["status"] == "not_executed"
                    and o.get("condition_still_valid") and o.get("still_held"))

    lo, hi = run(8.5), run(9.5)      # 阈值 = 9.0 * 1.02 = 9.18
    checks = [
        ("两组执行事实完全相同（均 not_executed 且仍持有）",
         lo["status"] == hi["status"] == "not_executed"
         and lo["still_held"] and hi["still_held"],
         f"低价组={lo['status']} 高价组={hi['status']}"),
        ("仅价格回升即让告警从「响」变「静默」",
         alerted(lo) and not alerted(hi),
         f"末价8.5→{'响' if alerted(lo) else '静默'}；末价9.5→{'响' if alerted(hi) else '静默'}"),
        ("价格回升时未执行成本转正（不卖反而少亏）",
         (hi["nonexec_cost"] or 0) > 0, f"nonexec_cost={hi['nonexec_cost']:+,.0f} 元"),
    ]
    log = [f"    {'✓' if ok else '✗'} {name} —— {detail}" for name, ok, detail in checks]
    return all(ok for _, ok, _ in checks), log


def main() -> None:
    audits = [
        ("A", "空指令污染买点兑现率（buy_fill_rate）", audit_a),
        ("B", "强风控告警熄火由价格驱动而非执行驱动", audit_b),
    ]
    print("=" * 78)
    print("复盘引擎自审计（内存 SQLite·直连生产函数 reconcile·不碰生产数据）")
    print("=" * 78)
    allok = True
    for tag, title, fn in audits:
        ok, log = fn()
        allok &= ok
        print(f"\n[{tag}] {title}：{'复现' if ok else '未复现'}")
        print("\n".join(log))
    print()
    print("=" * 78)
    print("两项均复现——结论可写入复盘" if allok else
          "有项目未复现——缺陷可能已修或原判断有误，应回头改文档而非改代码")
    print("=" * 78)
    sys.exit(0 if allok else 1)


if __name__ == "__main__":
    main()
