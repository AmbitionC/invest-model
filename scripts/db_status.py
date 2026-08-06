"""数据库盘点：**全部表**的行数 + 物理体积 + 日期覆盖范围。只读，用于状态/体检。

  python scripts/db_status.py              # 走 .env / INVEST_DB_URL
  python scripts/db_status.py --brief      # 只打汇总，不逐表列圈定标的

2026-08-05 改（owner 问「数据库有多大、涵盖哪些方向」）：
  - 表清单不再硬编码 17 张，改为**从 schema.py 自动枚举全部表**（当前 59 张），
    漏盘点会随新表加入自动消失；
  - 增加 **物理体积**（MySQL 走 information_schema，数据/索引分列；SQLite 走 dbstat 或文件大小）；
  - 日期列**自动探测**（按候选列名顺序取表里真实存在的第一个），新表不用改这里；
  - 按「方向」（行情底座 / 财务 / 资金面 / 因子模型 / 回测 / 投顾实盘 / 复盘监测 /
    宏观情绪 / 套利 / 美股）分组小计，直接回答「涵盖了哪些方向」。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import inspect  # noqa: E402

from invest_model.data.engine import make_engine  # noqa: E402
from invest_model.data import schema as S  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

# 日期列候选（按优先级探测，取表里真实存在的第一个）
DATE_CANDIDATES = (
    "trade_date", "cal_date", "snapshot_date", "plan_date", "rec_date",
    "report_date", "period", "end_date", "list_date", "created_at",
)

# 方向分组：(方向名, 表名前缀/精确名列表)
GROUPS: list[tuple[str, tuple[str, ...]]] = [
    ("行情底座", ("trade_calendar", "stock_info", "stock_daily", "stock_adj",
                 "stock_namechange", "index_daily")),
    ("财务与基本面", ("stock_fundamental", "stock_fina_indicator", "stock_fina_ext",
                     "fina_express", "quality_flag")),
    ("资金面/筹码", ("stock_northbound_flow", "stock_hk_hold", "stock_margin",
                    "stock_margin_detail", "holder_trade")),
    ("因子与模型", ("universe_snapshot", "factor_exposure", "model_prediction",
                   "portfolio_target", "model_registry", "factor_ic_log")),
    ("回测", ("backtest_run", "backtest_nav", "backtest_trades")),
    ("投顾融合/实盘决策", ("advisor_reco", "advisor_theme", "current_holding", "action_plan",
                          "action_plan_account", "policy_shadow", "signal_scorecard")),
    ("复盘与监测", ("holding_snapshot", "account_snapshot", "watch_alert", "review_report")),
    ("宏观/情绪/杠杆", ("macro_series", "fear_daily", "fear_intraday", "leverage_signal")),
    ("套利模块", ("reverse_repo_daily", "cb_basic", "cb_daily", "dividend_event",
                 "watermeter_signal", "flow_score", "carry_signal", "alpha_candidate",
                 "sleeve_target", "arb_scorecard")),
    ("美股模块", ("us_stock_info", "us_stock_daily", "us_fundamental_q", "us_valuation",
                 "us_option_candidate", "us_action_plan", "us_plan_account",
                 "us_account_snapshot", "us_current_holding")),
]


def all_tables() -> list[str]:
    """从 schema.py 的 MetaData 枚举全部表名（新表自动纳入盘点）。"""
    return sorted(S.metadata.tables.keys())


def phys_sizes(repo: BaseRepository) -> dict[str, tuple[float, float]]:
    """{表名: (数据MB, 索引MB)}。取不到时返回空 dict（SQLite 无 dbstat 编译选项等）。"""
    url = str(repo.engine.url)
    try:
        if url.startswith("sqlite"):
            df = repo.read_sql(
                "SELECT name tbl, SUM(pgsize)/1048576.0 mb FROM dbstat GROUP BY name")
            return {r.tbl: (float(r.mb), 0.0) for r in df.itertuples()}
        df = repo.read_sql(
            "SELECT table_name tbl, data_length/1048576.0 d, index_length/1048576.0 i "
            "FROM information_schema.tables WHERE table_schema = DATABASE()")
        return {str(r.tbl): (float(r.d or 0), float(r.i or 0)) for r in df.itertuples()}
    except Exception:  # noqa: BLE001
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brief", action="store_true", help="跳过圈定标的逐行核对")
    a = ap.parse_args()

    engine = make_engine()
    repo = BaseRepository(engine)
    insp = inspect(engine)
    sizes = phys_sizes(repo)

    print("== 数据库盘点 ==")
    print(f"引擎: {engine.url.get_backend_name()}　｜　"
          f"schema 定义表数: {len(all_tables())}　｜　"
          f"体积口径: {'information_schema' if sizes and not str(engine.url).startswith('sqlite') else ('dbstat' if sizes else '不可得')}")
    print()

    grand_rows, grand_mb = 0, 0.0
    grand_missing: list[str] = []
    for gname, tbls in GROUPS:
        lines, g_rows, g_mb = [], 0, 0.0
        for tbl in tbls:
            if not repo.table_exists(tbl):
                grand_missing.append(tbl)
                lines.append(f"  {tbl:<24}{'(表不存在)':>14}")
                continue
            n = repo.get_row_count(tbl)
            d, i = sizes.get(tbl, (0.0, 0.0))
            g_rows += n
            g_mb += d + i
            span = ""
            if n:
                cols = {c["name"] for c in insp.get_columns(tbl)}
                dc = next((c for c in DATE_CANDIDATES if c in cols), None)
                if dc:
                    r = repo.read_sql(f"SELECT MIN(`{dc}`) lo, MAX(`{dc}`) hi FROM `{tbl}`")
                    span = f"  {str(r['lo'].iloc[0]):>10} ~ {str(r['hi'].iloc[0]):<10} ({dc})"
            mb = f"{d + i:>9.1f}" if (d + i) else f"{'—':>9}"
            lines.append(f"  {tbl:<24}{n:>14,}{mb}{span}")
        grand_rows += g_rows
        grand_mb += g_mb
        print(f"【{gname}】 {g_rows:,} 行 / {g_mb:.1f} MB")
        print("\n".join(lines))
        print()

    known = {t for _, ts in GROUPS for t in ts}
    other = [t for t in all_tables() if t not in known]
    if other:
        print(f"【未分组】{other}")
        print()

    print("=" * 72)
    print(f"合计（已分组表）：{grand_rows:,} 行　/　{grand_mb:,.1f} MB"
          + (f"　＝ {grand_mb/1024:.2f} GB" if grand_mb > 1024 else ""))
    if grand_missing:
        print(f"未建表（schema 有定义、库里没有）：{len(grand_missing)} 张 → {grand_missing}")

    try:
        d = repo.read_sql(
            "SELECT MAX(trade_date) hi, COUNT(DISTINCT trade_date) days FROM stock_daily")
        print(f"行情 stock_daily 最新交易日: {d['hi'].iloc[0]}  （共 {d['days'].iloc[0]} 个交易日）")
    except Exception:  # noqa: BLE001
        pass

    if a.brief:
        return

    curated = ("002384,300308,300502,002281,688048,002428,600141,300260,002158,"
               "688409,688596,600552,000725,603773,002371,688361,300666,600703,"
               "688106,002851,300466,002050,688316,688158,688629,688122,002897,"
               "300395,000833,002648,300750,600118,600160,688733").split(",")
    codes = [f"{c}.SZ" if c[0] in "03" else f"{c}.SH" for c in curated]
    try:
        ph = ",".join(f":c{i}" for i in range(len(codes)))
        params = {f"c{i}": c for i, c in enumerate(codes)}
        df = repo.read_sql(
            f"SELECT d.code, i.name, MAX(d.trade_date) hi, COUNT(*) n "
            f"FROM stock_daily d LEFT JOIN stock_info i ON d.code=i.ts_code "
            f"WHERE d.code IN ({ph}) GROUP BY d.code, i.name", params)
        have = set(df["code"]) if not df.empty else set()
        print("-" * 72)
        print(f"圈定标的核对：{len(have)}/{len(codes)} 有行情")
        for _, r in df.sort_values("code").iterrows():
            print(f"  {r['code']:<11}{str(r['name'] or ''):<8} 行数={int(r['n']):>4} 最新={r['hi']}")
        missing = [c for c in codes if c not in have]
        if missing:
            print(f"  ⚠️ 缺行情: {missing}")
    except Exception as e:  # noqa: BLE001
        print(f"圈定标的核对 ERROR: {e}")


if __name__ == "__main__":
    main()
