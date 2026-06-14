#!/usr/bin/env python3
"""标的发现系统入口。

扫描市场，发现潜力个股和 ETF 候选，写入 discovery_candidates 表。

使用方式：
    python3 scripts/run_discovery.py --mode daily   # 通道A：大单异动（日频）
    python3 scripts/run_discovery.py --mode weekly  # 通道A+B+ETF（周频）
    python3 scripts/run_discovery.py --mode all --date 20260613

discovery_candidates 表记录所有候选，status='pending' 表示待人工确认。
确认后调用 promote_candidate() 将标的升级到 stock_pool。
"""

import argparse
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from invest_model.db import get_engine
from invest_model.models.ddl import create_all_tables
from invest_model.repositories.stock_pool_repo import StockPoolRepository
from invest_model.repositories.calendar_repo import CalendarRepository
from invest_model.discovery.stock_screener import StockScreener
from invest_model.discovery.etf_rotator import ETFDiscoveryScanner
from invest_model.discovery.report import save_candidates, list_pending

# ── 参数 ──────────────────────────────────────────────
parser = argparse.ArgumentParser(description="标的发现系统")
parser.add_argument(
    "--mode",
    choices=["daily", "weekly", "all"],
    default="all",
    help="运行模式: daily=通道A(大单异动), weekly=通道A+B+ETF, all=全部",
)
parser.add_argument("--date", default=None, help="扫描日期 YYYYMMDD（默认最新交易日）")
parser.add_argument(
    "--list", action="store_true", help="仅列出当前有效候选，不运行新扫描"
)
args = parser.parse_args()

# ── 初始化 ──────────────────────────────────────────
engine = get_engine()
create_all_tables(engine)

pool_repo = StockPoolRepository(engine)
cal = CalendarRepository(engine)

# 确定扫描日期
if args.date:
    scan_date = args.date
else:
    dates = cal.get_trade_dates("20200101", datetime.now().strftime("%Y%m%d"))
    scan_date = dates[-1] if dates else datetime.now().strftime("%Y%m%d")

# ── 仅列出候选 ────────────────────────────────────────
if args.list:
    pending = list_pending(engine, scan_date)
    if pending.empty:
        print("当前无有效候选（pending）")
    else:
        print(f"\n当前有效候选 ({len(pending)} 只)：")
        print(f"  {'代码':12s} {'名称':10s} {'来源':18s} {'评分':6s} {'扫描日':8s} {'到期日':8s}")
        print("  " + "-" * 80)
        for _, row in pending.iterrows():
            print(
                f"  {row['code']:12s} {str(row.get('name','') or ''):10s} "
                f"{row['source']:18s} {float(row['score'] or 0):.4f}  "
                f"{row['scan_date']}  {row['expire_date']}"
            )
            if row.get("reason"):
                print(f"    └ {row['reason'][:100]}")
    sys.exit(0)

# ── 获取已在池内的标的（排除候选）────────────────────
all_pool = pool_repo.get_pool("core")
etf_pool = pool_repo.get_pool("etf")
import pandas as pd
all_in_pool = pd.concat([all_pool, etf_pool], ignore_index=True)
exclude_codes = all_in_pool["code"].tolist() if not all_in_pool.empty else []

print("=" * 70)
print(f"  标的发现系统 [mode={args.mode}]")
print("=" * 70)
print(f"  扫描日期: {scan_date}")
print(f"  已排除池内标的: {len(exclude_codes)} 只")

total_new = 0

# ── 通道 A：大单异动（日频，daily / weekly / all）──────
if args.mode in ("daily", "weekly", "all"):
    print(f"\n{'─'*60}")
    print("  通道 A：大单异动扫描")
    print("─" * 60)

    screener = StockScreener(engine)
    channel_a = screener.scan_daily(scan_date, exclude_codes=exclude_codes)

    if channel_a:
        for c in channel_a:
            print(f"  [{c.source}] {c.code:12s} score={c.score:.4f}")
            print(f"    └ {c.reason[:120]}")
        n = save_candidates(channel_a, engine)
        total_new += n
        print(f"  → 写入 {n} 条新候选")
    else:
        print("  → 无符合条件的大单异动标的")

# ── 通道 B：板块补涨（周频，weekly / all）──────────────
if args.mode in ("weekly", "all"):
    print(f"\n{'─'*60}")
    print("  通道 B：板块补涨扫描")
    print("─" * 60)

    channel_b = screener.scan_weekly(scan_date, exclude_codes=exclude_codes)

    if channel_b:
        for c in channel_b:
            print(f"  [{c.source}] {c.code:12s} score={c.score:.4f}")
            print(f"    └ {c.reason[:120]}")
        n = save_candidates(channel_b, engine)
        total_new += n
        print(f"  → 写入 {n} 条新候选")
    else:
        print("  → 无符合条件的补涨候选")

# ── ETF 轮动发现（周频，weekly / all）──────────────────
if args.mode in ("weekly", "all"):
    print(f"\n{'─'*60}")
    print("  ETF 轮动发现扫描")
    print("─" * 60)

    etf_scanner = ETFDiscoveryScanner(engine, top_n=5)
    etf_candidates = etf_scanner.scan(scan_date, exclude_codes=exclude_codes)

    if etf_candidates:
        for c in etf_candidates:
            print(f"  [{c.source}] {c.code:12s} score={c.score:.4f}")
            print(f"    └ {c.reason[:120]}")
        n = save_candidates(etf_candidates, engine)
        total_new += n
        print(f"  → 写入 {n} 条新候选")
    else:
        print("  → 无符合条件的 ETF 候选")

# ── 汇总 ─────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  扫描完成，共新增 {total_new} 条候选")
print()

pending = list_pending(engine, scan_date)
if not pending.empty:
    print(f"  当前有效候选 ({len(pending)} 只)：")
    for _, row in pending.iterrows():
        print(f"    {row['code']:12s} [{row['source']:18s}] score={float(row.get('score') or 0):.4f}  到期:{row['expire_date']}")

print()
print("  提示：使用以下命令将候选升级到股票池：")
print("    from invest_model.discovery.report import promote_candidate")
print("    promote_candidate('XXXXXX.SH', 'core', engine)")
print("=" * 70)
