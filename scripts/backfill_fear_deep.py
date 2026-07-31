"""恐慌指数深度历史回填（tushare 流式版·不落全市场日线）。

背景：生产库 stock_daily 全市场深度只到 ~2023 年初，`backfill_fear.py`（查库版）
最多回填到 ~2023-08。要把恐慌曲线拉到 10 年（覆盖 2015 顶、2016 熔断、2018 熊、
2021 顶），需要 2014 年起的全市场日线——约 900 万行，**不该灌进生产库**。

本脚本改为在运行器内存中流式计算：
  1. 从 tushare 逐交易日 `daily(trade_date=...)` 拉全市场行情（1 天 1 次调用），
     按年分块只保留计算窗口（目标日前 200 自然日）需要的部分；
  2. 基准指数（默认沪深300）一次性拉全区间；
  3. 逐目标日把预载帧喂给 ``fear_gauge(stock_df=..., idx_df=...)``——与生产/查库版
     **同一套公式同一份代码**，只是数据来源不同；
  4. 仅把结果 upsert 进 ``fear_daily``（每天 1 行），全市场行情用完即弃。

默认跳过 fear_daily 已有的日期（与查库版回填衔接、中断可续跑）；``--force`` 覆盖重算。

用法：
  python scripts/backfill_fear_deep.py --start 20150101 --end 20231231
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.fear import fear_gauge  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402
from scripts.fear_gauge import persist_fear  # noqa: E402

STOCK_LOOKBACK_DAYS = 200   # fear_gauge 全市场窗口（自然日），与 signals/fear.py 一致
IDX_LOOKBACK_DAYS = 420     # fear_gauge 指数窗口（自然日）


def _shift(dt: str, days: int) -> str:
    return (pd.Timestamp(dt) - pd.Timedelta(days=days)).strftime("%Y%m%d")


def _fetch_day(ts: TushareClient, d: str, attempts: int = 5, cooldown: int = 60):
    """单日全市场拉取（镜像级韧性）：客户端内置重试耗尽后再加长冷却重试。

    镜像偶发分钟级不可用（ReadTimeout 连发）会击穿内置短重试；这里每次失败
    冷却 60s 再试，共 5 轮（约 5 分钟窗口）。仍失败则抛 RuntimeError，由调用方
    优雅收尾——已算天数都已落库，bump 触发器续跑即可。"""
    for k in range(attempts):
        try:
            return ts.get_daily_bulk(d)
        except Exception as e:  # noqa: BLE001
            print(f"  拉取 {d} 失败({k+1}/{attempts}): {repr(e)[:80]}，{cooldown}s 后再试",
                  flush=True)
            time.sleep(cooldown)
    raise RuntimeError(f"拉取 {d} 连续 {attempts} 轮失败（镜像不可用），中断续跑")


def main() -> None:
    ap = argparse.ArgumentParser(description="恐慌指数深度回填（tushare 流式）")
    ap.add_argument("--db", default=None)
    ap.add_argument("--start", required=True, help="目标区间起 YYYYMMDD")
    ap.add_argument("--end", required=True, help="目标区间止 YYYYMMDD")
    ap.add_argument("--benchmark", default="000300.SH")
    ap.add_argument("--force", action="store_true", help="已有日期也覆盖重算")
    args = ap.parse_args()

    engine = make_engine(args.db) if args.db else make_engine()
    repo = BaseRepository(engine)
    ts = TushareClient()

    cal = ts.get_trade_calendar(_shift(args.start, STOCK_LOOKBACK_DAYS), args.end)
    open_days = sorted(cal[cal["is_open"] == 1]["cal_date"].astype(str))
    targets = [d for d in open_days if args.start <= d <= args.end]
    if not targets:
        print("区间内无交易日。")
        return

    if not args.force:
        have = set(repo.read_sql(
            "SELECT trade_date FROM fear_daily WHERE trade_date>=:s AND trade_date<=:e",
            {"s": args.start, "e": args.end})["trade_date"].astype(str))
        todo = [d for d in targets if d not in have]
    else:
        todo = targets
    print(f"目标 {len(targets)} 个交易日，其中待算 {len(todo)} 天（已有跳过={not args.force}）")
    if not todo:
        return

    idx_lo = _shift(min(todo), IDX_LOOKBACK_DAYS)
    idx_df = ts.get_index_daily(args.benchmark, idx_lo, args.end)
    idx_df = idx_df[["code", "trade_date", "close"]].copy()
    idx_df["trade_date"] = idx_df["trade_date"].astype(str)
    print(f"基准 {args.benchmark} [{idx_lo}~{args.end}] {len(idx_df)} 行")

    # 全市场帧缓存：{trade_date: DataFrame(code,trade_date,close,pct_chg)}
    # 按目标日推进，仅保留 200 自然日回看窗口内的帧，用完即弃。
    frames: dict[str, pd.DataFrame] = {}
    ok = skip = 0
    t0 = time.time()
    for i, dt in enumerate(todo, 1):
        win_lo = _shift(dt, STOCK_LOOKBACK_DAYS)
        for stale in [d for d in frames if d < win_lo]:
            del frames[stale]
        try:
            for d in open_days:
                if win_lo <= d <= dt and d not in frames:
                    raw = _fetch_day(ts, d)
                    if raw is None or raw.empty:
                        frames[d] = pd.DataFrame(columns=["code", "trade_date", "close", "pct_chg"])
                    else:
                        f = raw[["code", "trade_date", "close", "pct_chg"]].copy()
                        f["trade_date"] = f["trade_date"].astype(str)
                        frames[d] = f
        except RuntimeError as e:
            # 镜像持续不可用：优雅收尾而非崩溃——进度已逐日落库，续跑自动接续。
            print(f"⚠️ {e}；本轮到 {dt} 前中断（已落库 {ok} 天），bump 触发器续跑剩余段。")
            break
        stock_df = pd.concat(frames.values(), ignore_index=True)
        try:
            g = fear_gauge(engine, dt, benchmark=args.benchmark,
                           stock_df=stock_df, idx_df=idx_df)
            persist_fear(engine, g)
            ok += 1
        except Exception as e:  # noqa: BLE001 — 单日样本不足则跳过不阻断
            skip += 1
            print(f"  跳过 {dt}: {repr(e)[:90]}")
        if i % 20 == 0 or i == len(todo):
            el = time.time() - t0
            print(f"  进度 {i}/{len(todo)} 已到 {dt}（{el/60:.1f} 分钟，缓存 {len(frames)} 天帧）",
                  flush=True)
    print(f"✓ 深度回填完成：{ok} 天落库，{skip} 天跳过（{min(todo)}~{max(todo)}）")


if __name__ == "__main__":
    main()
