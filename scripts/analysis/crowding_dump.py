"""交易拥挤度/量能/两融杠杆 10 年日频取数（tushare 流式·只写 CSV 不落库）。

源自陈老师（重远投资观）顶部识别方法梳理（P29/E24 v2 候选信号）：
  - 交易拥挤度：双创（创业板 300/301 + 科创 688/689）成交额占全市场比
  - 量能：全市场成交总额（天量=顶部特征、地量=底部特征候选）
  - 杠杆拥挤度：两融融资余额 / 全市场流通市值（2015 顶经典指标）
  - 换手：全市场成交额 / 流通市值

每交易日 3 次调用（daily / margin / daily_basic），断点可续跑（已有日期跳过）。
输出 results/crowding_daily.csv：
  trade_date, total_amt_yi(全市场成交额·亿元), dual_amt_yi(双创成交额·亿元),
  dual_ratio(双创占比), rzye_yi(融资余额·亿元), circ_mv_yi(流通市值·亿元),
  margin_ratio(融资余额/流通市值), turnover(成交额/流通市值)

用法：python scripts/analysis/crowding_dump.py --start 20150101 [--out results/crowding_daily.csv]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.sources.tushare_client import TushareClient  # noqa: E402

DUAL_PREFIX = ("300", "301", "688", "689")  # 创业板 + 科创板


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20150101")
    ap.add_argument("--end", default=None)
    ap.add_argument("--out", default="results/crowding_daily.csv")
    args = ap.parse_args()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    cli = TushareClient()
    end = args.end or pd.Timestamp.utcnow().strftime("%Y%m%d")
    cal = cli.get_trade_calendar(args.start, end)
    days = sorted(cal[cal["is_open"] == 1]["cal_date"].astype(str))

    have: set[str] = set()
    rows: list[dict] = []
    if out.exists():
        old = pd.read_csv(out, dtype={"trade_date": str})
        have = set(old["trade_date"])
        rows = old.to_dict("records")
    todo = [d for d in days if d not in have]
    print(f"目标 {len(days)} 交易日，待取 {len(todo)} 天（已有 {len(have)} 天跳过）")

    t0 = time.time()
    for i, d in enumerate(todo, 1):
        try:
            daily = cli.get_daily_bulk(d)
            if daily is None or daily.empty:
                continue
            amt = pd.to_numeric(daily["amount"], errors="coerce")  # 千元
            total_amt = float(amt.sum()) / 1e5                     # → 亿元
            dual = daily["code"].astype(str).str.startswith(DUAL_PREFIX)
            dual_amt = float(amt[dual].sum()) / 1e5

            basic = cli.get_daily_basic(d)
            circ_mv = float(pd.to_numeric(basic["circ_mv"], errors="coerce").sum()) / 1e4 \
                if basic is not None and not basic.empty else float("nan")  # 万元→亿元

            mg = cli.get_margin(d)
            rzye = float(pd.to_numeric(mg["rzye"], errors="coerce").sum()) / 1e8 \
                if mg is not None and not mg.empty else float("nan")        # 元→亿元

            rows.append({
                "trade_date": d,
                "total_amt_yi": round(total_amt, 1),
                "dual_amt_yi": round(dual_amt, 1),
                "dual_ratio": round(dual_amt / total_amt, 4) if total_amt else float("nan"),
                "rzye_yi": round(rzye, 1),
                "circ_mv_yi": round(circ_mv, 1),
                "margin_ratio": round(rzye / circ_mv, 5) if circ_mv and circ_mv == circ_mv else float("nan"),
                "turnover": round(total_amt / circ_mv, 5) if circ_mv and circ_mv == circ_mv else float("nan"),
            })
        except Exception as e:  # noqa: BLE001 — 单日失败跳过不阻断
            print(f"  跳过 {d}: {repr(e)[:80]}")
        if i % 100 == 0 or i == len(todo):
            pd.DataFrame(rows).sort_values("trade_date").to_csv(out, index=False)
            print(f"  进度 {i}/{len(todo)} 已到 {d}（{(time.time()-t0)/60:.1f} 分钟）", flush=True)

    pd.DataFrame(rows).sort_values("trade_date").to_csv(out, index=False)
    print(f"✓ 完成：{len(rows)} 行 → {out}")


if __name__ == "__main__":
    main()
