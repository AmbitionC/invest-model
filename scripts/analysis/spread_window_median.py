"""上证−创业板点差中位数的统计窗口敏感性 + 生产库数据对账（用户质疑命题·只读不落库）。

背景：E20 首跑用生产库近 5 年数据实测中位数 900，与博主主张的"均衡中位数 500~550"
相差近一倍。用户质疑：是取数口径不对，还是我们的数据有问题？本脚本双管齐下：
  1) 数据对账：tushare index_daily（独立于生产库的源头）全历史拉取 000001.SH 与
     399006.SZ，与生产库 index_daily 重叠区间逐日比对收盘价，量化差异；
  2) 窗口敏感性：对 D = 上证收盘 − 创业板收盘，按多个统计窗口（创业板 2010 上市以来 /
     近10年 / 近5年 / 近3年 / 逐个自然年）分别计算中位数，检验"500~550"在哪个窗口成立。

输出 results/spread_window_median.txt（人读摘要）+ results/spread_full_history.csv
（trade_date,sse,chinext,D 全历史，供复现），由 workflow 提交回 master。
  python scripts/analysis/spread_window_median.py [--out-dir results]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.sources.tushare_client import TushareClient  # noqa: E402

SSE = "000001.SH"
CHINEXT = "399006.SZ"
START = "20100601"  # 创业板指基日 2010-05-31


def _fetch_full(client: TushareClient, code: str) -> pd.DataFrame:
    """tushare index_daily 单次上限 8000 行，按 5 年分段拉全历史。"""
    frames = []
    for y0 in range(2010, 2031, 5):
        seg = client.get_index_daily(code, f"{y0}0101", f"{y0 + 4}1231")
        if seg is not None and not seg.empty:
            frames.append(seg[["trade_date", "close"]])
    df = pd.concat(frames, ignore_index=True).drop_duplicates("trade_date")
    df = df[df["trade_date"] >= START].sort_values("trade_date").reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna(subset=["close"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="results")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    client = TushareClient()
    sse = _fetch_full(client, SSE).rename(columns={"close": "sse"})
    chi = _fetch_full(client, CHINEXT).rename(columns={"close": "chinext"})
    full = sse.merge(chi, on="trade_date", how="inner").sort_values("trade_date")
    full["D"] = full["sse"] - full["chinext"]
    full.to_csv(out_dir / "spread_full_history.csv", index=False)

    lines: list[str] = []
    lines.append(f"tushare 全历史：{full['trade_date'].min()} ~ {full['trade_date'].max()}，{len(full)} 个交易日")

    # ---- 1) 与生产库对账 ----
    try:
        repo = BaseRepository(make_engine())
        db = repo.read_sql(
            "SELECT code, trade_date, close FROM index_daily WHERE code IN (:a,:b) ORDER BY trade_date",
            {"a": SSE, "b": CHINEXT})
        db["close"] = pd.to_numeric(db["close"], errors="coerce")
        piv = db.pivot_table(index="trade_date", columns="code", values="close").reset_index()
        cmp_ = full.merge(piv, on="trade_date", how="inner", suffixes=("", "_db"))
        lines.append("")
        lines.append("== 生产库 index_daily 对账（重叠区间逐日）==")
        lines.append(f"重叠交易日：{len(cmp_)}（库覆盖 {piv['trade_date'].min()} ~ {piv['trade_date'].max()}）")
        for col, ts in [("sse", SSE), ("chinext", CHINEXT)]:
            d = (cmp_[col] - cmp_[ts]).abs()
            lines.append(f"{ts}: 最大绝对差 {d.max():.3f} 点 ｜ 差>0.01 点的天数 {(d > 0.01).sum()}")
    except Exception as exc:  # 库不可达时不阻塞窗口分析
        lines.append(f"[对账跳过] 生产库读取失败：{exc}")

    # ---- 2) 中位数的窗口敏感性 ----
    last = full["trade_date"].max()
    lines.append("")
    lines.append("== D = 上证 − 创业板 中位数（按统计窗口）==")

    def med(df: pd.DataFrame) -> str:
        return f"{df['D'].median():.0f}（n={len(df)}，D范围 {df['D'].min():.0f} ~ {df['D'].max():.0f}）"

    windows = [
        ("2010上市以来全历史", full),
        ("近15年", full[full["trade_date"] >= "20110701"]),
        ("近10年", full[full["trade_date"] >= "20160701"]),
        ("近5年", full[full["trade_date"] >= "20210701"]),
        ("近3年", full[full["trade_date"] >= "20230701"]),
    ]
    for name, df in windows:
        lines.append(f"{name}: {med(df)}")

    lines.append("")
    lines.append("== 逐自然年中位数 ==")
    full["year"] = full["trade_date"].str[:4]
    for y, g in full.groupby("year"):
        lines.append(f"{y}: {g['D'].median():.0f}")

    inband = full[(full["D"] >= 400) & (full["D"] <= 660)]
    lines.append("")
    lines.append(f"全历史处于 500~550±20%（400~660）区间的交易日占比：{len(inband) / len(full):.1%}")

    text = "\n".join(lines)
    (out_dir / "spread_window_median.txt").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
