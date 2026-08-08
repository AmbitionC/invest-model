# -*- coding: utf-8 -*-
"""宽基四腿·操作手册图（买卖点标注 + 预期收益 + 当前状态）

owner 2026-08-04：「标注出明确的买卖点和按这个操作执行后的预期收益」。

**回测引擎直接复用 `long_window_backtest.run`**——图和表共用同一个 run()，
不另起一套（此前两套引擎给出过 0.66pp 的差，教训见 CLAUDE.md）。

产出：
  results/broad_playbook.png   四腿价格图（买卖点逐笔标注）+ 净值/仓位 + 预期收益条
  results/broad_trades.csv     全部成交流水（日期/方向/触发原因/价格/金额/占比）
  stdout                       预期收益表 + 当前闸位状态（今天该干什么）
只读 CSV，不落库、不联网。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib import font_manager  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

from long_window_backtest import (  # noqa: E402
    CASH,
    FONT,
    LEGS,
    first_tradable,
    prep,
    run,
)

COL = {"沪深300": "#c0392b", "创业板": "#2980b9", "科创50": "#8e44ad", "红利": "#1e8449"}
ETF = {"沪深300": "510300 沪深300ETF", "创业板": "159915 创业板ETF",
       "科创50": "588000 科创50ETF", "红利": "515080 红利ETF"}


def gates(df: pd.DataFrame, nm: str, mode: str) -> dict:
    """今天各腿的闸位读数——图上要画线，表里要说"现在该干什么"。"""
    last = float(df.c.iloc[-1])
    exp = float(df.exp.iloc[-1]) if df.exp.notna().any() else float("nan")
    peak = float(df.peak.iloc[-1])
    from invest_model.broad_gates import BUY_MUL, SELL_MUL   # XV-5：闸位唯一真源
    bm, sm = BUY_MUL[nm], SELL_MUL[nm]
    return {"date": str(df.trade_date.iloc[-1]), "last": last, "exp": exp, "peak": peak,
            "buy_line": exp * bm, "sell_line": exp * sm, "bm": bm, "sm": sm,
            "dd_peak": last / peak - 1, "mode": mode}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=".")
    ap.add_argument("--out-dir", default="results")
    a = ap.parse_args()
    root, outd = Path(a.data), Path(a.out_dir)
    outd.mkdir(parents=True, exist_ok=True)
    font_manager.fontManager.addfont(FONT)
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei"]
    plt.rcParams["axes.unicode_minus"] = False

    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))

    data = {nm: prep(root, f, col, trf) for nm, f, col, trf, _, _ in LEGS}
    starts = {nm: first_tradable(data[nm][0], mode, fx) for nm, _, _, _, fx, mode in LEGS}
    res, gt, all_tr = {}, {}, []
    for nm, f, col, trf, _fx, mode in LEGS:
        df, ret = data[nm]
        r = run(df, ret, fmap, nm, starts[nm], str(df.trade_date.iloc[-1]), mode)
        res[nm] = r
        gt[nm] = gates(df, nm, mode)
        for t in r["trades"]:
            all_tr.append({"腿": nm, **t})

    tr = pd.DataFrame(all_tr)
    tr.to_csv(outd / "broad_trades.csv", index=False)

    # ── 预期收益表 ───────────────────────────────────────────
    print("=" * 100)
    print("一、按这套规则执行的历史表现（每腿 100 元起，闲置现金 2%/年，信号次日收盘成交）")
    print("=" * 100)
    print(f"{'腿':8s}{'区间':>20s}{'年数':>6s}{'策略年化':>9s}{'买入持有':>9s}{'超额':>8s}"
          f"{'夏普':>7s}{'日频回撤':>9s}{'均仓':>6s}{'买':>5s}{'卖':>5s}{'年均动手':>9s}")
    for nm in res:
        r = res[nm]
        n = r["nb"] + r["ns"]
        print(f"{nm:8s}{starts[nm][:6] + '~' + gt[nm]['date'][:6]:>20s}{r['yrs']:>6.1f}"
              f"{r['ann']:>9.2%}{r['bh']:>9.2%}{(r['ann'] - r['bh']) * 100:>+8.2f}"
              f"{r['sharpe']:>7.2f}{r['mdd']:>9.1%}{r['posavg']:>6.0%}"
              f"{r['nb']:>5d}{r['ns']:>5d}{n / r['yrs']:>9.1f}")

    print("\n  ⚠ 三条口径提醒（红队 2026-08-04）：")
    print("    · 2015 前无恐慌数据 ⟹ 恐慌抢买腿在早期自然不触发，长窗＝纯「锚买+月卖」")
    print("    · 各腿区间不同，**不可横向比年化**；19.5 年「四腿合计」是不可实现组合，已退役")
    print("    · 十年滚动窗口的不重叠独立样本仅 1.4~2.0 个，胜率数字不可当独立证据")

    # 逐笔成交摘要
    print("\n" + "=" * 100)
    print("二、买卖点：全部成交流水摘要（明细见 results/broad_trades.csv）")
    print("=" * 100)
    for nm in res:
        t = tr[tr["腿"] == nm]
        if t.empty:
            continue
        buy, sell = t[t.side == "买"], t[t.side == "卖"]
        kinds = t.why.str.extract(r"^([^(（]+)")[0].value_counts()
        print(f"\n  {nm}（{ETF[nm]}）：买 {len(buy)} 笔 / 卖 {len(sell)} 笔")
        print(f"    触发类型：" + "、".join(f"{k} {v} 笔" for k, v in kinds.items()))
        if len(buy):
            print(f"    首笔买 {buy.date.iloc[0]}  最近一笔买 {buy.date.iloc[-1]}"
                  f"  单笔买入金额中位 {buy.amount.median():.2f} 元")
        if len(sell):
            print(f"    首笔卖 {sell.date.iloc[0]}  最近一笔卖 {sell.date.iloc[-1]}")
        big = buy.nlargest(3, "amount")
        for _, x in big.iterrows():
            print(f"    · 最大买入 {x.date}  {x.amount:6.2f} 元  {x.why}")

    # ── 当前状态：今天该干什么 ────────────────────────────────
    f_now = None
    for d in sorted(fmap)[::-1]:
        f_now = fmap[d]
        break
    print("\n" + "=" * 100)
    print(f"三、当前闸位（数据截至各腿最后交易日；恐慌 EOD={f_now:.0f}）——今天该干什么")
    print("=" * 100)
    print(f"  {'腿':8s}{'收盘':>9s}{'买入线':>9s}{'卖出线':>9s}{'距买入线':>9s}{'距卖出线':>9s}  判定")
    for nm in res:
        g = gt[nm]
        if g["mode"] == "ladder":
            state = (f"距峰 {g['dd_peak']:+.0%}｜阶梯腿：距峰 ≤−50% 才开第一档"
                     f"（还需再跌 {(1 - 0.5) / (1 + g['dd_peak']) - 1:+.0%}）")
            print(f"  {nm:8s}{g['last']:>9.0f}{'—':>9s}{g['sell_line']:>9.0f}"
                  f"{'—':>9s}{g['last'] / g['sell_line'] - 1:>+9.0%}  {state}")
            continue
        db, ds = g["last"] / g["buy_line"] - 1, g["last"] / g["sell_line"] - 1
        if db < 0:
            state = "🟢 买入窗开：周五收盘买当前现金 20%"
        elif ds > 0:
            state = "🔴 卖出区：月末收盘卖持仓 5%"
        else:
            state = "⚪ 持有区：不买不卖（P52「不动也是决策」）"
        print(f"  {nm:8s}{g['last']:>9.0f}{g['buy_line']:>9.0f}{g['sell_line']:>9.0f}"
              f"{db:>+9.0%}{ds:>+9.0%}  {state}")
    print(f"\n  恐慌抢买（四腿共用）：EOD 恐慌 {f_now:.0f} < 75 ⟹ 未触发。"
          f"触发时买当前现金 50%（还需 收盘 < 滚动5年中位线）")

    _chart(res, gt, tr, starts, outd)


def _chart(res, gt, tr, starts, outd: Path) -> None:
    fig = plt.figure(figsize=(17, 15.5))
    gs = fig.add_gridspec(3, 2, height_ratios=[1, 1, 1.05], hspace=0.30, wspace=0.16, top=0.935)
    fig.suptitle("宽基四腿·操作手册：买卖点与执行后的表现（2026-08-04 红队修正口径）",
                 fontsize=18, weight="bold", y=0.972)

    for k, nm in enumerate(res):
        ax = fig.add_subplot(gs[k // 2, k % 2])
        r, g = res[nm], gt[nm]
        dt = pd.to_datetime(r["dates"])
        # 价格用信号口径（红利腿信号是价格指数，与买卖闸同源）
        px = pd.Series(index=dt, dtype=float)
        t = tr[tr["腿"] == nm]
        ax.plot(dt, _price_series(nm, r, starts), lw=1.1, color="#7f8c8d", label="指数收盘")
        b, s = t[t.side == "买"], t[t.side == "卖"]
        ax.scatter(pd.to_datetime(b.date), b.price, s=np.clip(b.amount * 6, 12, 150),
                   marker="^", color="#1e8449", alpha=.85, zorder=5, label=f"买 {len(b)} 笔")
        ax.scatter(pd.to_datetime(s.date), s.price, s=22, marker="v",
                   color="#c0392b", alpha=.7, zorder=5, label=f"卖 {len(s)} 笔")
        if g["mode"] == "ladder":
            # 阶梯腿**不走锚买**（生产代码是 if mode=="ladder": ... elif <锚买>），
            # 画锚买线会误导。它的买点是距全历史峰的四档回撤。
            for th, fr in zip((0.50, 0.55, 0.60, 0.65), (0.30, 0.35, 0.40, 0.50)):
                ax.axhline(g["peak"] * (1 - th), color="#1e8449", ls=":", lw=1.0)
            ax.text(dt[0], g["peak"] * 0.50, f" 阶梯档 距峰−50%={g['peak'] * 0.5:.0f}"
                                             f" …−65%={g['peak'] * 0.35:.0f}（投现金 30/35/40/50%）",
                    color="#1e8449", fontsize=8.5, va="bottom")
        elif g["exp"] == g["exp"]:
            ax.axhline(g["buy_line"], color="#1e8449", ls="--", lw=1.3)
            ax.text(dt[0], g["buy_line"], f" 买入线 {g['buy_line']:.0f}（今）",
                    color="#1e8449", fontsize=9, va="bottom")
        if g["exp"] == g["exp"]:
            ax.axhline(g["sell_line"], color="#c0392b", ls="--", lw=1.3)
            ax.text(dt[0], g["sell_line"], f" 卖出线 {g['sell_line']:.0f}（今）",
                    color="#c0392b", fontsize=9, va="bottom")
        ax.set_yscale("log")
        ax.set_title(f"{nm}（{ETF[nm]}）  策略 {r['ann']:+.2%} vs 买持 {r['bh']:+.2%}"
                     f"  超额 {(r['ann'] - r['bh']) * 100:+.2f}pp",
                     fontsize=12.5, weight="bold", color=COL[nm])
        ax.legend(fontsize=9, loc="upper left")
        ax.grid(alpha=.25)
        ax.tick_params(labelsize=9)

    # 净值 + 仓位
    ax = fig.add_subplot(gs[2, 0])
    for nm in res:
        r = res[nm]
        ax.plot(pd.to_datetime(r["dates"]), r["curve"] / 100.0, lw=1.6, color=COL[nm],
                label=f"{nm} {r['curve'][-1] / 100:.2f}×")
    ax.set_yscale("log")
    ax.set_title("⑤ 每腿 100 元执行到今天变成多少倍（对数轴）", fontsize=12.5, weight="bold")
    ax.legend(fontsize=9.5, loc="upper left")
    ax.grid(alpha=.25)

    ax = fig.add_subplot(gs[2, 1])
    names = list(res)
    x = np.arange(len(names))
    st = [res[n]["ann"] * 100 for n in names]
    bh = [res[n]["bh"] * 100 for n in names]
    ax.bar(x - .2, st, .4, color=[COL[n] for n in names], label="按规则执行")
    ax.bar(x + .2, bh, .4, color="#bdc3c7", label="买入持有")
    for i, (p, q) in enumerate(zip(st, bh)):
        ax.text(i - .2, p + .3, f"{p:.1f}", ha="center", fontsize=10, weight="bold")
        ax.text(i + .2, q + .3, f"{q:.1f}", ha="center", fontsize=9.5, color="#555")
        ax.annotate(f"{p - q:+.1f}pp", xy=(i, max(p, q) + 1.6), ha="center",
                    fontsize=10, color=("#1e8449" if p > q else "#c0392b"), weight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{n}\n{res[n]['yrs']:.1f}年" for n in names], fontsize=10)
    ax.set_ylabel("年化（%）")
    ax.set_ylim(0, max(st + bh) * 1.25)
    ax.legend(fontsize=9.5)
    ax.set_title("⑥ 预期收益：区间不同不可横向比，只看每组内部的差", fontsize=12.5, weight="bold")

    out = outd / "broad_playbook.png"
    fig.savefig(out, dpi=115, bbox_inches="tight")
    print(f"\nsaved {out}")
    print(f"saved {outd / 'broad_trades.csv'}  （{len(tr)} 笔）")


def _price_series(nm, r, starts):
    """图上画的价格＝信号口径的指数收盘（与买卖闸同源）。"""
    from long_window_backtest import LEGS as _L
    root = Path(".")
    for n2, f, col, trf, _fx, _m in _L:
        if n2 == nm:
            d = pd.read_csv(root / f, dtype={"trade_date": str}).sort_values("trade_date")
            d = d[(d.trade_date >= starts[nm]) & (d.trade_date <= str(r["dates"][-1]))]
            return pd.to_numeric(d[col]).to_numpy()
    raise KeyError(nm)


if __name__ == "__main__":
    main()
