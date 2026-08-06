# -*- coding: utf-8 -*-
"""乖离率的**历史极值排名**口径 —— **两个尾部一起**（owner 2026-08-05：
「乖离率历史排名极值这个验证完了吗，这个极值包括极大值和极小值」）。

## 为什么要有这个脚本

博主的原口径是**全历史极值排名**（P39/E37 命题原文：「用全历史极值排名（不是滚动分位）」，
他数的是「创业板 27.53% 近十年排名第五，前四分别是…」）。而两次 E 验证都没有完整覆盖排名口径：

| 尾部 | 已测的口径 | 排名口径覆盖情况 |
|---|---|---|
| **极大值（高尾）** | E37①＝前 5% **分位**；E37②＝**超过此前历史最大值**（＝因果排名第 1） | ❌ **「进前 K」（K=3/5/10）从没测过** |
| **极小值（低尾）** | E56＝前 2%/5%/10% **分位** | ✅ 2026-08-05 已补测因果排名 K=3/5/10 |

本脚本把两头都按排名补齐，口径完全对称。

## 两种排名口径必须分开（本次最有价值的区分）

  · **因果排名**（tradable）：当日读数是否为**截至当日**见过的最极端 K 个之一。
    这才是能写进规则的版本，也是 E37②「超过此前历史最大值」的推广（那是 K=1）。
  · **事后 episode 谱**（descriptive）：全历史按不重叠 60td 归并后排名。
    用来回答「现在排第几」，**但不可交易**——"谁是第 1 名"要看完全部历史才知道。

## 治理边界

本脚本产出**探索读数，不是判据**。E37 与 E56 都已按各自写死的判据 FAIL；
换排名口径要不要改判，**必须实测，不能靠推理**——

🔴 **留痕：我在低尾这条上推错过一次。** 当时写「排名前五是分位 2% 的真子集（5/500=1%<2%），
而 X=2% 共现已实测为 0，故前五必然也是 0」。**这条对逐日排名成立，对 episode 排名不成立**
——episode 排名把同一轮连续深跌折叠成一个代表点，第 4/5 名的代表日可以落在 2~5% 分位带
（实测创业板 20181018 因果分位 3.44%、20250407 为 2.43%，两天都过 B2 价格闸）。
**结论没变，但那条理由作废，已换成实测。**

只读 results/*.csv，不落库、不联网。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))
from long_window_backtest import LEGS  # noqa: E402
from e56_bias_low_tail import (  # noqa: E402
    bias_and_causal_pct, episodes, first_tradable, prep_all, with_warm,
)

GAP = 60          # 不重叠 episode 的间隔（交易日），与 E56 判据③同口径
TOPN = 8          # 列出前 8，owner 关心的是前 5


def extreme_episodes(b: np.ndarray, dates: np.ndarray, i0: int, side: str = "low",
                     gap: int = GAP) -> list[dict]:
    """把某一尾按不重叠 episode 归并：每段取其**最极端点**作为该 episode 的代表。

    side="low" 取最低点、"high" 取最高点。做法：按极端程度从大到小扫，
    若该日与已选中的任一代表相距 ≤gap 则并入既有 episode，否则新开一个。
    等价于「贪心取全局最极端点、屏蔽其前后 gap 天、再取次极端」。
    """
    key = b if side == "low" else -b
    order = np.argsort(key[i0:], kind="stable") + i0
    reps: list[int] = []
    for i in order:
        if b[i] != b[i]:
            continue
        if all(abs(i - j) > gap for j in reps):
            reps.append(int(i))
        if len(reps) >= 40:
            break
    return [{"i": i, "date": str(dates[i]), "bias": float(b[i])} for i in reps]


def low_episodes(b, dates, i0, gap: int = GAP):
    """向后兼容的别名（broad_export_web 引用）。"""
    return extreme_episodes(b, dates, i0, "low", gap)


def causal_rank_hits(b: np.ndarray, K: int, side: str, warm: int = 500) -> np.ndarray:
    """因果排名命中：当日读数是否为**截至当日**见过的最极端 K 个之一。

    这是 E37 判据②「超过此前历史最大值」的推广——那条相当于 K=1。
    """
    hist: list[float] = []
    hit = np.zeros(len(b), bool)
    for i in range(len(b)):
        if b[i] != b[i]:
            continue
        if len(hist) >= warm:
            arr = np.asarray(hist)
            kth = (np.partition(arr, K - 1)[K - 1] if side == "low"
                   else np.partition(arr, -K)[-K])
            if (b[i] <= kth) if side == "low" else (b[i] >= kth):
                hit[i] = True
        hist.append(b[i])
    return hit


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    ap.add_argument("--w", type=int, default=60, help="均线窗口（博主口径 60）")
    a = ap.parse_args()
    root = Path(a.data)
    fear = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fmap = dict(zip(fear.trade_date, pd.to_numeric(fear.score)))

    # 预备：四腿的乖离率、价格、闸位
    D = {}
    for nm, f, col, trf, _fx, mode in LEGS:
        df0, ret = prep_all(root, f, col, trf)
        b, pct = bias_and_causal_pct(df0.c, a.w)
        df = with_warm(df0, 500)
        # 🔴 2026-08-05 修正：此前这里用 `first_tradable`（＝expanding 中位线锚的 WARM=500
        # 预热日），但**那是锚的预热，不是乖离率的预热**。乖离率只需要 60 个交易日。
        # 后果：①每条 anchor 腿的极值谱都被砍掉了前 ~499 个可算日，沪深300 的谱首位
        # （2007-01-29 +35.74%）其实不是全历史最高——被砍掉的段里 2007-01-22 有 +37.64%；
        # ②科创50 是 ladder 腿、first_tradable 返回数据首日 ⟹ **腿间口径还不一致**。
        # 现统一为「bias 首个可算日」，全指数同一口径。
        i0 = int(np.argmax(~np.isnan(b)))
        D[nm] = dict(df=df, ret=ret, mode=mode, b=b, pct=pct, i0=i0)

    print("=" * 112)
    print(f"乖离率历史极值排名 —— **两个尾部**（相对 MA{a.w}）")
    print(f"episode 按不重叠 {GAP} 个交易日归并，每段取该段最极端点为代表")
    print("=" * 112)

    SIDES = (("low", "极小值（低尾·跌得太深）", "P65 / E56（2026-08-05）"),
             ("high", "极大值（高尾·涨得太猛）", "P39 / E37（2026-08-02）"))

    summary = {}
    for side, label, tested in SIDES:
        print(f"\n{'█' * 112}")
        print(f"■ {label}　既有验证：{tested}")
        print(f"{'█' * 112}")

        rows5 = []
        for nm in D:
            z = D[nm]
            b, df, i0 = z["b"], z["df"], z["i0"]
            d, c, r12 = df.trade_date.values, df.c.values, df.r1250.values
            eps = extreme_episodes(b, d, i0, side)
            cur = float(b[-1])
            valid = b[i0:][~np.isnan(b[i0:])]
            rank_day = (int((valid < cur).sum()) + 1 if side == "low"
                        else int((valid > cur).sum()) + 1)
            rank_ep = (sum(1 for e in eps if e["bias"] < cur) + 1 if side == "low"
                       else sum(1 for e in eps if e["bias"] > cur) + 1)
            print(f"\n【{nm}】{d[i0]}~{d[-1]}　当前 {cur:+.2%}　"
                  f"逐日排名 第 {rank_day}/{len(valid)}　episode 谱 第 {rank_ep}")
            print(f"  {'排名':>4s}{'日期':>10s}{'乖离率':>9s}{'恐慌':>6s}"
                  f"{'后20日':>9s}{'后60日':>9s}{'后250日':>10s}"
                  + ("{:>12s}".format("60日内回MA60下") if side == "high" else
                     "{:>12s}".format("过B2价格闸")))
            for k, e in enumerate(eps[:8], 1):
                i = e["i"]
                fv = fmap.get(str(d[i]), np.nan)
                fs = f"{fv:.0f}" if fv == fv else "—"
                fwd = []
                for h in (20, 60, 250):
                    fwd.append(f"{c[i+h]/c[i]-1:>+9.1%}" if i + h < len(c) else f"{'—':>9s}")
                if side == "high":
                    seg = b[i + 1:i + 61]
                    seg = seg[~np.isnan(seg)]
                    extra = ("✅是" if (len(seg) and (seg < 0).any())
                             else ("❌否" if len(seg) else "—"))
                else:
                    extra = "✅是" if (r12[i] == r12[i] and c[i] < r12[i]) else "❌否"
                print(f"  {k:>4d}{e['date']:>10s}{e['bias']:>9.2%}{fs:>6s}"
                      f"{fwd[0]}{fwd[1]}{fwd[2]:>10s}{extra:>12s}")
                if k <= 5:
                    rows5.append(dict(
                        leg=nm, rank=k, date=e["date"], bias=e["bias"], fear=fv,
                        f20=(c[i+20]/c[i]-1) if i+20 < len(c) else np.nan,
                        f60=(c[i+60]/c[i]-1) if i+60 < len(c) else np.nan,
                        f250=(c[i+250]/c[i]-1) if i+250 < len(c) else np.nan,
                        extra=(extra == "✅是")))
            summary.setdefault(side, {})[nm] = dict(rank_day=rank_day, n=len(valid),
                                                    rank_ep=rank_ep, cur=cur)

        R = pd.DataFrame(rows5)
        print(f"\n  ── 事后 episode 谱**前五**汇总（四腿合计 n={len(R)}·探索，非判据）──")
        for h, cn in ((20, "f20"), (60, "f60"), (250, "f250")):
            v = R[cn].dropna()
            print(f"    后 {h:>3d} 日：均值 {v.mean():>+7.1%}　中位 {v.median():>+7.1%}"
                  f"　为正 {int((v > 0).sum())}/{len(v)}"
                  f"　最差 {v.min():>+7.1%}　最好 {v.max():>+7.1%}")
        tag = "60 日内回到 MA60 下方" if side == "high" else "同时过 B2 价格闸"
        print(f"    {tag}：{int(R.extra.sum())}/{len(R)}")

        # ── 因果排名（可交易口径）K=1/3/5/10 ──────────────────────────
        print(f"\n  ── 因果排名（可交易口径）：截至当日见过的最{'低' if side=='low' else '高'} K 个 ──")
        print(f"    {'腿':>7s}{'K':>4s}{'触发日':>8s}{'独立事件':>10s}{'恐慌≥75':>9s}"
              f"{'后20日均值':>12s}{'为正':>9s}"
              + ("{:>14s}".format("60日内回MA60下") if side == "high"
                 else "{:>12s}".format("过B2价格闸")))
        for nm in D:
            z = D[nm]
            b, df, i0 = z["b"], z["df"], z["i0"]
            d, c, r12 = df.trade_date.values, df.c.values, df.r1250.values
            for K in (1, 3, 5, 10):
                hit = causal_rank_hits(b, K, side)
                sel = [i for i in np.where(hit)[0] if i >= i0]
                if not sel:
                    print(f"    {nm:>7s}{K:>4d}{0:>8d}{'—':>10s}{'—':>9s}{'—':>12s}{'—':>9s}{'—':>12s}")
                    continue
                hot = sum(1 for i in sel if fmap.get(str(d[i]), np.nan) >= 75)
                f20 = [c[i+20]/c[i]-1 for i in sel if i+20 < len(c)]
                if side == "high":
                    ok = 0
                    for i in sel:
                        seg = b[i+1:i+61]; seg = seg[~np.isnan(seg)]
                        ok += bool(len(seg) and (seg < 0).any())
                else:
                    ok = sum(1 for i in sel if r12[i] == r12[i] and c[i] < r12[i])
                m = f"{np.mean(f20):+.1%}" if f20 else "—"
                pos = f"{sum(1 for x in f20 if x > 0)}/{len(f20)}" if f20 else "—"
                print(f"    {nm:>7s}{K:>4d}{len(sel):>8d}{episodes(sel):>10d}{hot:>9d}"
                      f"{m:>12s}{pos:>9s}{f'{ok}/{len(sel)}':>12s}")

    # ── 汇总裁决表 ────────────────────────────────────────────────
    print("\n" + "=" * 112)
    print("汇总：两个尾部各自的验证状态（这张表回答「验证完了吗」）")
    print("=" * 112)
    print(f"  {'尾部':>18s}{'既有 E 验证':>22s}{'裁决':>10s}{'排名口径已覆盖?':>18s}")
    print(f"  {'极大值（高尾）':>18s}{'P39/E37 2026-08-02':>22s}{'FAIL':>10s}"
          f"{'K=1 已测·K=3/5/10 本次补':>18s}")
    print(f"  {'极小值（低尾）':>18s}{'P65/E56 2026-08-05':>22s}{'FAIL':>10s}"
          f"{'本次已补 K=3/5/10':>18s}")
    print("\n  当前四腿在两个尾部的位置：")
    print(f"  {'腿':>7s}{'当前乖离率':>12s}{'低尾逐日排名':>14s}{'低尾谱':>8s}"
          f"{'高尾逐日排名':>14s}{'高尾谱':>8s}")
    for nm in D:
        lo, hi = summary["low"][nm], summary["high"][nm]
        lo_d = "第{}/{}".format(lo["rank_day"], lo["n"])
        hi_d = "第{}/{}".format(hi["rank_day"], hi["n"])
        print("  {:>7s}{:>+12.2%}{:>14s}{:>8s}{:>14s}{:>8s}".format(
            nm, lo["cur"], lo_d, "第{}".format(lo["rank_ep"]),
            hi_d, "第{}".format(hi["rank_ep"])))

    print("\n" + "=" * 112)
    print("读法（治理边界）")
    print("=" * 112)
    print("  1. 上面所有前瞻收益都是**探索，不是判据**。E37 与 E56 已各自按写死的判据 FAIL，")
    print("     本表不改判——它补的是「排名口径覆盖不全」这个缺口，不是重新开一次验证。")
    print("  2. **事后 episode 谱不可交易**：当天你不知道自己排第几。可交易的是因果排名那张表。")
    print("  3. 若某一尾的因果排名读数看着好，那证明的是**另一个命题**（单独作短线信号），")
    print("     需要自己的 P/E，且按双引擎判定必须整条短线腿一起上（进场+退出+仓位）。")


if __name__ == "__main__":
    main()
