# -*- coding: utf-8 -*-
"""乖离率均值回归研究的**数据包**（P70/E60 的第 1 步）。

owner 2026-08-06 澄清了我此前的误读：他说的不是「乖离率破极值后买入、等一个方向性卖出
信号」（那是 E59 测的东西，是价格方向的赌注），而是**乖离率这个量本身在数学意义上就会
均值回归**——`bias = P/MA − 1` 是围绕 0 震荡的量，极值必然收敛。

**本脚本只造数、不下结论。** 之所以单独成一步，是因为下一步要让多个独立通道交叉验证
数据质量，它们必须对着同一份、可校验的产物看。

## 核心：Δbias 的精确分解（恒等式，不是近似）

    bias_t = P_t / M_t − 1        ⟹    1 + bias_t = P_t / M_t

    ln[(1 + bias_{t+H}) / (1 + bias_t)] = ln(P_{t+H}/P_t) − ln(M_{t+H}/M_t)
                                          ╰─ 价格腿 ─╯     ╰─ 均线腿 ─╯

⟹ 乖离率从 −30% 回到 0，可以是价格涨 42.9%（均线不动），也可以是**均线自己跌下来
30%（价格一动不动）**。两者对账户的意义完全相反。**「指标必然回归」是真的，
但它能不能变成钱，全看这个分解怎么劈。** 本数据包把两条腿逐日物化，供下一步检验。

产物：`results/bias_meanrev/<指数>.csv` + `manifest.json`（行数/日期范围/空值/md5/抽样点），
manifest 是交叉验证通道的对账依据。只读 results/*.csv，不落库、不联网。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1]))
from e57_bias_top3_leg import UNIVERSE, load  # noqa: E402

MAS = (20, 60, 120)                  # 乖离率的均线窗口（主口径 60，另两档作对照臂）
HORIZONS = (5, 10, 20, 60, 120)      # 前瞻期（交易日）
MA_MAIN = 60


def build(root: Path, fear: dict[str, float], nm: str, f: str, col: str) -> pd.DataFrame:
    d = load(root, f, col).reset_index(drop=True)
    out = pd.DataFrame({"trade_date": d.trade_date.astype(str), "close": d.c.astype(float)})

    for w in MAS:
        m = out.close.rolling(w).mean()
        out[f"ma{w}"] = m
        out[f"bias{w}"] = out.close / m - 1.0

    out["fear"] = out.trade_date.map(fear)

    # 前瞻价格收益 + 前瞻乖离率水平（主口径 MA60）
    for h in HORIZONS:
        out[f"fwd_ret{h}"] = out.close.shift(-h) / out.close - 1.0
        out[f"fwd_bias{h}"] = out[f"bias{MA_MAIN}"].shift(-h)

    # 🔴 Δbias 的精确分解（恒等式）：ln[(1+b_{t+H})/(1+b_t)] = ln(P 涨幅) − ln(MA 涨幅)
    #    residual 列是恒等式的数值残差，交叉验证通道应确认它 ≈ 0（浮点级）。
    lnP, lnM = np.log(out.close), np.log(out[f"ma{MA_MAIN}"])
    lnB = np.log1p(out[f"bias{MA_MAIN}"])
    for h in HORIZONS:
        leg_p = lnP.shift(-h) - lnP
        leg_m = lnM.shift(-h) - lnM
        out[f"dlnbias{h}"] = lnB.shift(-h) - lnB
        out[f"leg_price{h}"] = leg_p
        out[f"leg_ma{h}"] = leg_m
        out[f"resid{h}"] = out[f"dlnbias{h}"] - (leg_p - leg_m)

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="results")
    ap.add_argument("--out", default="results/bias_meanrev")
    a = ap.parse_args()
    root, out = Path(a.data), Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    fdf = pd.read_csv(root / "fear_daily_dump.csv", dtype={"trade_date": str})
    fear = dict(zip(fdf.trade_date, pd.to_numeric(fdf.score)))
    print(f"恐慌 fear_daily：{fdf.trade_date.min()}~{fdf.trade_date.max()}  {len(fdf)} 行"
          f"（⚠️ 只回填到 2015，早于此的极值点没有恐慌读数，相关性一节须据此裁剪样本）")

    man: dict = {"mas": list(MAS), "horizons": list(HORIZONS), "ma_main": MA_MAIN,
                 "fear_start": fdf.trade_date.min(), "fear_end": fdf.trade_date.max(),
                 "indices": {}}
    print(f"\n{'指数':>9s}{'行数':>7s}{'起':>10s}{'止':>10s}"
          f"{'有恐慌':>8s}{'bias60 可算':>12s}{'恒等式残差':>12s}{'md5':>10s}")
    for nm, f, col, oos in UNIVERSE:
        df = build(root, fear, nm, f, col)
        p = out / f"{nm}.csv"
        df.to_csv(p, index=False, float_format="%.10g")
        md5 = hashlib.md5(p.read_bytes()).hexdigest()

        rmax = float(np.nanmax(np.abs(df[[f"resid{h}" for h in HORIZONS]].to_numpy())))
        nb = int(df[f"bias{MA_MAIN}"].notna().sum())
        nf = int(df.fear.notna().sum())
        man["indices"][nm] = {
            "file": p.name, "oos": bool(oos), "md5": md5, "rows": len(df),
            "start": df.trade_date.iloc[0], "end": df.trade_date.iloc[-1],
            "n_bias60": nb, "n_fear": nf, "max_identity_resid": rmax,
            "close_min": float(df.close.min()), "close_max": float(df.close.max()),
            "bias60_min": float(df[f"bias{MA_MAIN}"].min()),
            "bias60_max": float(df[f"bias{MA_MAIN}"].max()),
            # 抽样点：交叉验证通道可用它逐个手算复核
            "spot": [{"trade_date": r.trade_date, "close": float(r.close),
                      "ma60": float(r.ma60), "bias60": float(r.bias60)}
                     for r in df.dropna(subset=["ma60"]).iloc[[0, len(df) // 2, -1]].itertuples()],
        }
        print(f"{nm:>9s}{len(df):>7d}{df.trade_date.iloc[0]:>10s}{df.trade_date.iloc[-1]:>10s}"
              f"{nf:>8d}{nb:>12d}{rmax:>12.2e}{md5[:8]:>10s}")

    (out / "manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    print(f"\n数据包 → {out}/  （{len(man['indices'])} 个指数 + manifest.json）")
    print("恒等式残差应全部是浮点级（~1e−16）；任何一个不是，说明分解写错了。")


if __name__ == "__main__":
    main()
