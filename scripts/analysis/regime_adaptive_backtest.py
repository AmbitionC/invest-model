"""市况自适应策略对照回测（用户命题 2026-07-15·一次性分析，只读不落库不改生产）。

命题：宽幅震荡市适合"恐慌暴跌买入、情绪修复卖出"；单边上涨市适合"买入持有"；
趋势走坏时不接飞刀。分市况适配不同买卖策略是否更好？——制定策略、真数据回测、
交叉对比验证"适配"本身的价值。

与既有治理的关系：E9 v2（趋势市中条件化 MA60 闸）与 P16（顶部识别）均为"分市况"
思想的碎片；本实验检验统一的市况层是否成立。若过判据 → 登记 P19 提案走治理。

── 市况判别（逐日·仅用截至当日数据·常量预登记写死）──────────────────
  MA60 斜率 slope = MA60[t]/MA60[t-20] - 1
  UP（趋势上行）  : 收盘 >= MA60 且 slope >= +1%
  DOWN（趋势走坏）: 收盘 <  MA60 且 slope <= -1%
  RANGE（震荡）   : 其余

── 策略（信号=收盘，仓位次日生效；换手单边成本 0.05%）─────────────────
  S1 趋势跟随（现系统风格代理）: 持有 iff 收盘>=MA20 且 收盘>=MA60（不分市况全程跑）
  S2 震荡逆向（用户提议风格）  : 入场=收盘距近60日最高收盘回撤>=10%（恐慌暴跌代理）;
       离场=收盘站回MA20（情绪修复代理）或 自入场收盘价-8%止损（不分市况全程跑）
  S3 市况自适应（提议本体）    : UP 段用 S1 规则；RANGE 段用 S2 规则；DOWN 段空仓
  基线: BH 买入持有
  （实盘的市况判断可再叠加投顾大盘方向信号；投顾信号仅 15 天历史无法参与回测，
   本实验只验证"纯数据市况层"，如实标注。恐慌暴跌实盘可用恐慌指数≥75，
   回测统一用回撤代理保证全历史可算。）

── 判读标准（跑数前写死，勿按结果回调；逐指数判定后看占比）────────────
  ① 交叉适配成立：RANGE 日内 S2 日均收益 > S1；且 UP 日内 S1 日均收益 > S2
     ——两条同时成立的指数 ≥ 2/3（这是"适配"命题的直接检验）
  ② 自适应增益：S3 相对 S1 全程 Sharpe ≥ +0.10 且 MaxDD 改善 ≥ 3pp 的指数 ≥ 2/3
  ③ 不接飞刀价值：DOWN 日指数平均日收益为负（空仓避损为正）的指数 ≥ 2/3
  （跑数前修订·合成用例暴露的边界缺陷防护：S2 止损后 10 日冷却；S3 需连续 5 日
   RANGE 方启用逆向——下跌段反弹日会被短暂标为 RANGE，无缓冲会在趋势边界反复接飞刀。
   两防护为设计常量、跑真数据前写死，非看结果回调。合成冒烟测试仅验证机制：
   10 个随机种子下 S3 下跌段暴露稳定 ≤8%、RANGE_CONFIRM 显著降换手，S3/S2 相对
   MDD 随种子翻转（噪声主导、合成序列的市况标签与构造段本就错位），故合成序列
   的收益数字对判据无证明力，判定只看真实指数数据。）
  三条全过 → 建议登记 P19（市况自适应层，先影子/计划层提示）；否则分项如实报告。

局限：指数级验证（个股噪声更大，晋升前须个股级/组合级复核）；市况判别参数
（60/20日、±1%、回撤10%、修复=MA20）为预登记工程常量非寻优产物；成本简化。

用法（Actions）：python scripts/analysis/regime_adaptive_backtest.py [--out ...]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

MA_T, MA_S = 60, 20          # 趋势线 / 斜率回看
SLOPE_UP, SLOPE_DN = 0.01, -0.01
DIP_DD = 0.10                # 震荡逆向入场：距60日高点回撤
STOP = 0.08                  # 逆向止损
COOLDOWN = 10                # 止损后 N 交易日内不再入场（防下跌趋势里反复接飞刀）
RANGE_CONFIRM = 5            # S3 需连续 N 日 RANGE 才启用逆向（市况边界过渡缓冲）
COST = 0.0005                # 单边换手成本
MIN_ROWS = 800


def _regimes(close: pd.Series) -> pd.Series:
    ma60 = close.rolling(MA_T).mean()
    slope = ma60 / ma60.shift(MA_S) - 1
    r = pd.Series("RANGE", index=close.index)
    r[(close >= ma60) & (slope >= SLOPE_UP)] = "UP"
    r[(close < ma60) & (slope <= SLOPE_DN)] = "DOWN"
    r[ma60.isna() | slope.isna()] = "NA"
    return r


def _positions(close: pd.Series, regime: pd.Series, mode: str) -> pd.Series:
    """收盘信号 → 当日目标仓位（次日生效由调用方 shift）。mode: S1|S2|S3"""
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(MA_T).mean()
    hi60 = close.rolling(60).max()
    pos = np.zeros(len(close))
    holding, entry = False, np.nan
    cooldown = 0
    range_streak = 0
    for i in range(len(close)):
        c = float(close.iloc[i])
        m20 = float(ma20.iloc[i]) if np.isfinite(ma20.iloc[i]) else np.nan
        m60 = float(ma60.iloc[i]) if np.isfinite(ma60.iloc[i]) else np.nan
        reg = regime.iloc[i]
        range_streak = range_streak + 1 if reg == "RANGE" else 0
        if cooldown > 0:
            cooldown -= 1
        if reg == "NA" or not np.isfinite(m20) or not np.isfinite(m60):
            holding = False
            continue
        style = ("S1" if mode == "S1" else "S2" if mode == "S2" else
                 ("S1" if reg == "UP" else
                  "S2" if (reg == "RANGE" and range_streak >= RANGE_CONFIRM) else "CASH"))
        if style == "CASH":
            holding = False
        elif style == "S1":
            holding = (c >= m20) and (c >= m60)
        else:  # S2
            if holding:
                if np.isfinite(entry) and c / entry - 1 <= -STOP:
                    holding = False
                    cooldown = COOLDOWN
                elif c >= m20:
                    holding = False
            else:
                dd = c / float(hi60.iloc[i]) - 1 if np.isfinite(hi60.iloc[i]) else 0.0
                if dd <= -DIP_DD and cooldown == 0:
                    holding, entry = True, c
        pos[i] = 1.0 if holding else 0.0
    return pd.Series(pos, index=close.index)


def _evaluate(close: pd.Series, pos_signal: pd.Series) -> dict:
    ret = close.pct_change().fillna(0.0)
    pos = pos_signal.shift(1).fillna(0.0)          # 次日生效
    turn = pos.diff().abs().fillna(0.0)
    strat = pos * ret - turn * COST
    nav = (1 + strat).cumprod()
    years = len(strat) / 252
    ann = nav.iloc[-1] ** (1 / years) - 1 if years > 0 else 0
    sharpe = strat.mean() / strat.std() * np.sqrt(252) if strat.std() > 0 else 0
    mdd = float((nav / nav.cummax() - 1).min())
    return {"ann": ann, "sharpe": sharpe, "mdd": mdd, "daily": strat}


def main() -> None:
    ap = argparse.ArgumentParser(description="市况自适应策略对照回测")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db)
    repo = BaseRepository(engine)
    codes = repo.read_sql(
        "SELECT code, COUNT(*) n FROM index_daily GROUP BY code HAVING n>=:m", {"m": MIN_ROWS})
    L = ["## 市况自适应策略对照回测（判读标准预登记）", ""]
    per_index = []
    for code in codes["code"].astype(str):
        df = repo.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE code=:c ORDER BY trade_date",
            {"c": code})
        close = pd.Series(pd.to_numeric(df["close"], errors="coerce").values,
                          index=df["trade_date"].astype(str)).dropna()
        if len(close) < MIN_ROWS:
            continue
        reg = _regimes(close)
        ret = close.pct_change().fillna(0.0)
        res = {m: _evaluate(close, _positions(close, reg, m)) for m in ("S1", "S2", "S3")}
        bh_nav = (1 + ret).cumprod()
        bh = {"ann": bh_nav.iloc[-1] ** (252 / len(ret)) - 1,
              "sharpe": ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else 0,
              "mdd": float((bh_nav / bh_nav.cummax() - 1).min())}
        # 交叉适配：按市况桶比较两风格的全序列日收益（含空仓日=0，衡量风格×市况产出）
        s1d, s2d = res["S1"]["daily"], res["S2"]["daily"]
        range_days, up_days, down_days = (reg == "RANGE"), (reg == "UP"), (reg == "DOWN")
        cross_range = float(s2d[range_days].mean() - s1d[range_days].mean()) * 252
        cross_up = float(s1d[up_days].mean() - s2d[up_days].mean()) * 252
        down_ann = float(ret[down_days].mean()) * 252 if down_days.any() else float("nan")
        c1 = cross_range > 0 and cross_up > 0
        c2 = (res["S3"]["sharpe"] - res["S1"]["sharpe"] >= 0.10
              and res["S3"]["mdd"] - res["S1"]["mdd"] >= 0.03)
        c3 = np.isfinite(down_ann) and down_ann < 0
        per_index.append((code, c1, c2, c3))
        L += [f"### {code}（{close.index[0]}~{close.index[-1]}，"
              f"UP {up_days.mean():.0%}/RANGE {range_days.mean():.0%}/DOWN {down_days.mean():.0%}）",
              "",
              "| 策略 | 年化 | Sharpe | MaxDD |", "|---|---:|---:|---:|",
              f"| S1 趋势跟随(全程) | {res['S1']['ann']:+.1%} | {res['S1']['sharpe']:.2f} | {res['S1']['mdd']:.1%} |",
              f"| S2 震荡逆向(全程) | {res['S2']['ann']:+.1%} | {res['S2']['sharpe']:.2f} | {res['S2']['mdd']:.1%} |",
              f"| **S3 市况自适应** | {res['S3']['ann']:+.1%} | {res['S3']['sharpe']:.2f} | {res['S3']['mdd']:.1%} |",
              f"| 买入持有 | {bh['ann']:+.1%} | {bh['sharpe']:.2f} | {bh['mdd']:.1%} |",
              "",
              f"- 交叉适配：RANGE 日内 S2−S1 年化差 **{cross_range:+.1%}**；UP 日内 S1−S2 年化差 **{cross_up:+.1%}**"
              f" → {'✅ 双向成立' if c1 else '❌ 不成立'}",
              f"- 自适应增益(S3 vs S1)：Sharpe {res['S3']['sharpe'] - res['S1']['sharpe']:+.2f}、"
              f"MaxDD {(res['S3']['mdd'] - res['S1']['mdd']) * 100:+.1f}pp → {'✅' if c2 else '❌'}",
              f"- DOWN 日指数年化 **{down_ann:+.1%}** → 不接飞刀{'✅ 有价值' if c3 else '❌ 无证据'}",
              ""]
    n = len(per_index)
    if n:
        p1 = sum(1 for _, a, _, _ in per_index if a)
        p2 = sum(1 for _, _, b, _ in per_index if b)
        p3 = sum(1 for _, _, _, c in per_index if c)
        gate = 2 * n / 3
        L += ["## 预登记判据裁决", "",
              f"| 判据 | 通过指数 | 门槛(≥2/3={gate:.1f}) | 结论 |", "|---|---:|---:|---|",
              f"| ① 交叉适配双向成立 | {p1}/{n} | {gate:.1f} | {'✅' if p1 >= gate else '❌'} |",
              f"| ② S3 增益(Sharpe+0.10 且 MaxDD+3pp) | {p2}/{n} | {gate:.1f} | {'✅' if p2 >= gate else '❌'} |",
              f"| ③ DOWN 段避损价值 | {p3}/{n} | {gate:.1f} | {'✅' if p3 >= gate else '❌'} |",
              "",
              ("**三条全过 → 建议登记 P19 市况自适应层（先影子/计划层提示，owner 拍板）**"
               if min(p1, p2, p3) >= gate else
               "**未全过 → 分项如实记录，不登记生产提案（细节见上）**")]
    md = "\n".join(L)
    print(md, flush=True)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
