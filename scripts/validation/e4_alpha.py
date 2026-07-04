"""E4：因子/模型分位到底是 alpha 还是小盘 beta？

预登记：
  H0: composite 分数(rank_pct)对市值(small_size)控制后**残差 IC ≈ 0** —— 即主要是小盘/低价 beta。
  指标: 各调仓日 rank-IC(rank_pct, 前瞻收益) raw；剔除 small_size 后的偏相关 IC；
        composite 多空价差 vs small_size 单因子多空价差。
  过关: 控制 size 后偏 IC **仍稳定为正** → 验证分位可信、P5 值得；≈0 → 验证层须用相对口径、诚实定位 smart-beta。

复用 model_prediction(rank_pct) + factor_exposure(small_size) + stock_daily 前瞻收益。只读。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common import BaseRepository, md_table, pct, trade_calendar

VERSION = "ic_v1"


def _partial_ic(x: pd.Series, y: pd.Series, z: pd.Series) -> float:
    """x 对 z 回归取残差后与 y 的 Spearman（控制 z 的偏 IC）。"""
    d = pd.DataFrame({"x": x, "y": y, "z": z}).dropna()
    if len(d) < 20 or d["z"].std() == 0:
        return float("nan")
    # 用秩回归近似偏相关
    xr, yr, zr = d["x"].rank(), d["y"].rank(), d["z"].rank()
    b = np.polyfit(zr, xr, 1)
    resid = xr - (b[0] * zr + b[1])
    if resid.std() == 0:
        return float("nan")
    return float(np.corrcoef(resid, yr)[0, 1])


def run(repo: BaseRepository) -> str:
    L = ["## E4 —— 因子分位：alpha 还是小盘 beta", ""]
    for t in ["model_prediction", "factor_exposure", "stock_daily"]:
        if not repo.table_exists(t):
            return "\n".join(L + [f"**数据不足**：缺表 {t}。"])
    preds = repo.read_sql(
        "SELECT trade_date, code, rank_pct FROM model_prediction WHERE version=:v", {"v": VERSION})
    if preds.empty:
        return "\n".join(L + [f"**数据不足**：model_prediction(version={VERSION}) 为空。"])
    preds["trade_date"] = preds["trade_date"].astype(str)
    preds["rank_pct"] = pd.to_numeric(preds["rank_pct"], errors="coerce")
    dates = sorted(preds["trade_date"].unique())
    if len(dates) < 3:
        return "\n".join(L + ["**数据不足**：调仓日 < 3。"])

    # small_size 暴露
    size = repo.read_sql(
        "SELECT trade_date, code, value FROM factor_exposure WHERE factor='small_size'")
    has_size = not size.empty
    if has_size:
        size["trade_date"] = size["trade_date"].astype(str)
        size["value"] = pd.to_numeric(size["value"], errors="coerce")

    # 前瞻收益：各调仓日 → 下个调仓日
    from common import close_panel
    codes = sorted(preds["code"].unique())
    panel = close_panel(repo, codes, dates)

    raw_ics, part_ics, comp_ls, size_ls = [], [], [], []
    for d, nxt in zip(dates[:-1], dates[1:]):
        pr = preds[preds["trade_date"] == d][["code", "rank_pct"]].dropna()
        if len(pr) < 20:
            continue
        fwd = {c: (panel.get(c, {}).get(nxt, np.nan) / panel.get(c, {}).get(d, np.nan) - 1)
               for c in pr["code"]
               if panel.get(c, {}).get(d) and panel.get(c, {}).get(nxt)}
        m = pr.assign(fwd=pr["code"].map(fwd)).dropna()
        if len(m) < 20:
            continue
        raw_ics.append(float(m["rank_pct"].rank().corr(m["fwd"].rank())))
        top = m[m["rank_pct"] >= 0.8]["fwd"]; bot = m[m["rank_pct"] <= 0.2]["fwd"]
        if len(top) and len(bot):
            comp_ls.append(top.mean() - bot.mean())
        if has_size:
            sz = size[size["trade_date"] == d][["code", "value"]].rename(columns={"value": "sz"})
            mm = m.merge(sz, on="code")
            if len(mm) >= 20:
                part_ics.append(_partial_ic(mm["rank_pct"], mm["fwd"], mm["sz"]))
                th = mm[mm["sz"] >= mm["sz"].quantile(0.8)]["fwd"]
                tl = mm[mm["sz"] <= mm["sz"].quantile(0.2)]["fwd"]
                if len(th) and len(tl):
                    size_ls.append(th.mean() - tl.mean())

    def ms(a):
        a = [x for x in a if x is not None and np.isfinite(x)]
        return (np.mean(a), np.std(a), len(a)) if a else (float("nan"), float("nan"), 0)

    ri, rs, rn = ms(raw_ics)
    pi, ps, pn = ms(part_ics)
    cl, _, _ = ms(comp_ls)
    sl, _, _ = ms(size_ls)
    L.append(f"- 跨 {rn} 个调仓区间。")
    L.append("")
    L.append(md_table(["指标", "值"],
                      [["raw rank-IC (rank_pct)", f"{ri:+.3f} (IR {ri/rs:+.2f})" if rs else f"{ri:+.3f}"],
                       ["控制 small_size 偏 IC", f"{pi:+.3f}" if np.isfinite(pi) else "NA(无size)"],
                       ["composite 多空价差(均)", pct(cl)],
                       ["small_size 多空价差(均)", pct(sl) if np.isfinite(sl) else "NA"]]))
    verdict = "数据不足判定"
    if np.isfinite(pi):
        if pi > 0.02:
            verdict = "**过关**：控制市值后偏 IC 仍正 → 存在超越小盘 beta 的选股信息，验证分位可信、P5 值得。"
        elif pi <= 0.01:
            verdict = "**证伪/存疑**：控制市值后偏 IC≈0 → 分位主要是小盘/低价 beta；验证层须用相对同业口径、诚实定位 smart-beta。"
        else:
            verdict = "**边界**：控制市值后偏 IC 微弱，需更长样本确认。"
    L += ["", "### 结论", f"- {verdict}"]
    return "\n".join(L)


if __name__ == "__main__":
    from common import get_repo
    print(run(get_repo()))
