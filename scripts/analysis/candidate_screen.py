"""候选标的模型体检（用户命题·只读不落库）：把投顾直播点名的个股逐个过一遍
生产买点闸 + 模型质量分 + 均线站位，判"可买入 / 值得观察 / 别碰"。

投顾主导 + 模型参谋：标的由投顾（直播）给，模型只做质量分/时机/风控参谋、不选股。
本脚本对一组给定 code：
  - 复合买点 detect_buypoints（① MA60 前置闸 → ② 回踩/突破 → ③ 量化 → ④ 环境，恐慌放松④）
  - 模型研判 _model_view（方向 前X% ★ + 因子归因）
  - 均线站位（收盘/MA20/MA60/是否站上MA60/MA60趋势/距MA20）
并归类：✅买点触发 / 🟡趋势在待回踩(观察) / ⛔左侧不买 / ❔数据不足。

来源：2026-07-21 会员直播「案例仅做技术分析、不作为投资建议」点名的个股（用户要求用模型验证）。
只读 stock_daily / 预测表，不落库、不改生产。需在有 DB 的环境（Actions/FC）跑。
  python scripts/analysis/candidate_screen.py [--out results/candidate_screen.md]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from invest_model.data import make_engine  # noqa: E402
from invest_model.orchestration.action_plan import (  # noqa: E402
    _latest_data_date, _latest_pred_date, _model_trust, _model_view,
)
from invest_model.orchestration.closed_loop import ClosedLoop  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.buypoint import BuyPointConfig, detect_buypoints  # noqa: E402
from invest_model.signals.fear import fear_gauge  # noqa: E402

# 2026-07-21 会员直播点名个股（技术分析案例；用户要求用模型验证是否值得观察/买入）
CANDIDATES = [
    ("300014.SZ", "亿纬锂能", "业绩增长线：储能超预期+业绩预告向上缺口"),
    ("300285.SZ", "国瓷材料", "涨价材料线：氧化锆涨价、回踩前期突破位"),
    ("600884.SH", "杉杉股份", "底部反转：底部涨停信号"),
    ("600988.SH", "赤峰黄金", "底部反转：6月底背离、缺口后堆量上行"),
    ("601899.SH", "紫金矿业", "黄金板块：走势弱于赤峰黄金"),
    ("600118.SH", "中国卫星", "卫星航天：缺口首次遇阻、回踩中期均线支撑后再挑战"),
    ("688598.SH", "金博股份", "题材+技术共振（历史案例）：周线三重顶突破主升浪"),
    ("000938.SZ", "紫光股份", "华为超节点算力：服务器/交换机相对抗跌"),
    ("301165.SZ", "锐捷网络", "华为超节点算力：交换机相对抗跌"),
]


def _classify(reason: str) -> str:
    if "买点触发" in reason or "抄底买点" in reason:
        return "✅可买入"
    if "样本不足" in reason:
        return "❔数据不足"
    if "MA60" in reason and "左侧" in reason:
        return "⛔左侧不买(MA60下方/下行)"
    if "未现买点" in reason:
        return "🟡趋势在·待回踩/突破(观察)"
    if "量化" in reason:
        return "🟡现买点但量化分弱"
    if "环境" in reason:
        return "🟡现买点但大盘环境差"
    if "业绩驱动" in reason:
        return "⛔业绩驱动下跌禁抄"
    return "❔其它"


def run(engine, out_path: str | None) -> str:
    loop = ClosedLoop(engine)
    repo = BaseRepository(engine)
    dt = _latest_data_date(loop)

    fear = None
    try:
        fear = float(fear_gauge(engine, dt)["score"])
    except Exception:  # noqa: BLE001
        pass
    pred_date = _latest_pred_date(loop, dt)
    preds = loop.pred_repo.get_predictions(pred_date, loop.cfg.version) if pred_date else pd.DataFrame()
    u = set(loop.uni_repo.get_universe(pred_date, loop.cfg.universe.method)) if pred_date else set()
    rank_map = (dict(zip(preds["code"], pd.to_numeric(preds["rank_pct"], errors="coerce")))
                if not preds.empty and "rank_pct" in preds.columns else {})
    tf_map = (dict(zip(preds["code"], preds["top_factors"]))
              if not preds.empty and "top_factors" in preds.columns else {})
    try:
        m_ic_ir = None
        if loop.repo.table_exists("model_registry"):
            mq = loop.repo.read_sql(
                "SELECT cv_ic_ir FROM model_registry WHERE version=:v", {"v": loop.cfg.version})
            if not mq.empty and pd.notna(mq["cv_ic_ir"].iloc[0]):
                m_ic_ir = float(mq["cv_ic_ir"].iloc[0])
        trust = _model_trust(m_ic_ir) if m_ic_ir is not None else 0.5
    except Exception:  # noqa: BLE001
        trust = 0.5
    try:
        gross = float(loop.mt.gross_exposure(dt, list(u) if u else None))
    except Exception:  # noqa: BLE001
        gross = 0.6

    codes = [c for c, _, _ in CANDIDATES]
    cfg = BuyPointConfig()
    bps = detect_buypoints(engine, dt, codes, gross if np.isfinite(gross) else 0.6,
                           rank_map, cfg, fear=fear)

    # 均线站位批量预载（一个查询）
    ma_start = (pd.to_datetime(dt) - pd.Timedelta(days=140)).strftime("%Y%m%d")
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    params.update(s=ma_start, d=dt)
    mp = repo.read_sql(
        f"SELECT code, trade_date, close FROM stock_daily "
        f"WHERE trade_date>=:s AND trade_date<=:d AND code IN ({ph})", params)
    ma_px: dict[str, pd.Series] = {}
    if not mp.empty:
        mp["close"] = pd.to_numeric(mp["close"], errors="coerce")
        for c, g in mp.groupby("code"):
            ma_px[str(c)] = g.sort_values("trade_date")["close"].dropna().reset_index(drop=True)

    def _ma(c: str):
        s = ma_px.get(c)
        if s is None or len(s) < 60:
            return float("nan"), float("nan"), "?", "?", float("nan")
        ma20 = float(s.tail(20).mean()); ma60 = float(s.tail(60).mean())
        prev60 = float(s.tail(65).head(60).mean()) if len(s) >= 65 else ma60
        last = float(s.iloc[-1])
        return last, ma20, ma60, ("是" if last >= ma60 else "否"), ("↑" if ma60 > prev60 else "↓")

    L = [f"# 直播点名个股·模型体检（决策日 {dt}）", ""]
    fear_txt = ("%.0f" % fear) if fear is not None else "?"
    L.append(f"- 恐慌 {fear_txt}（≥75 才放松环境闸）· 大盘 gross 闸 {gross:.2f} · 模型信任 trust {trust:.2f}")
    L.append("- 口径：投顾（直播）给标的，模型只做买点/质量/风控参谋。买点闸①MA60→②回踩/突破→③量化→④环境。")
    L.append("\n| 标的 | 收盘 | MA20 | MA60 | 站上MA60 | MA60 | 距MA20 | 模型研判 | 判定 | 直播理由 |")
    L.append("|---|---:|---:|---:|:--:|:--:|--:|---|---|---|")

    buys, watches, rejects = [], [], []
    for code, name, note in CANDIDATES:
        b = bps.get(code)
        reason = b.reason if b else "观察：行情样本不足"
        last, ma20, ma60, above, trend = _ma(code)
        px_last = b.last if (b and np.isfinite(b.last)) else last
        dev = (px_last / ma20 - 1) if (np.isfinite(px_last) and np.isfinite(ma20) and ma20) else float("nan")
        mr = rank_map.get(code)
        view = _model_view(mr, trust, tf_map.get(code)) if mr is not None else "—(无模型覆盖)"
        cls = _classify(reason)
        (buys if "✅" in cls else watches if "🟡" in cls else rejects).append((name, cls))
        def _n(x, d=2): return f"{x:.{d}f}" if isinstance(x, float) and np.isfinite(x) else "—"
        L.append(f"| {name} {code} | {_n(px_last)} | {_n(ma20)} | {_n(ma60)} | {above} | {trend} | "
                 f"{(f'{dev:+.0%}' if np.isfinite(dev) else '—')} | {view} | {cls} | {note} |")

    L.append("\n## 小结")
    L.append(f"- ✅可买入（买点触发）：{('、'.join(n for n, _ in buys) or '无')}")
    L.append(f"- 🟡值得观察（趋势在/待回踩/待环境）：{('、'.join(n for n, _ in watches) or '无')}")
    L.append(f"- ⛔暂不建议（左侧/禁抄）：{('、'.join(n for n, _ in rejects) or '无')}")
    L.append("\n### 读法")
    L.append("- ✅买点触发＝技术+量化+环境三闸都过（恐慌≥75 会放松环境闸）；但直播原话强调"
             "「案例仅技术分析、不作投资建议」，且暴力反弹后严禁追高——买点触发也宜等回踩、勿追涨停。")
    L.append("- 🟡趋势在但未现买点＝方向可以、位置未到，进观察池等回踩 MA20 或突破新高再动。")
    L.append("- ⛔左侧（MA60 下方/下行）＝系统「不接飞刀」铁律，无论题材多热都不给买点。")
    L.append("- 模型研判「前X%」＝全市场质量分位（越小越靠前/越好）；★＝决断度×模型信任。ETF/无覆盖记「—」。")
    md = "\n".join(L)
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(md, encoding="utf-8")
    return md


def main() -> None:
    ap = argparse.ArgumentParser(description="直播点名个股模型体检")
    ap.add_argument("--db", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    engine = make_engine(args.db) if args.db else make_engine()
    print(run(engine, args.out))


if __name__ == "__main__":
    main()
