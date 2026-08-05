"""实盘操作计划（最终交付物）：结合当前持仓 + 目标组合 + 风控评估 → 操作清单。

按需生成：对给定日期（默认最新数据日），对每只持仓/目标票给出
  动作（买/加/减/清/持）+ 当前→目标权重 + 触发理由 + 关键价位 + 账户层提示。

风控判定复用 :mod:`invest_model.portfolio.risk`，与回测同一套逻辑。
目标组合复用 :meth:`ClosedLoop._build_targets`（投顾为主 + 量化补充）。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.orchestration.closed_loop import ClosedLoop, LoopConfig
from invest_model.portfolio.risk import (armed_ladder, evaluate_holding, profit_protect,
                                         replay_hold_tier, replay_pp_tier, time_stop)
from invest_model.portfolio.sizing import buy_shares, min_lot
from invest_model.repositories.holding_repo import HoldingRepo
from invest_model.signals.buypoint import BuyPointConfig, detect_buypoints

logger = get_logger()

WATCH_POOL_CAP = 30    # 观察池上限：A 级全留 + 最近的 B 级，控制在此数量内


@dataclass
class ActionPlan:
    plan_date: str
    rows: list[dict] = field(default_factory=list)
    account: dict = field(default_factory=dict)
    etf_watch: list = field(default_factory=list)   # ETF 观察清单（watch_etf.txt 趋势/买点位）
    footer: str = ""            # 数据口径页脚（各数据源截至日+锚定声明），render 末尾输出

    def to_markdown(self) -> str:
        return render_markdown(self)


def _footer_line(dt: str, snap_d: str | None, adv_d: str | None,
                 fear_d: str | None) -> str:
    """数据口径页脚（纯函数）：各源截至日 + 落后决策日的标 ⚠️ + 价格锚声明。"""
    def _seg(label: str, d: str | None) -> str:
        if not d:
            return f"{label} 缺失⚠️"
        return f"{label} {d}" + ("" if str(d) >= dt else "⚠️落后")
    segs = [f"行情/决策日 {dt}", _seg("持仓快照", snap_d),
            _seg("投顾信号至", adv_d), _seg("恐慌指数至", fear_d)]
    return ("> 数据口径：" + " · ".join(segs)
            + " ｜ 价格锚为决策日收盘，盘中价不作触发/止损确认依据。")


def _build_data_footer(loop, dt: str) -> str:
    """回读各数据源最新日期，生成口径页脚（best-effort，失败不阻断计划）。"""
    def _max(table: str, col: str) -> str | None:
        try:
            if loop.repo.table_exists(table):
                d = loop.repo.read_sql(f"SELECT MAX({col}) m FROM {table}")
                v = d["m"].iloc[0]
                return str(v) if v is not None and pd.notna(v) else None
        except Exception:  # noqa: BLE001
            pass
        return None
    adv_d = max(filter(None, [_max("advisor_reco", "rec_date"),
                              _max("advisor_theme", "rec_date")]), default=None)
    return _footer_line(dt, _max("holding_snapshot", "snapshot_date"),
                        adv_d, _max("fear_daily", "trade_date"))


def _advisor_stance(loop: ClosedLoop, dt: str) -> tuple[str, str, int]:
    """投顾主题风向聚合（P20）：读有效主题，按方向分组计数出提示行；
    大盘类主题（主题名含"大盘"）reduce 行数 ≥2 且多于 long → stance=reduce。
    返回 (stance, 提示行文本, 大盘reduce行数)。失败返回 neutral（fail-open 不阻断计划）。"""
    try:
        th = loop.adv_repo.get_active_theme(dt)
        if th is None or th.empty:
            return "neutral", "", 0
        th = th.copy()
        th["theme"] = th["theme"].astype(str)
        th["direction"] = th["direction"].astype(str)
        # 汇总行（不逐主题 firehose）：按方向计数 + 大盘主题单列（宏观关键读数+驱动 P20）。
        cn = {"long": "看多", "reduce": "减仓", "avoid": "回避", "short": "看空",
              "watch": "观望", "exit": "退出"}
        dc = th["direction"].value_counts().to_dict()
        order = ["long", "reduce", "avoid", "short", "watch", "exit"]
        seg = "、".join(f"{cn.get(d, d)}{dc[d]}个" for d in order if d in dc)
        for d, n in dc.items():
            if d not in order:
                seg += f"、{d}{n}个"
        mkt = th[th["theme"].str.contains("大盘")]
        n_red = int((mkt["direction"] == "reduce").sum())
        n_long = int((mkt["direction"] == "long").sum())
        mtxt = ""
        if not mkt.empty:
            def _nm(t: str) -> str:
                return t.replace("大盘/", "").replace("大盘", "") or "大盘"
            longs = sorted({_nm(t) for t in mkt.loc[mkt["direction"] == "long", "theme"]})
            reds = sorted({_nm(t) for t in mkt.loc[mkt["direction"] == "reduce", "theme"]})
            parts = []
            if longs:
                parts.append(f"看多{len(longs)}个（{'、'.join(longs)}）")
            if reds:
                parts.append(f"看空{len(reds)}个（{'、'.join(reds)}）")
            if parts:
                mtxt = "。对大盘：" + "、".join(parts)
        line = f"{len(th)}个主题，{seg}{mtxt}"
        stance = "reduce" if (n_red >= 2 and n_red > n_long) else \
                 ("long" if (n_long >= 2 and n_long > n_red) else "neutral")
        return stance, line, n_red
    except Exception:  # noqa: BLE001
        return "neutral", "", 0


def _latest_data_date(loop: ClosedLoop) -> str:
    df = loop.repo.read_sql("SELECT MAX(trade_date) AS d FROM stock_daily")
    return str(df["d"].iloc[0])

def _latest_pred_date(loop: ClosedLoop, dt: str) -> str | None:
    df = loop.repo.read_sql(
        "SELECT MAX(trade_date) AS d FROM model_prediction WHERE version=:v AND trade_date<=:d",
        {"v": loop.cfg.version, "d": dt},
    )
    d = df["d"].iloc[0]
    return str(d) if d is not None else None


def _name_map(loop: ClosedLoop, codes: list[str]) -> dict[str, str]:
    if not codes:
        return {}
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    df = loop.repo.read_sql(
        f"SELECT ts_code AS code, name FROM stock_info WHERE ts_code IN ({ph})", params)
    m = {str(c): str(n) for c, n in zip(df["code"], df["name"]) if str(n)}
    # 补名：ETF/转债等不在 stock_info，用最新持仓快照里的名称兜底
    miss = [c for c in codes if c not in m]
    if miss:
        try:
            if loop.repo.table_exists("holding_snapshot"):
                ph2 = ",".join(f":m{i}" for i in range(len(miss)))
                p2 = {f"m{i}": c for i, c in enumerate(miss)}
                sn = loop.repo.read_sql(
                    "SELECT code, name FROM holding_snapshot WHERE snapshot_date="
                    "(SELECT MAX(snapshot_date) FROM holding_snapshot) "
                    f"AND code IN ({ph2})", p2)
                for c, n in zip(sn["code"], sn["name"]):
                    if str(n):
                        m[str(c)] = str(n)
        except Exception:  # noqa: BLE001
            pass
    return m


def _close_hist(loop: ClosedLoop, code: str, start: str, dt: str) -> pd.Series:
    """收盘序列（前复权口径，P11）：除权除息缺口抹平后再喂均线/硬止损，
    避免分红/送转日的机械跳空假触发风控；无复权因子时 fail-open 退回原价。"""
    from invest_model.data.adjust import qfq_close_hist
    return qfq_close_hist(loop.repo, code, start, dt)


def _etf_watch_rows(loop: ClosedLoop, dt: str, held: set[str]) -> list[dict]:
    """ETF 观察清单（config/watch_etf.txt，非持仓部分）：从 stock_daily 前复权算
    MA20/MA60/趋势/相对 MA20 位置，判定 回踩买点区/偏离上方/左侧。与计划同数据源
    （stock_daily 由 ingest_etf_daily 灌前复权 ETF 日线），口径对齐 BuyPointConfig。"""
    from pathlib import Path
    p = Path(__file__).resolve().parents[2] / "config" / "watch_etf.txt"
    if not p.exists():
        return []
    warm = (pd.to_datetime(str(dt)) - pd.Timedelta(days=150)).strftime("%Y%m%d")
    out: list[dict] = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        head = ln.split("#")[0].strip()
        if not head:
            continue
        code = head.split()[0]
        if code in held:                       # 已持仓的在「当前持仓」段，观察段不重复
            continue
        note = ln.split("#", 1)[1].strip() if "#" in ln else ""
        s = _close_hist(loop, code, warm, dt)
        if s is None or len(s.dropna()) < 60:
            out.append({"code": code, "note": note, "last": float("nan"),
                        "ma20": float("nan"), "trend": "?", "dev": float("nan"),
                        "state": "无数据（待 ingest-etf 回填）"})
            continue
        c = s.dropna().reset_index(drop=True)
        last, ma20, ma60 = float(c.iloc[-1]), float(c.tail(20).mean()), float(c.tail(60).mean())
        ma60_prev = float(c.tail(65).head(60).mean()) if len(c) >= 65 else ma60
        ma60_up = ma60 > ma60_prev
        dev = last / ma20 - 1
        if not (last >= ma60 and ma60_up):
            st = "左侧（MA60未走平/上行，不买）"
        elif abs(dev) <= 0.03:
            st = "✅回踩买点区（企稳放量则买）"
        elif dev > 0.06:
            st = f"偏离MA20 {dev:+.0%}，勿追、等回踩"
        else:
            st = "上方运行，等回踩MA20"
        out.append({"code": code, "note": note, "last": last, "ma20": ma20,
                    "trend": ("MA60↑" if ma60_up else "MA60↓"), "dev": dev, "state": st})
    return out


def _hs300_median_hint(loop: ClosedLoop, dt: str) -> str | None:
    """P26：沪深300 收盘相对全历史 expanding 中位线的位置（提示-only）。

    基底 results/index_dump_000300_SH.csv（tushare 2005 起全历史，E21 同源数据），
    基底末日之后的收盘从 index_daily 增量补齐；任何一步失败返回 None 不影响计划。
    """
    from pathlib import Path
    base = Path(__file__).resolve().parents[2] / "results" / "index_dump_000300_SH.csv"
    if not base.exists():
        return None
    hist = pd.read_csv(base, dtype={"trade_date": str})[["trade_date", "close"]]
    hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
    hist = hist.dropna()
    tail = loop.repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code='000300.SH' AND trade_date>:s "
        "ORDER BY trade_date", {"s": str(hist["trade_date"].max())})
    if not tail.empty:
        tail["close"] = pd.to_numeric(tail["close"], errors="coerce")
        hist = pd.concat([hist, tail.dropna()], ignore_index=True)
    hist = hist[hist["trade_date"] <= str(dt)]
    if len(hist) < 500:  # E21 预热口径
        return None
    last = float(hist["close"].iloc[-1])
    closes = hist["close"].to_numpy(dtype=float)
    med = float(pd.Series(closes).median())
    dev = last / med - 1
    # 双锚显示（owner 2026-07-31 命题）：全量 expanding=深值位（策略锚，E21 全历史最优——
    # 2007 型泡沫会提前清仓、回撤 -24% vs 混合锚 -57%）；滚动5年=时代位（修正早年低点位拖低、
    # 仅作参考读数）。两锚分歧大（如 2024-09：-5.3% vs -21.4%）本身就是"时代性便宜"信号。
    roll = float(pd.Series(closes[-1250:]).median()) if len(closes) >= 1250 else None
    roll_txt = f"｜滚动5年锚 {roll:.0f}（{last / roll - 1:+.1%}·参考）" if roll else ""
    side = "下方" if last < med else "上方"
    act = "宽基低吸窗口（下方只买不卖）" if last < med else "只减不加（上方只卖不买）"
    # 状态语境（E21 稳健性复核建议）：expanding 口径下最近一次处中位线下方的月末距今几个月，
    # 防止"上方"被误读为即期卖出指令——它是持续多月的慢变量防御态。
    streak = ""
    try:
        ym = hist["trade_date"].str[:6]
        me_idx = hist.index[(ym != ym.shift(-1))].tolist()
        last_below = None
        for i in me_idx:
            j = hist.index.get_loc(i) + 1
            if closes[j - 1] < float(pd.Series(closes[:j]).median()):
                last_below = str(hist.loc[i, "trade_date"])
        if last_below and side == "上方":
            months = (int(str(dt)[:4]) - int(last_below[:4])) * 12 + int(str(dt)[4:6]) - int(last_below[4:6])
            streak = f"；该口径自 {last_below[:6]} 起连续约 {months} 个月处上方＝慢变量防御态、非即期卖出指令"
    except Exception:  # noqa: BLE001
        streak = ""
    return (f"指数贵贱（P26·提示）：沪深300 {last:.0f} 处全历史中位线 {med:.0f} **{side}**"
            f"（{dev:+.1%}）＝{act}{roll_txt}；口径 E21（下方日未来3年年化+13.8% vs 上方-1.1%）{streak}")


_BASE_SLEEVE_CODES = ("510300.SH",)   # P27 指数底仓标的：沪深300ETF


def _index_hist_by(loop: ClosedLoop, dt: str, csv_name: str, db_code: str,
                   col: str = "close") -> pd.Series | None:
    """通用：指数全历史收盘（静态基底 CSV + index_daily 增量），截至 dt。P27 v2 多腿共用。"""
    from pathlib import Path
    base = Path(__file__).resolve().parents[2] / "results" / csv_name
    if not base.exists():
        return None
    hist = pd.read_csv(base, dtype={"trade_date": str})[["trade_date", col]].rename(
        columns={col: "close"})
    hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
    hist = hist.dropna()
    try:
        tail = loop.repo.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE code=:c AND trade_date>:s "
            "ORDER BY trade_date", {"c": db_code, "s": str(hist["trade_date"].max())})
        if not tail.empty:
            tail["close"] = pd.to_numeric(tail["close"], errors="coerce")
            hist = pd.concat([hist, tail.dropna()], ignore_index=True)
    except Exception:  # noqa: BLE001 — 无增量不阻断（基底本身够用）
        pass
    hist = hist[hist["trade_date"] <= str(dt)].reset_index(drop=True)
    return hist.set_index("trade_date")["close"] if len(hist) >= 500 else None


def _p31_sell_hint(loop: ClosedLoop, dt: str) -> str | None:
    """卖出纪律（2026-08-02 owner 拍板：删除 H 强度分层，回归无条件月卖 5%）。

    E30 红队三条独立证据证伪 H 分层的收益主张：无条件卖 5% 在两种资金口径下全面 ≥ H 分层；
    随机打乱 H（N=300）后真值年化落在打乱分布第 4 百分位、夏普第 52 百分位（噪声位）；
    bootstrap（B=2000）冠军为 flat5/flat10。定义无关上界：卖 5% 与卖 10% 夏普同为 0.61、
    年化仅差 0.45pp ⇒ 任何在两档间切换的规则贡献上限 0.45pp。
    **卖出机制本身保留**——在锚上方持续减仓是全套最大单一贡献（回撤 −50%→−22%）。
    """
    hs = _index_hist_by(loop, dt, "index_dump_000300_SH.csv", "000300.SH")
    if hs is None:
        return None
    c = hs.to_numpy(dtype=float)
    med = float(pd.Series(c).median())
    if c[-1] < med:
        return None                              # 下方只买不卖
    return (f"卖出纪律：沪深300 {c[-1]:.0f} 处中位线 {med:.0f} 上方（{c[-1] / med - 1:+.1%}）"
            f"＝各腿在各自卖出线（中位线×1.30，创业板×1.43）上方按月减 5%（无条件、不看情绪强度）。"
            f"E30 红队已证伪按强度分层的收益主张，规则回归简单；卖出机制本身是回撤控制的主要来源")


# 宽基四腿买卖闸：唯一定义在 invest_model/broad_gates.py（P58，2026-08-05）。
# 那里同时记着「为什么是 1.30」「代价是什么」「为什么这不算 E51 通过」。
from invest_model.broad_gates import BUY_MUL as _BUY_MUL, SELL_MUL as _SELL_MUL  # noqa: E402


def _sell_above(name: str):
    """卖出闸判定：收盘 > 中位线 × 该腿倍数。"""
    mul = _SELL_MUL[name]
    return lambda c, m, r: c > m * mul


_BROAD_LEGS = [
    # (名称, 基底CSV, 列, DB代码, ETF, 买规则, 卖规则)——E28 简化篮子（owner 2026-08-01：
    # 只做沪深300/创业板/科创50/红利；中证500/1000 配置记档保留可随时恢复）
    ("沪深300", "index_dump_000300_SH.csv", "close", "000300.SH", "510300",
     lambda c, m, r: c < m, "＜全量中位线（周频·池20%）",
     _sell_above("沪深300"), "＞中位线×1.30（月减5%）"),
    ("创业板", "spread_full_history.csv", "chinext", "399006.SZ", "159915",
     lambda c, m, r: c < m * 0.90, "＜中位线−10%带（周频·池20%）",
     _sell_above("创业板"), "＞中位线×1.43（月减5%）"),
    # 2026-08-02 回滚：此前部署的「长持不设卖出」不是 E31 验证过的配置。实测同窗口
    # 含卖出 13.00%/夏普0.59/回撤−17.2%/卡玛0.76 vs 不卖 13.21%/0.55/−22.6%/0.58
    # ＝多赚 0.21pp 换 5.4pp 回撤，风险调整后严格更差 ⇒ 与其余三腿统一，按月减 5%。
    ("科创50", "index_dump_000688_SH.csv", "close", "000688.SH", "588000",
     lambda c, m, r: False, "深回撤阶梯 L50（距全历史峰 −50/−55/−60/−65 四档，各档一轮只买一次，"
     "投当前现金 30/35/40/50%）+ 恐慌抢买",
     _sell_above("科创50"), "＞中位线×1.30（月减5%）"),
    ("红利", "index_dump_000922_CSI.csv", "close", "000922.CSI", "515080",
     lambda c, m, r: c < m, "＜全量中位线（周频·池20%·临时价格锚，E26 估值锚待验）",
     _sell_above("红利"), "＞中位线×1.30（月减5%）"),
]


def _broad_leg_states(loop: ClosedLoop, dt: str) -> list[dict]:
    """四腿的窗口判定读数——提示行 `_broad_legs_hint` 与落库 `_persist_broad_leg_state`
    共用同一份计算，保证 issue #9 的计划、库表、前端板块三处永远是同一个数。

    state：buy=买入窗开 ｜ panic=恐慌抢买窗（价格未到但恐慌≥75）｜ sell=卖出区 ｜ hold=持有区。
    """
    fear = _fear_score(loop, dt)
    out: list[dict] = []
    for name, csvn, col, code, etf, fbuy, buy_txt, fsell, sell_txt in _BROAD_LEGS:
        try:
            s = _index_hist_by(loop, dt, csvn, code, col=col)
            if s is None:
                continue
            c = s.to_numpy(dtype=float)
            last, med = float(c[-1]), float(pd.Series(c).median())
            r1250 = float(pd.Series(c[-1250:]).median()) if len(c) >= 1250 else None
            # 乖离率（P39）：收盘/MA60−1，及其**因果全历史分位**（只用当日可得历史）。
            # ⚠️ **只作展示与波动刻度，不参与任何买卖闸判定。** E37（2026-08-02）已判死
            # 它作方向信号：进入全历史前 5% 分位后未来 20 日反而 +0.53~+5.70pp、
            # 破历史极值后 60 日 +16.5~+33.7%＝顶部机械信号第四次失败。唯一未被否定的
            # 残留是「破极值后 60 日最大回撤 −11.5~−21.2%、明显高于常态」⟹ 波动刻度。
            bias = bias_pct = bias_rank = None
            if len(c) >= 60:
                ma60 = float(pd.Series(c[-60:]).mean())
                if ma60 > 0:
                    bias = last / ma60 - 1.0
                    bs = (pd.Series(c) / pd.Series(c).rolling(60).mean() - 1.0).dropna()
                    if len(bs) >= 250:
                        bias_pct = float((bs <= bias).mean())
                        bias_rank = int((bs < bias).sum()) + 1   # 1＝历史最低
            price_buy = bool(fbuy(last, med, r1250))
            panic = fear is not None and fear >= 75
            if price_buy:
                state = "buy"
            elif panic:
                state = "panic"
            elif fsell(last, med, r1250):
                state = "sell"
            else:
                state = "hold"
            out.append({
                "name": name, "etf": etf, "code": code,
                "last": last, "med": med, "r1250": r1250,
                "anchor": med if "中位线" in buy_txt else (r1250 or med),
                "buy_mul": _BUY_MUL[name], "sell_mul": _SELL_MUL[name],
                "buy_txt": buy_txt, "sell_txt": sell_txt,
                "state": state, "fear": fear,
                "bias60": bias, "bias_pct": bias_pct, "bias_rank": bias_rank,
            })
        except Exception:  # noqa: BLE001
            continue
    return out


_BROAD_STATE_TXT = {"buy": "🟢买入窗开", "panic": "🟢恐慌抢买窗",
                    "sell": "🔴卖出区", "hold": "⚪持有区"}


def _broad_legs_hint(loop: ClosedLoop, dt: str) -> str | None:
    """P27 v2：独立宽基账户·四腿窗口状态（owner 2026-08-01 拍板：废除 25% 总资产目标、
    两账户独立决策、多指数配置）。各腿独立触发；恐慌≥75 时任何腿均可抢买池 50%。"""
    sts = _broad_leg_states(loop, dt)
    if not sts:
        return None
    rows = [f"{s['name']}({s['etf']}) {s['last']:.0f} 距锚{s['last'] / s['anchor'] - 1:+.0%} "
            f"{_BROAD_STATE_TXT[s['state']]}" for s in sts]
    return ("宽基账户（P27 v2·独立决策·四腿）：" + " ｜ ".join(rows)
            + "。买入：" + "；".join(f"{n}{bt}" for n, _, _, _, _, _, bt, _, _ in _BROAD_LEGS)
            + "；恐慌≥75 任意腿抢买池 50%；月度入金四腿各 25%、池内现金放货基")


def _persist_broad_leg_state(loop: ClosedLoop, dt: str, sts: list[dict],
                             shares_map: dict, cost_map: dict, last_close: dict) -> None:
    """四腿状态落库（`/invest/broad` 与前端「宽基指数」板块的数据源）。best-effort。

    shares/mkt_value/cost_price 取**实盘** current_holding 里对应 ETF 的持仓；本表逐日累积，
    因而它就是 owner 要的「以当前时间为起点」的仓位账本（历史仓位只有回测口径，不入此表）。
    """
    rows = []
    for s in sts:
        etf = f"{s['etf']}.SH" if s["etf"].startswith(("5", "6")) else f"{s['etf']}.SZ"
        etf = _BROAD_ETF.get(s["name"], etf)
        sh = float(shares_map.get(etf, 0) or 0)
        px = float(last_close.get(etf, 0) or 0)
        rows.append({
            "trade_date": str(dt), "leg": s["name"], "etf": etf,
            "close": round(s["last"], 4), "median": round(s["med"], 4),
            "buy_line": round(s["med"] * s["buy_mul"], 4),
            "sell_line": round(s["med"] * s["sell_mul"], 4),
            "buy_mul": s["buy_mul"], "sell_mul": s["sell_mul"],
            "state": s["state"], "fear": s["fear"],
            "bias60": None if s["bias60"] is None else round(s["bias60"], 6),
            "bias_pct": None if s["bias_pct"] is None else round(s["bias_pct"], 6),
            "bias_rank": s["bias_rank"],
            "shares": sh, "mkt_value": round(sh * px, 3),
            "cost_price": float(cost_map.get(etf, 0) or 0),
        })
    if rows:
        loop.repo.upsert("broad_leg_state", pd.DataFrame(rows), ["trade_date", "leg"])


# ── 陈老师宽基体系·执行纪律层（P51~P53，提示-only）───────────────────────────
# 2026-08-04 内化审计的产物。此前几轮一直在试图把他的每样东西变成"能不能跑赢"的回测规则，
# 但他体系里很大一部分根本不是收益规则，是**执行纪律与判断框架**——它们不做收益主张，
# 因而也不适用 E 系列的超额判据（用"能不能跑赢"去卡它们是判据错配）。
# 这一层的验收标准只有两条：①算术可复核 ②不改变任何仓位、不产生自动交易。
_BROAD_ETF = {"沪深300": "510300.SH", "创业板": "159915.SZ",
              "科创50": "588000.SH", "红利": "515080.SH"}


def _broad_anchor_states(loop: ClosedLoop, dt: str) -> list[dict]:
    """四腿的锚位读数（收盘 / expanding 中位线 / 买卖闸倍数），P51~P52 共用。"""
    out = []
    for name, csvn, col, code, etf, _fb, _bt, _fs, _st in _BROAD_LEGS:
        try:
            s = _index_hist_by(loop, dt, csvn, code, col=col)
            if s is None:
                continue
            c = s.to_numpy(dtype=float)
            last, med = float(c[-1]), float(pd.Series(c).median())
            out.append({"name": name, "etf": _BROAD_ETF.get(name, ""), "last": last, "med": med,
                        "buy_mul": _BUY_MUL[name],
                        "sell_mul": _SELL_MUL[name]})
        except Exception:  # noqa: BLE001
            continue
    return out


def _first_lot_cap(last: float, med: float) -> float:
    """容错三步法的「首笔上限」——把他的口算推广成本系统锚位的通式。

    原始算术（2026-03-23）：3700 买入、极端情形跌到 3100 补仓，要把均价压回 3400 需后备
    资金 46%、压回 3300 需 63% ⟹ **首笔最多动用总资金 37%**。
    本系统的对应量：极端落点取 P28 已上线的深危机口径「中位线下方 10%」＝ med×0.90，
    安全线取 P26 的 expanding 中位线 med（＝"指数会长期停留其上的点位"的系统定义）。
    解 f：在 last 处投 f、在 0.9·med 处投 (1−f)，要求成交均价 ≤ med
        f/last + (1−f)/(0.9·med) ≥ 1/med  ⟹  f ≤ (1/med − 1/d) / (1/last − 1/d),  d = 0.9·med
    在卖出闸 1.30·med 处该式给出 32.5%，与他实盘口算的 37% 同量级——这不是巧合，
    是同一个"留够后备把均价压到长期停留位"的约束。
    """
    if last <= med:
        return 1.0                      # 中位线下方：全额可用（跌到 0.9·med 补仓仍安全）
    d = 0.90 * med
    den = 1.0 / last - 1.0 / d
    if den >= -1e-12:
        return 0.0
    return max(0.0, min(1.0, (1.0 / med - 1.0 / d) / den))


def _fault_tolerance_hint(loop: ClosedLoop, dt: str, cost_map: dict,
                          shares_map: dict, last_close: dict) -> str | None:
    """P51 容错自检行（提示-only）：把"安全"还原成可计算量。

    陈老师的定义：**安全 ＝ 持仓均价 ≤ 指数会长期停留其上的点位**（他取沪深300 的
    3300~3400）。本系统把"长期停留点位"落在 P26 的 expanding 中位线上——同一个锚，
    因而这条行与买卖闸完全同源，不引入新参数。
    三问（答不上就是没有容错，不该按那个仓位买）：
      ①还要再跌多少我才扛不住 ②均价会到哪 ③还需多少后备资金
    这条行不产生任何买卖指令，只把三问的答案先算出来摆在决策前面。
    """
    rows = []
    for st in _broad_anchor_states(loop, dt):
        etf, last, med = st["etf"], st["last"], st["med"]
        sh = float(shares_map.get(etf, 0) or 0)
        cost = float(cost_map.get(etf, 0) or 0)
        px = float(last_close.get(etf, 0) or 0)
        cap = _first_lot_cap(last, med)
        if sh <= 0 or cost <= 0 or px <= 0:
            rows.append(f"{st['name']} 无持仓·首笔上限 {cap:.0%}")
            continue
        k = px / last                                  # ETF 价 ↔ 指数点 的换算因子
        cost_idx = cost / k if k > 0 else float("nan")
        tgt = med * k                                  # 安全线对应的 ETF 价
        if cost_idx <= med:
            rows.append(f"{st['name']} 均价≈{cost_idx:.0f}点 ✅安全（≤中位线 {med:.0f}）")
        elif px < tgt:
            need = sh * (cost - tgt) / (tgt / px - 1.0)
            rows.append(f"{st['name']} 均价≈{cost_idx:.0f}点 ⚠高于中位线 {med:.0f}，"
                        f"按现价还需后备 {need:,.0f} 元才能压回安全线")
        else:
            rows.append(f"{st['name']} 均价≈{cost_idx:.0f}点 ⚠高于中位线 {med:.0f}，"
                        f"且现价也在线上＝**这个价位买不回容错，只能等**")
    if not rows:
        return None
    return ("容错自检（P51·提示）：" + " ｜ ".join(rows)
            + "。安全＝均价≤该指数 expanding 中位线（与 P26 同锚）；"
              "首笔上限＝留够后备、在中位线下方 10% 补完仍能把均价压回安全线的最大首投比例"
              "（卖出闸处约 32%，与实盘口算的 37% 同量级）。容错是一票否决项：答不上"
              "「还需多少后备/均价会到哪」就不该按那个仓位买")


def _broad_no_action_hint(loop: ClosedLoop, dt: str, shares_map: dict) -> str | None:
    """P52「不动也是决策」行（提示-only）：四腿都没触发时，把不动的正反两条理由写出来。

    源自他的实盘表述习惯——对"不动"分别给出**为何不卖**与**为何不买**两条理由，
    而不是沉默跳过。沉默会让人把"系统没说话"读成"系统失灵"或"可以随便动"。
    """
    sts = _broad_anchor_states(loop, dt)
    if not sts:
        return None
    fear = _fear_score(loop, dt)
    if fear is not None and fear >= 75:
        return None                                   # 恐慌抢买窗开着，不是"不动"的日子
    act = [s for s in sts
           if s["last"] < s["med"] * s["buy_mul"] or s["last"] > s["med"] * s["sell_mul"]]
    if act:
        return None                                   # 有腿触发，交给 P27 v2 行
    held = [s["name"] for s in sts if float(shares_map.get(s["etf"], 0) or 0) > 0]
    gaps = "、".join(f"{s['name']}距买入线 {s['last'] / (s['med'] * s['buy_mul']) - 1:+.0%}"
                    f"/距卖出线 {s['last'] / (s['med'] * s['sell_mul']) - 1:+.0%}" for s in sts)
    why_hold = (f"为何不卖：{'、'.join(held)} 均未上到各自卖出闸，减仓会白白让出 beta 底座"
                if held else "为何不卖：本账户暂无宽基持仓，无可卖")
    return (f"宽基不动（P52·提示）：四腿今日均未触发买卖闸——{gaps}。"
            f"{why_hold}；为何不买：价格在买入闸上方，此时买入会抬高均价、压低容错，"
            f"而容错是一票否决项。**不动是本日的决策结果，不是系统没跑**")


def _hs300_hist(loop: ClosedLoop, dt: str) -> pd.DataFrame | None:
    """P26/P27/P28 共用：沪深300 全历史收盘（静态基底 CSV + index_daily 增量），截至 dt。"""
    from pathlib import Path
    base = Path(__file__).resolve().parents[2] / "results" / "index_dump_000300_SH.csv"
    if not base.exists():
        return None
    hist = pd.read_csv(base, dtype={"trade_date": str})[["trade_date", "close"]]
    hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
    hist = hist.dropna()
    tail = loop.repo.read_sql(
        "SELECT trade_date, close FROM index_daily WHERE code='000300.SH' AND trade_date>:s "
        "ORDER BY trade_date", {"s": str(hist["trade_date"].max())})
    if not tail.empty:
        tail["close"] = pd.to_numeric(tail["close"], errors="coerce")
        hist = pd.concat([hist, tail.dropna()], ignore_index=True)
    hist = hist[hist["trade_date"] <= str(dt)].reset_index(drop=True)
    return hist if len(hist) >= 500 else None


def _fear_score(loop: ClosedLoop, dt: str) -> float | None:
    df = loop.repo.read_sql(
        "SELECT score FROM fear_daily WHERE trade_date<=:d ORDER BY trade_date DESC LIMIT 1",
        {"d": str(dt)})
    if df.empty or pd.isna(df["score"].iloc[0]):
        return None
    return float(df["score"].iloc[0])


def _base_sleeve_hint(loop: ClosedLoop, dt: str, mv: dict, equity: float) -> str | None:
    """P27：指数底仓状态与建仓窗口（提示-only，执行由 owner 手动）。"""
    hist = _hs300_hist(loop, dt)
    if hist is None or equity <= 0:
        return None
    target = float(os.getenv("BASE_SLEEVE_TARGET", "0.25"))
    base_mv = sum(float(mv.get(c, 0.0)) for c in _BASE_SLEEVE_CODES)
    ratio = base_mv / equity
    last = float(hist["close"].iloc[-1])
    med = float(hist["close"].median())
    fear = _fear_score(loop, dt)
    open_reasons = []
    if last < med:
        open_reasons.append(f"P26 中位线下方（{last / med - 1:+.1%}）")
    if fear is not None and fear >= 75:
        open_reasons.append(f"E17 恐慌窗口（{fear:.0f}≥75）")
    if open_reasons:
        win = "**开**（" + "、".join(open_reasons) + "）＝可分批买入底仓至目标"
    else:
        fear_txt = f"{fear:.0f}" if fear is not None else "—"
        win = (f"关（沪深300 处中位线上方 {last / med - 1:+.1%}、恐慌 {fear_txt}<75）"
               f"＝只等不追、不建仓")
    return (f"指数底仓（P27）：当前底仓占比 {ratio:.0%}（目标 {target:.0%}，标的 510300 沪深300ETF）"
            f"｜建仓窗口：{win}")


def _leverage_window_hint(loop: ClosedLoop, dt: str) -> str | None:
    """P28：杠杆窗口识别（三信号取二共振才输出，平时静默；提示-only、L≤30% 硬顶）。"""
    hist = _hs300_hist(loop, dt)
    if hist is None:
        return None
    c = hist["close"].to_numpy(dtype=float)
    last, med, peak = float(c[-1]), float(pd.Series(c).median()), float(c.max())
    fear = _fear_score(loop, dt)
    sigs = []
    if last < med * 0.90:
        sigs.append(f"①中位线下方≥10%（{last / med - 1:+.1%}）")
    if last / peak - 1 <= -0.40:
        sigs.append(f"②距历史峰回撤≥40%（{last / peak - 1:+.1%}）")
    if fear is not None and fear >= 85:
        sigs.append(f"③深度恐慌（{fear:.0f}≥85）")
    if len(sigs) < 2:
        return None
    return (f"🚨 杠杆窗口（P28·提示-only）：{len(sigs)}/3 信号共振——" + "、".join(sigs)
            + "＝极高确定性底部窗口开启。规则：仅宽基指数、债务比例 L≤30% 硬顶"
              "（E23：50% 历史爆仓证伪）、owner 手动执行、系统不自动交易")


def _and_leverage_state(loop: ClosedLoop, dt: str) -> dict | None:
    """P30（AND 共振·低价×恐慌加杠杆信号）状态：owner 2026-08-01 拍板上线。

    定义（写死）：沪深300 收盘 < 全历史 expanding 中位线 且 恐慌 EOD ≥75。
    十一年半仅 2024-01/02 微盘崩一段共 5 天（其后 250 日 +21%~+25%）＝极稀有强信号。
    输出为常驻状态行（未触发也显示、强透出），触发时🚨并反复提醒；硬约束继承 P28：
    L≤30%、仅宽基、owner 手动、系统零自动交易。同时计算 P28 三信号数供落库/前端。
    """
    hist = _hs300_hist(loop, dt)
    if hist is None:
        return None
    c = hist["close"].to_numpy(dtype=float)
    last, med, peak = float(c[-1]), float(pd.Series(c).median()), float(c.max())
    fear = _fear_score(loop, dt)
    low = last < med
    panic = fear is not None and fear >= 75
    p28 = sum([last < med * 0.90, last / peak - 1 <= -0.40,
               fear is not None and fear >= 85])
    return {"active": bool(low and panic), "low": low, "panic": panic,
            "close": last, "median": med, "gap": last / med - 1,
            "fear": fear, "p28_count": int(p28)}


def _and_leverage_hint(st: dict | None) -> str | None:
    if st is None:
        return None
    fear_txt = f"{st['fear']:.0f}" if st["fear"] is not None else "—"
    if st["active"]:
        return (f"🚨🚨 加杠杆信号（P30·AND 共振）触发：沪深300 {st['close']:.0f} 低于全历史中位线"
                f"（{st['gap']:+.1%}）且恐慌 {fear_txt}≥75——十一年半仅在 2024-02 微盘崩出现过一段"
                f"（其后一年 +21%~+25%）＝极高确定性窗口。规则：仅宽基指数、债务 L≤30% 硬顶、"
                f"融资成本≤6%/年、owner 手动执行、系统不自动交易。本提醒在信号存续期每日重复")
    lo = "✓低价" if st["low"] else f"✗价格（中位线上方 {st['gap']:+.1%}）"
    pa = "✓恐慌" if st["panic"] else f"✗恐慌（{fear_txt}<75）"
    return (f"杠杆信号（P30·AND 共振）：未触发——{lo} × {pa}"
            f"｜P28 深危机窗 {st['p28_count']}/3。两者任一触发将强提醒")


def _persist_leverage_signal(loop: ClosedLoop, dt: str, st: dict,
                             snapshot_ts: str = "EOD") -> None:
    """杠杆信号状态落库（FaaS API/前端强透出数据源）。best-effort。"""
    import json as _json
    loop.repo.upsert("leverage_signal", pd.DataFrame([{
        "trade_date": str(dt), "snapshot_ts": snapshot_ts,
        "and_active": int(st["active"]), "p28_count": st["p28_count"],
        "close": round(st["close"], 2), "median": round(st["median"], 2),
        "fear": st["fear"], "detail": _json.dumps({
            "low": st["low"], "panic": st["panic"], "gap": round(st["gap"], 4),
            "rules": "L≤30%硬顶·仅宽基·融资成本≤6%·owner手动·零自动交易",
        }, ensure_ascii=False),
    }]), ["trade_date", "snapshot_ts"])


def _round_lot(shares: float) -> float:
    """A 股按 100 股取整（卖出允许零股，这里统一向最接近的手取整）。"""
    return float(round(shares / 100.0) * 100)


def _update_policy_shadow(loop: ClosedLoop, dt: str, reco: pd.DataFrame, bps: dict) -> None:
    """研报速通影子验证：逐日更新两条虚拟净值，供 4~6 周后复核该政策。

    fast＝信号次一交易日收盘直入；gate＝旧严格闸门首次触发日收盘入（未触发即空仓）。
    只记 research A/B 级、近 90 天的信号；每计划日刷新 last_close 与两侧收益。
    """
    if reco is None or reco.empty or "source_type" not in reco.columns:
        return
    back = (pd.to_datetime(dt) - pd.Timedelta(days=90)).strftime("%Y%m%d")
    sig = reco[(reco["source_type"] == "research") & (reco["grade"].isin({"A", "B"}))
               & (reco["rec_date"].astype(str) >= back)]
    if sig.empty:
        return
    exist = pd.DataFrame()
    if loop.repo.table_exists("policy_shadow"):
        exist = loop.repo.read_sql(
            "SELECT * FROM policy_shadow WHERE signal_date>=:b", {"b": back})
    ex_map = {(str(r["signal_date"]), str(r["code"])): dict(r)
              for _, r in exist.iterrows()} if not exist.empty else {}

    def _close_at(code: str, day: str) -> float | None:
        df = loop.repo.read_sql(
            "SELECT close FROM stock_daily WHERE code=:c AND trade_date=:d",
            {"c": code, "d": day})
        if df.empty:
            return None
        v = pd.to_numeric(df["close"].iloc[0], errors="coerce")
        return float(v) if pd.notna(v) and v > 0 else None

    out = []
    for _, s in sig.iterrows():
        key = (str(s["rec_date"]), str(s["code"]))
        row = ex_map.get(key) or {"signal_date": key[0], "code": key[1],
                                  "grade": str(s["grade"]), "d0_date": None,
                                  "d0_close": None, "gate_date": None, "gate_close": None}
        if not row.get("d0_close"):
            d0 = loop.repo.read_sql(
                "SELECT MIN(trade_date) AS d FROM stock_daily "
                "WHERE code=:c AND trade_date>:s AND trade_date<=:d",
                {"c": key[1], "s": key[0], "d": dt})["d"].iloc[0]
            if d0 is not None:
                row["d0_date"], row["d0_close"] = str(d0), _close_at(key[1], str(d0))
        bp = bps.get(key[1])
        if not row.get("gate_close") and bp is not None and getattr(bp, "is_buy", False):
            row["gate_date"], row["gate_close"] = dt, _close_at(key[1], dt)
        last = _close_at(key[1], dt)
        if last:
            row["last_date"], row["last_close"] = dt, last
            d0c = pd.to_numeric(row.get("d0_close"), errors="coerce")
            gtc = pd.to_numeric(row.get("gate_close"), errors="coerce")
            row["fast_ret"] = round(last / float(d0c) - 1, 6) if pd.notna(d0c) and d0c else None
            row["gate_ret"] = round(last / float(gtc) - 1, 6) if pd.notna(gtc) and gtc else None
        out.append({k: row.get(k) for k in (
            "signal_date", "code", "grade", "d0_date", "d0_close", "gate_date",
            "gate_close", "last_date", "last_close", "fast_ret", "gate_ret")})
    if out:
        loop.repo.upsert("policy_shadow", pd.DataFrame(out), ["signal_date", "code"])


def build_action_plan(engine, cfg: LoopConfig | None = None, dt: str | None = None,
                      cash: float = 0.0, persist: bool = True,
                      min_trade: float = 0.01, buypoint: bool = True,
                      bp_cfg: BuyPointConfig | None = None) -> ActionPlan:
    """生成操作计划。

    engine：数据库引擎；cfg：LoopConfig（含 risk / portfolio / version）；
    dt：决策日（默认最新数据日）；cash：账户现金（用于折算总权益与股数）；
    buypoint：True=研报标的先进观察池、仅买点触发才建议买入（手册第1-2步）。
    """
    loop = ClosedLoop(engine, cfg)
    dt = dt or _latest_data_date(loop)
    rc = loop.cfg.risk
    hrepo = HoldingRepo(engine)
    holdings = hrepo.get_all()

    # 持仓现价优先用"不早于最新行情日"的持仓快照(券商真实价)：抗 Tushare EOD 发布延迟、
    # 手动补跑也能反映当日；快照更旧则退回 EOD。均线/历史仍走 stock_daily。{code:(date,price)}
    snap_px: dict[str, tuple[str, float]] = {}
    try:
        if loop.repo.table_exists("holding_snapshot"):
            sp = loop.repo.read_sql(
                "SELECT snapshot_date, code, last_price FROM holding_snapshot "
                "WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM holding_snapshot)")
            for _, r in sp.iterrows():
                v = pd.to_numeric(r["last_price"], errors="coerce")
                if str(r["snapshot_date"]) >= dt and pd.notna(v) and float(v) > 0:
                    snap_px[str(r["code"])] = (str(r["snapshot_date"]), float(v))
    except Exception:  # noqa: BLE001
        pass

    # ── 当前持仓估值 ──
    held_codes = list(holdings["code"]) if not holdings.empty else []
    last_close: dict[str, float] = {}
    cost_map: dict[str, float] = {}
    shares_map: dict[str, float] = {}
    entry_map: dict[str, str] = {}
    for _, h in holdings.iterrows():
        # 近15日窗取"≤决策日最近有效收盘"：ETF 行情由独立 timer 入库，当日行缺失时
        # 此前回退成本价 → 现价列失真、权益/权重分母跟着偏（0716 事故），改为回退昨收。
        start15 = (pd.Timestamp(str(dt)) - pd.Timedelta(days=15)).strftime("%Y%m%d")
        s = _close_hist(loop, h["code"], start15, dt)
        px = float(s.iloc[-1]) if not s.empty else float(h["cost_price"] or 0)
        if h["code"] in snap_px:
            px = snap_px[h["code"]][1]                # 券商快照现价(≥最新行情日)优先
        last_close[h["code"]] = px
        cost_map[h["code"]] = float(h["cost_price"] or 0)
        shares_map[h["code"]] = float(h["shares"] or 0)
        entry_map[h["code"]] = str(h["entry_date"] or "")
    # 现金真源：调用方未传 cash（--cash 0）时回退读最新 account_snapshot 的现金——
    # 持仓快照 ingest 已把券商现金写入，令快照成为现金唯一真源，免手工维护 ACCOUNT_CASH 变量。
    if cash <= 0:
        try:
            if loop.repo.table_exists("account_snapshot"):
                cs = loop.repo.read_sql(
                    "SELECT cash FROM account_snapshot "
                    "WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM account_snapshot)")
                if not cs.empty and pd.notna(cs["cash"].iloc[0]) and float(cs["cash"].iloc[0]) > 0:
                    cash = float(cs["cash"].iloc[0])
        except Exception:  # noqa: BLE001
            pass

    mv = {c: last_close[c] * shares_map[c] for c in held_codes}
    equity = sum(mv.values()) + max(0.0, cash)
    if equity <= 0:
        equity = 1.0
    cur_w = {c: mv[c] / equity for c in held_codes}

    # ── 目标组合（投顾为主 + 量化补充）──
    pred_date = _latest_pred_date(loop, dt)
    preds = loop.pred_repo.get_predictions(pred_date, loop.cfg.version) if pred_date else pd.DataFrame()
    u = set(loop.uni_repo.get_universe(pred_date, loop.cfg.universe.method)) if pred_date else set()
    if u and not preds.empty:
        preds = preds[preds["code"].isin(u)]
    # 模型质量分位（rank_pct = 全市场因子分位；用作投顾标的的"质量参谋"，显性展示）
    rank_map = (dict(zip(preds["code"], pd.to_numeric(preds["rank_pct"], errors="coerce")))
                if not preds.empty and "rank_pct" in preds.columns else {})
    # 因子层归因（可解释性）：每票 top3 贡献因子（score=Σwᵢfᵢ 分解，见 rulebook）
    tf_map = (dict(zip(preds["code"], preds["top_factors"]))
              if not preds.empty and "top_factors" in preds.columns else {})
    # 收益三来源定位（买前定位赚哪种钱：成长/修复/红利——价投批判篇）
    src_map: dict[str, str] = {}
    try:
        if pred_date:
            from invest_model.repositories.factor_repo import FactorRepository
            expo = FactorRepository(engine).get_exposures_wide(pred_date)
            src_map = _return_sources(expo)
    except Exception:  # noqa: BLE001 — 定位失败不阻断计划
        src_map = {}
    # 模型层置信度：注册表交叉验证 IC（信息系数）→ 该版本因子对未来收益的区分力
    m_ic_mean = m_ic_ir = m_hit = None
    try:
        if loop.repo.table_exists("model_registry"):
            mq = loop.repo.read_sql(
                "SELECT cv_ic_mean, cv_ic_ir, cv_hit_rate FROM model_registry WHERE version=:v",
                {"v": loop.cfg.version})
            if not mq.empty:
                m_ic_mean = _f(mq["cv_ic_mean"].iloc[0])
                m_ic_ir = _f(mq["cv_ic_ir"].iloc[0])
                m_hit = _f(mq["cv_hit_rate"].iloc[0])
    except Exception:  # noqa: BLE001
        pass
    model_trust = _model_trust(m_ic_ir)
    gross = loop.mt.gross_exposure(dt, list(u) if u else None)
    adv_stance, adv_stance_line, adv_mkt_reduce = _advisor_stance(loop, dt)
    targets, meta = loop._build_targets(dt, preds, gross, cur_codes=set(held_codes))
    exit_codes = loop.adv_repo.get_exit_codes(dt)

    # ── 观察池 + 复合买点（手册第1-2步）：研报标的先观察，仅买点触发才建议买入 ──
    # 例外「研报速通」（20260703 数据验证）：research 信号的 α 集中在信号后前几日，
    # 等回踩=逆向选择（103条信号严格闸只放行2%，研报子集立即买均值+12%）。
    # A/B 级研报信号 3 个交易日内免闸半仓直入，余下半仓仍走回踩/突破闸补足；
    # 影子净值落库 policy_shadow 供 4~6 周复核，RESEARCH_FAST_ENTRY=0 一键回退。
    watch_rows: list[dict] = []
    buy_codes: set[str] = set()
    fresh_fast: set[str] = set()
    bps: dict = {}
    if buypoint:
        reco = loop.adv_repo.get_active_reco(dt)
        # 观察池收敛：A 级全留 + B 级取最近的，总量封顶（避免历史 B 级堆积把观察池撑爆）。
        pool: list[str] = []
        if not reco.empty:
            p = reco[(reco["direction"] == "long")
                     & (reco["grade"].isin({"A", "B"}))
                     & (~reco["code"].isin(exit_codes))].copy()
            if "rec_date" in p.columns:
                p = p.sort_values("rec_date", ascending=False)
            p = p.drop_duplicates("code")
            a_codes = p[p["grade"] == "A"]["code"].tolist()
            b_codes = p[p["grade"] == "B"]["code"].tolist()
            pool = a_codes + b_codes[: max(0, WATCH_POOL_CAP - len(a_codes))]
        # P20（owner 拍板 2026-07-17）：投顾大盘 reduce 共振（≥2 行）→ 环境闸收紧
        # min_gross 0.6→0.8。只收紧不放松；恐慌抄底放松分支（fear_buy/fear_min_gross）
        # 不受影响——经过验证的规则优先。ADVISOR_STANCE_GATE=0 一键回退。
        if (adv_stance == "reduce"
                and os.getenv("ADVISOR_STANCE_GATE", "1").lower() not in ("0", "false")):
            from dataclasses import replace as _dc_replace
            bp_cfg = _dc_replace(bp_cfg or BuyPointConfig(), min_gross=0.8)
        bps = detect_buypoints(engine, dt, pool, gross, rank_map, bp_cfg)
        buy_codes = {c for c, bp in bps.items() if bp.is_buy}
        fast_on = os.getenv("RESEARCH_FAST_ENTRY", "1").lower() not in ("0", "false")
        if fast_on and not reco.empty and "source_type" in reco.columns:
            recent = loop.repo.read_sql(
                "SELECT DISTINCT trade_date FROM stock_daily WHERE trade_date<=:d "
                "ORDER BY trade_date DESC LIMIT 3", {"d": dt})
            cut = str(recent["trade_date"].min()) if len(recent) else dt
            rr = reco[(reco["source_type"] == "research")
                      & (reco["grade"].isin({"A", "B"}))
                      & (reco["rec_date"].astype(str) >= cut)]
            fresh_fast = {c for c in rr["code"]
                          if c not in held_codes and c not in exit_codes}
        # 目标里：投顾票未触发买点的 → 移出建议买入、转观察池（持仓的不动，交风控管；
        # 研报速通票不移出——免闸直入，但只给一半目标权重）
        for c in list(targets):
            if (meta.get(c, {}) or {}).get("source") == "advisor" \
                    and c not in buy_codes and c not in held_codes and c not in fresh_fast:
                targets.pop(c, None)
        for c in fresh_fast & set(targets):
            if c not in buy_codes:            # 闸门已确认的给全额，未确认的先半仓
                targets[c] = float(targets[c]) * 0.5
        if persist:
            try:
                _update_policy_shadow(loop, dt, reco, bps)
            except Exception as e:  # noqa: BLE001 - 影子验证失败不阻断计划生成
                logger.warning(f"policy_shadow 更新失败：{e}")
        # 观察池清单（含趋势未过/未现买点等原因），持仓中的/已进目标的（研报速通）不再列观察
        wnames = _name_map(loop, [c for c in pool if c not in held_codes])
        for c in pool:
            if c in buy_codes or c in held_codes or c in targets:
                continue
            bp = bps.get(c)
            g = reco.loc[reco["code"] == c, "grade"].iloc[0] if not reco.empty else None
            ma20 = getattr(bp, "ma20", float("nan")) if bp else float("nan")
            brk = getattr(bp, "breakout", float("nan")) if bp else float("nan")
            trig = (f"回踩≈{ma20} / 突破>{brk}" if np.isfinite(ma20) and np.isfinite(brk) else "—")
            w_reason = bp.reason if bp else "观察"
            if src_map.get(c):
                w_reason = f"{w_reason}｜定位:{src_map[c]}"
            watch_rows.append({
                "plan_date": dt, "code": c, "name": wnames.get(c, ""), "action": "watch",
                "cur_weight": 0.0, "tgt_weight": 0.0, "shares_delta": 0.0,
                "reason": w_reason, "stop_price": None,
                "ref_price": round(bp.last, 2) if bp and np.isfinite(getattr(bp, "last", float("nan"))) else None,
                "grade": g, "trigger": trig, "model_rank": rank_map.get(c),
                "model_view": _model_view(rank_map.get(c), model_trust, tf_map.get(c))})

    # ── 逐票决策 ──
    all_codes = sorted(set(held_codes) | set(targets))
    names = _name_map(loop, all_codes)
    # 目标(非持仓)票补当日收盘价，用于折算股数/参考买入价
    missing_px = [c for c in all_codes if c not in last_close]
    if missing_px:
        ph = ",".join(f":c{i}" for i in range(len(missing_px)))
        params = {f"c{i}": c for i, c in enumerate(missing_px)}
        params["d"] = dt
        pxdf = loop.repo.read_sql(
            f"SELECT code, close FROM stock_daily WHERE trade_date=:d AND code IN ({ph})", params)
        for _, rr in pxdf.iterrows():
            last_close[rr["code"]] = float(pd.to_numeric(rr["close"], errors="coerce"))
    warm = (pd.to_datetime(dt) - pd.Timedelta(days=150)).strftime("%Y%m%d")
    reset_floor = (pd.to_datetime(dt) - pd.Timedelta(days=35)).strftime("%Y%m%d")  # 档位回放窗口下限(≈1个调仓周期)
    # P16 顶部特征自动减半（用户 2026-07-13 定：控回撤证据充分，直升自动减仓）：
    # 浮盈达标持仓 波动骤放大+放量 → 目标减半一次。start_lb 取 ~2 年，够 250 日波动分位。
    top_start_lb = f"{int(dt[:4]) - 2}{dt[4:]}"
    top_trimmed: set[str] = set()                      # 近一个调仓周期内已因顶部特征减半者 → 不重复减
    try:
        _tt = loop.repo.read_sql(
            "SELECT DISTINCT code FROM action_plan WHERE plan_date>=:s AND plan_date<:d "
            "AND reason LIKE :r", {"s": reset_floor, "d": dt, "r": "%顶部特征%"})
        top_trimmed = set(_tt["code"].tolist()) if not _tt.empty else set()
    except Exception:  # noqa: BLE001 — 首日无历史计划不阻断
        top_trimmed = set()
    rows: list[dict] = []
    for c in all_codes:
        cw = cur_w.get(c, 0.0)
        tw = float(targets.get(c, 0.0))
        px = last_close.get(c)
        reason, stop_price = "", float("nan")
        buf_tier = 0
        grade = (meta.get(c, {}) or {}).get("grade")

        # 持仓的风控评估（优先级最高）
        if c in held_codes:
            real_entry = entry_map[c]                         # 真实建仓日（可能为空）
            hist = _close_hist(loop, c, warm, dt)             # 市场窗口：算真实 MA（与建仓日无关）
            cur_day = dt
            if c in snap_px:                                  # 追加当日券商现价 → 风控按最新价判定
                cur_day, snp = snap_px[c]
                # 当日 EOD 已入库则以官方收盘为准（风控是收盘价规则），快照只补 EOD 缺口——
                # 否则同日两行会污染均线（当日双计）、armed_ladder 的“截至昨日”回放
                # （iloc[:-1] 把今日 EOD 当昨日，梯子破位信号被永久吞掉）与时间止损天数。
                if cur_day not in hist.index:
                    hist = pd.concat([hist, pd.Series({cur_day: snp})])
            hold_hist = hist[hist.index >= real_entry] if real_entry else hist.iloc[0:0]  # 自建仓日(供时间止损)
            if not hist.empty and rc.enabled:
                # 移动止盈档位回放起点：真实建仓日 / 最近调仓日 / dt-35天 取最晚，
                # 限定在"当前调仓周期"内单调 → 与回测的每调仓日重置对齐；
                # 避免长持 winner 被几个月前的一次破位永久锁死"破MA20清仓"。
                reset_from = max(x for x in (real_entry, pred_date, reset_floor) if x)
                # P10 感知回放：均线用整段 hist（含 150 日预热）计算、只在窗口内推进档位，
                # 迁移规则与 evaluate_holding 同构 —— 保证「首破减半→之后持有」在实盘
                # 重建 prev_tier 时成立（此前喂窗口切片给 replay_tier：前 19 行 MA20=NaN
                # 记不上档 → 新仓每天重复减半；记上了又是档 3 → 收复 MA20 转盈即被清）。
                prev = replay_hold_tier(hist[hist.index < cur_day], cost_map[c], rc,
                                        replay_from=reset_from)
                buf_tier = prev
                # 硬止损对所有持仓一视同仁（owner 2026-07-17 去掉白名单逻辑）
                dec = evaluate_holding(hist, cost_map[c], rc,
                                       in_exit_codes=(c in exit_codes), prev_tier=prev)
                stop_price = dec.stop_price
                if dec.action == "exit":
                    tw, reason = 0.0, dec.reason
                elif dec.action == "trim":
                    tw, reason = cw * dec.keep_frac, dec.reason
                # 盈利保护（回撤止盈）：浮盈达标后自峰值回撤锁盈——先于时间止损检查。
                # 补 MA20 追踪对高位票「回吐 30%+ 才触发」的缺口（如 巨化 54.8→49.4 无动作）。
                elif entry_map[c] and not hold_hist.empty:
                    pp_prev = replay_pp_tier(hold_hist[hold_hist.index < cur_day],
                                             cost_map[c], rc)
                    ppd = profit_protect(hold_hist, cost_map[c], rc, prev_tier=pp_prev)
                    # 盈利后均线梯子：与峰值回撤并行，先触发者生效（回测：回吐 19.8%→13.3%）
                    lad = armed_ladder(hist, real_entry, cost_map[c], rc)
                    guard = None
                    for cand_dec in (ppd, lad):
                        if cand_dec is None:
                            continue
                        if guard is None or (cand_dec.action == "exit" and guard.action != "exit"):
                            guard = cand_dec
                    if guard is not None:
                        tw = 0.0 if guard.action == "exit" else cw * guard.keep_frac
                        reason = guard.reason
                    else:
                        # 时间止损（手册第3步）：仅在未触发其它风控时检查
                        ts = time_stop(hold_hist, rc, prev_tier=prev)
                        if ts is not None:
                            tw = 0.0 if ts.action == "exit" else cw * ts.keep_frac
                            reason = ts.reason
            if not np.isfinite(stop_price) and cost_map[c] > 0:
                stop_price = cost_map[c] * (1 - rc.hard_stop_pct)

            # P16 顶部特征自动减半：仅在其它风控未触发（reason 为空）、
            # 未在本周期减过时；浮盈达标+波动骤放大+放量 → 目标减半一次（锁盈不砍损）。
            if (rc.enabled and not reason
                    and c not in top_trimmed and cw > 1e-6):
                try:
                    from invest_model.signals.top_feature import top_feature_now
                    tf_close = _close_hist(loop, c, top_start_lb, dt)
                    _vs = loop.repo.read_sql(
                        "SELECT trade_date, volume FROM stock_daily "
                        "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
                        {"c": c, "s": top_start_lb, "d": dt})
                    tf_vol = (pd.to_numeric(_vs.set_index("trade_date")["volume"], errors="coerce")
                              if not _vs.empty else pd.Series(dtype=float))
                    tf_vol.index = tf_vol.index.astype(str)
                    if (not tf_close.empty and top_feature_now(
                            tf_close, tf_vol.reindex(tf_close.index),
                            cost_map.get(c, 0.0), entry_map.get(c) or None)):
                        tw, reason = cw * 0.5, "顶部特征减半（P16·波动骤放大+放量、浮盈达标）"
                except Exception:  # noqa: BLE001 — 顶部信号失败不阻断计划
                    pass

        # 动作判定
        if reason:                                    # 风控已判定（清仓/减仓/时间止损/逻辑证伪）
            action = "sell" if tw <= 1e-6 else "trim"
            # 盈利仓的风控离场本质是止盈（如 巨化 +18% 破MA20 清仓），文案只写
            # "破MA20清仓"曾被误读成止损——展示层加「止盈·」前缀并附浮盈，不改判定。
            # 逻辑证伪除外：那是论点失效离场，与盈亏无关，标"止盈"会误导。
            cost = cost_map.get(c, 0.0)
            if (cost and cost > 0 and px and px > 0 and px / cost - 1 > 0
                    and not reason.startswith("逻辑证伪")):
                reason = f"止盈·{reason}（浮盈{px / cost - 1:+.1%}）"
        elif c in held_codes:
            # 尊重真实持仓、实事求是：风控没触发就持有——不因"没挤进模型 top-N 目标"而强制换出/减配。
            # 换出只保留给风控触发 / 投顾明确剔除（exit_codes 已在风控里判为逻辑证伪清仓）。
            # 展示层区分两种"持有"：破位缓冲期（首破已提示过减半）/ 健康持有。
            if buf_tier >= 1 and np.isfinite(stop_price):
                hold_txt = f"持有观察(已破MA20缓冲·止损{stop_price:.2f}兜底)"
            else:
                hold_txt = "持有"
            action, reason, tw = "hold", hold_txt, cw
        elif cw <= 1e-6 and tw > 1e-6:                 # 非持仓、进入目标 → 新建仓（买点已在观察池闸控）
            action, reason = "buy", _entry_reason(grade, meta.get(c, {}))
            if src_map.get(c):                          # 买前定位赚哪种钱（收益三来源）
                reason = f"{reason}｜定位:{src_map[c]}"
        else:
            action, reason = "hold", "持有"

        if action == "hold" and abs(tw - cw) < min_trade:
            shares_delta = 0.0
        elif action == "buy":
            # 买入按可执行口径定股数：整手/科创板200股起。高价股一手远超目标增量时
            # 判不可执行 → 降级为观察并明说原因，不再输出「—」股数的死指令。
            shares_delta = buy_shares(c, (tw - cw) * equity, px or 0.0)
            if shares_delta <= 0:
                lot = min_lot(c)
                lot_txt = (f"最小一笔{lot}股≈{lot * px:,.0f}元(占{lot * px / equity:.1%})"
                           if px and px > 0 and equity else f"最小一笔{lot}股")
                action, tw = "watch", cw
                reason = f"买点有效但不可执行：{lot_txt} 远超目标增量——账户规模不足，跳过"
        else:
            shares_delta = _round_lot((tw - cw) * equity / px) if px and px > 0 else 0.0
            # 减仓的可执行口径（0722 长川科技实例：持1手却给"减半-100股"＝嘴上减半、
            # 股数清仓）。A股卖出委托同样整手：① 仅1手 → 减半不可拆，如实给二选一，
            # 不硬拆也不静默升级成清仓；② 多手 → 减仓股数夹在[1手, 持仓-1手]，
            # 防四舍五入把减半凑成清仓/凑成0。
            if action == "trim" and px and px > 0:
                held_sh = float(shares_map.get(c, 0) or 0)
                lot = min_lot(c)
                if 0 < held_sh <= lot:
                    shares_delta, tw = 0.0, cw
                    stop_txt = f"{stop_price:.2f}" if np.isfinite(stop_price) else "硬止损"
                    reason = (f"{reason}——⚠️仅持{int(held_sh)}股(最小一手)减半不可拆："
                              f"二选一 ①按纪律全清 ②持有以止损{stop_txt}兜底")
                elif held_sh > lot:
                    want = abs((tw - cw) * equity / px)
                    sd = min(max(lot, _round_lot(want)), _round_lot(held_sh) - lot)
                    shares_delta = -float(sd)

        trigger = (f"挂单≈{round(px, 2)}" if action in ("buy", "add") and px else "—")
        # 信号日涨停的买点触发：按信号日收盘挂单＝次日追涨停（0721 长川科技实例——
        # 计划给"挂单≈涨停价"误导执行，用户自行改挂回踩位才对）。改为挂回踩 MA20 位
        # + 警示，聊天纪律"涨停别追、等回踩"落进计划本身。
        _bp = bps.get(c)
        if action == "buy" and _bp is not None and getattr(_bp, "limit_up", False):
            _m20 = getattr(_bp, "ma20", float("nan"))
            trigger = (f"勿追价·回踩挂单≈{round(float(_m20), 2)}" if np.isfinite(_m20)
                       else "勿追价·等回踩再挂单")
            reason = f"⚠️信号日涨停、次日按收盘追=追涨停：{reason}——仅小仓、挂回踩位、不追价"
        if action == "buy" and c in fresh_fast and c not in buy_codes and px:
            # 研报速通：不追高开（次日平均跳空+3.2%后日内回落），尾盘建半仓
            reason = f"研报速通·半仓直入：{reason}"
            trigger = f"次日尾盘≤{round(px, 2)}建半仓；回踩带补半仓；3日内有效"
        rows.append({
            "plan_date": dt, "code": c, "name": names.get(c, ""),
            "action": action, "cur_weight": round(cw, 4), "tgt_weight": round(tw, 4),
            "shares_delta": shares_delta, "reason": reason,
            "stop_price": round(stop_price, 3) if np.isfinite(stop_price) else None,
            "ref_price": round(px, 3) if px else None, "grade": grade, "trigger": trigger,
            "model_rank": rank_map.get(c),
            "model_view": _model_view(rank_map.get(c), model_trust, tf_map.get(c)),
        })

    # ── 再入场（回测：右尾代价补回 2/3）──
    # 近 45 天内被盈利保护/梯子清仓的票，收盘创出离场以来区间新高 → 半仓接回，
    # 把「止盈下车后主升浪继续」的踏空补回来（002378 场景）。
    if rc.enabled and rc.reentry:
        try:
            back = (pd.to_datetime(dt) - pd.Timedelta(days=45)).strftime("%Y%m%d")
            ex_df = loop.repo.read_sql(
                "SELECT DISTINCT code FROM action_plan WHERE plan_date>=:b AND plan_date<:d "
                # 前缀匹配→包含匹配：reason 现在可能带「止盈·」前缀（见上"动作判定"）
                "AND action='sell' AND reason LIKE :r", {"b": back, "d": dt, "r": "%盈利保护%"})
            re_codes = [c for c in ex_df["code"]
                        if c not in held_codes and c not in targets and c not in exit_codes]
            re_names = _name_map(loop, re_codes)
            half_w = round(0.5 * (float(np.mean(list(targets.values()))) if targets else 0.05), 4)
            for c in re_codes:
                h = _close_hist(loop, c, back, dt)
                if len(h) < 5:
                    continue
                px = float(h.iloc[-1])
                if not (np.isfinite(px) and px > 0 and px >= float(h.iloc[:-1].max())):
                    continue
                re_sd = buy_shares(c, half_w * equity, px)
                if re_sd <= 0:                 # 高价股半仓不足最小一笔 → 不发不可执行指令
                    continue
                rows.append({
                    "plan_date": dt, "code": c, "name": re_names.get(c, ""),
                    "action": "buy", "cur_weight": 0.0, "tgt_weight": half_w,
                    "shares_delta": re_sd,
                    "reason": "创新高确认·半仓再入场（盈利止盈离场后趋势延续）",
                    "stop_price": round(px * (1 - rc.hard_stop_pct), 3),
                    "ref_price": round(px, 3), "grade": None,
                    "trigger": f"挂单≈{round(px, 2)}",
                    "model_rank": rank_map.get(c),
                    "model_view": _model_view(rank_map.get(c), model_trust)})
        except Exception as e:  # noqa: BLE001 - 再入场判定失败不阻断计划生成
            logger.warning(f"再入场判定失败：{e}")

    # ── 账户层 ──
    cost_basis = sum(cost_map[c] * shares_map[c] for c in held_codes)
    unreal = (sum(mv.values()) - cost_basis) / cost_basis if cost_basis > 0 else 0.0

    # 账户级风险提示：执行对账（上一日计划的清仓是否执行）/ 行业与单票集中度 / 仓位 vs 目标
    hints: list[str] = []
    if adv_stance_line:
        gate_note = ""
        if (adv_stance == "reduce"
                and os.getenv("ADVISOR_STANCE_GATE", "1").lower() not in ("0", "false")):
            gate_note = "。大盘看空占多，新买入更谨慎（要仓位更低才开新仓）"
        hints.append(f"投顾观点：{adv_stance_line}{gate_note}")
    # P26 指数贵贱提示行（提示-only，E21 4/4 过判据上线）：沪深300 相对全历史 expanding 中位线位置。
    # 全历史基底用仓内静态 CSV（2005 起），决策日后缺口从 index_daily 补——库内只有 2015 起故必须带基底。
    try:
        _p26 = _hs300_median_hint(loop, dt)
        if _p26:
            hints.append(_p26)
    except Exception:  # noqa: BLE001
        pass
    # P31 卖出分层（E25 双指数过关·2026-08-01）：相对高位强度 H 调卖出节奏，替换旧"上方月卖5%"。
    try:
        _p31 = _p31_sell_hint(loop, dt)
        if _p31:
            hints.append(_p31)
    except Exception:  # noqa: BLE001
        pass
    # P27 指数底仓提示行（owner 2026-07-30 拍板：五层能力圈一二层直接进系统）：
    # 底仓＝沪深300ETF，目标占比 BASE_SLEEVE_TARGET（默认25%）；建仓窗口只认两个已验证信号
    # （P26 中位线下方 / E17 恐慌≥75），上方不追——执行始终由 owner 手动。
    # P27 v2（2026-08-01 owner 拍板）：独立宽基账户四腿窗口状态取代旧"25% 底仓目标"行。
    try:
        _p27 = _broad_legs_hint(loop, dt)
        if _p27:
            hints.append(_p27)
    except Exception:  # noqa: BLE001
        pass
    # 同一份四腿读数落库 broad_leg_state，供 `/invest/broad` 与前端「宽基指数」板块——
    # 提示行与网站因此不可能出现两套数（P58 之后的一贯做法：一处计算、多处消费）。
    try:
        _bst = _broad_leg_states(loop, dt)
        if _bst:
            _persist_broad_leg_state(loop, dt, _bst, shares_map, cost_map, last_close)
    except Exception:  # noqa: BLE001
        pass
    # P51 容错自检行 / P52「不动也是决策」行（2026-08-04 内化审计产物，提示-only）：
    # 不做收益主张、不改仓位，故不走 E 系列超额判据；验收只有"算术可复核 + 零自动交易"。
    try:
        _p51 = _fault_tolerance_hint(loop, dt, cost_map, shares_map, last_close)
        if _p51:
            hints.append(_p51)
    except Exception:  # noqa: BLE001
        pass
    try:
        _p52 = _broad_no_action_hint(loop, dt, shares_map)
        if _p52:
            hints.append(_p52)
    except Exception:  # noqa: BLE001
        pass
    # P28 杠杆窗口提示行（提示-only·平时静默）：三信号取二共振才出现——
    # ①中位线下方≥10% ②距历史峰回撤≥40% ③恐慌≥85；L≤30% 硬顶（E23：50% 已证伪爆仓）。
    try:
        _p28 = _leverage_window_hint(loop, dt)
        if _p28:
            hints.append(_p28)
    except Exception:  # noqa: BLE001
        pass
    # P30 加杠杆信号（AND 共振·低价×恐慌，owner 2026-08-01 拍板上线）：常驻状态行，
    # 触发时🚨🚨每日重复提醒；状态同步落库 leverage_signal 供前端强透出。
    try:
        _p30_st = _and_leverage_state(loop, dt)
        if _p30_st:
            _p30 = _and_leverage_hint(_p30_st)
            if _p30:
                hints.append(_p30)
            _persist_leverage_signal(loop, dt, _p30_st)
    except Exception:  # noqa: BLE001
        pass
    # 参谋异议（提示层）：持仓中模型排位后 20%（rank_pct≤0.2）者单列——风控未触发不强制卖，供人工复核
    try:
        dissent = []
        for hc in held_codes:
            v = rank_map.get(hc)
            if v is not None and pd.notna(v) and float(v) <= 0.20:
                dissent.append(f"{names.get(hc, hc)}(评分排后{float(v) * 100:.0f}%)")
        if dissent:
            hints.append("模型不看好持仓：" + "、".join(dissent)
                         + "——模型评分排全市场后段，但没到风控卖点，先不强制卖、供你复核")
    except Exception:  # noqa: BLE001
        pass
    # 弱股提示（手册第3步·owner 2026-07-22 定·提示-only）：买入后 3 个交易日内未创
    # 买入日收盘价新高＝弱势股。只提示新仓（3~10 个交易日窗口，老仓由均线/止损体系管），
    # 不自动卖——手册原文"主动减仓一半或直接离场"留给人工决策。
    try:
        for hc in held_codes:
            ed = str(entry_map.get(hc) or "").strip()
            if len(ed) != 8 or ed >= dt:
                continue
            h = _close_hist(loop, hc, ed, dt).dropna()
            if len(h) < 4:                       # 买入日 + 至少3个交易日
                continue
            after = h.iloc[1:]
            days = len(after)
            base = float(h.iloc[0])              # 买入日收盘
            if 3 <= days <= 10 and float(after.max()) <= base:
                hints.append(
                    f"弱股提示：{names.get(hc, hc)} 买入后{days}个交易日未创买入日收盘新高"
                    f"（手册口径＝弱势股，宜主动减半或离场——仅提示，不自动卖）")
    except Exception:  # noqa: BLE001 — 提示层失败不阻断计划
        pass
    try:
        prev_plan = loop.repo.read_sql(
            "SELECT code, name, reason FROM action_plan "
            "WHERE plan_date=(SELECT MAX(plan_date) FROM action_plan WHERE plan_date<:d) "
            "AND action='sell'", {"d": dt})
        stale = [str(r["name"] or r["code"]) for _, r in prev_plan.iterrows()
                 if r["code"] in held_codes]
        if stale:
            hints.append(f"待办·还没卖：上次计划让清仓的 {'、'.join(stale)} 还在持仓里，先按纪律卖掉")
    except Exception:  # noqa: BLE001 - 首日无历史计划等情况不阻断
        pass
    try:
        ind_map = loop.industry_map()
        ind_w: dict[str, float] = {}
        for c in held_codes:
            ind_w[ind_map.get(c) or "未知"] = ind_w.get(ind_map.get(c) or "未知", 0.0) + cur_w.get(c, 0.0)
        for ind, w in sorted(ind_w.items(), key=lambda kv: -kv[1]):
            if ind != "未知" and w > 0.35:
                hints.append(f"仓位太集中：{ind}行业占 {w:.0%}（超35%），一个行业跌就整体跟跌，建议分散")
        heavy = [(names.get(c, c), w) for c, w in cur_w.items() if w > 0.20]
        for nm, w in sorted(heavy, key=lambda kv: -kv[1]):
            hints.append(f"单只太重：{nm} 占 {w:.0%}（超20%），建议分批减到 20% 以内")
        # E7 拥挤度（HHI+Top-3 聚合口径）：持仓行业按权重 + 投顾 long 信号池按主题，
        # 补单行业阈值盲区（多个中等行业同题材共振 / 投顾扎堆少数题材同涨同跌）。
        from invest_model.portfolio.crowding import crowding_hints
        adv_cat: list[str] = []
        try:
            ar = loop.adv_repo.get_active_reco(dt)
            if not ar.empty and "catalyst" in ar.columns:
                adv_cat = [str(x) for x in ar.loc[ar["direction"] == "long", "catalyst"]]
        except Exception:  # noqa: BLE001
            adv_cat = []
        hints.extend(crowding_hints(cur_w, ind_map, adv_cat))
    except Exception:  # noqa: BLE001
        pass
    invested = sum(mv.values()) / equity
    if invested - gross > 0.10:
        hints.append(f"仓位偏高：实际 {invested:.0%} 高于目标 {gross:.0%}，先留点现金缓冲、补足前不开新仓")

    # 排雷影子提示（提案 P7）：持仓/目标命中 ≥2 面红旗 → 建议深挖财报（不自动动仓）
    try:
        from invest_model.universe.quality_screen import latest_flags
        qf = latest_flags(engine, dt, list(set(held_codes) | set(targets)))
        for c, (nfl, fls) in sorted(qf.items(), key=lambda kv: -kv[1][0]):
            head = fls[0].split("（")[0] if fls else ""
            hints.append(
                f"财务预警：{names.get(c, c)} 有 {nfl} 项异常（{head} 等），可能财报有水分，"
                f"建议自己核对财报——只是提醒、不自动因此卖出")
    except Exception:  # noqa: BLE001 — 影子提示失败不阻断计划
        pass

    # 戴维斯双杀预警（财报#1 快报时效层）：近 7 日新披露快报/预告显示增速失速
    try:
        hints.extend(_express_alerts(loop, dt, list(set(held_codes) | set(targets)), names))
    except Exception:  # noqa: BLE001
        pass

    # 顶部特征追加提示（P16 自动减半已在动作层执行）：对本周期已减半、但顶部特征仍在的持仓，
    # 提示可考虑进一步兑现（避免重复自动减仓，改由人工判断二次动作）。新触发已成 trim 动作行，不在此列。
    try:
        from invest_model.signals.top_feature import top_feature_now
        start_lb = f"{int(dt[:4]) - 2}{dt[4:]}"          # 约 2 年回看，够 250 日波动分位
        still_top: list[str] = []
        for c in held_codes:
            if c not in top_trimmed:                      # 仅看"已减半"的；新触发走动作行
                continue
            close = _close_hist(loop, c, start_lb, dt)
            if close.empty:
                continue
            vser = loop.repo.read_sql(
                "SELECT trade_date, volume FROM stock_daily "
                "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
                {"c": c, "s": start_lb, "d": dt})
            vol = (pd.to_numeric(vser.set_index("trade_date")["volume"], errors="coerce")
                   if not vser.empty else pd.Series(dtype=float))
            vol.index = vol.index.astype(str)
            if top_feature_now(close, vol.reindex(close.index), cost_map.get(c, 0.0),
                               entry_map.get(c) or None):
                still_top.append(names.get(c, c))
        if still_top:
            hints.append(
                f"顶部特征仍在（本周期已自动减半）：{'、'.join(still_top)}——"
                f"顶部风险未消，可人工考虑进一步兑现（P16，见 model_change_proposals）")
    except Exception:  # noqa: BLE001 — 顶部提示失败不阻断计划
        pass

    # ── 套利统一资金账本（一体两面）：单一资金池按 A/B/α 分配、强制零杠杆 ──
    # ARB_ENABLED=0（默认观察态）：不发 arb 行、不缩放引擎 B → 计划与今天逐字一致。
    arb_rows: list[dict] = []
    ledger_extra: dict = {}
    try:
        from invest_model.arb.config import ArbConfig
        from invest_model.arb.ledger import build_arb_plan
        acfg = ArbConfig.from_env()
        fear_score = None
        try:
            from invest_model.signals.fear import fear_gauge
            fear_score = fear_gauge(engine, dt).get("score")
        except Exception:  # noqa: BLE001
            fear_score = None
        lg = build_arb_plan(engine, dt, acfg, equity, gross, fear_score, held_codes)
        ledger_extra = lg["account_extra"]
        if lg.get("viol_hint"):
            hints.append(lg["viol_hint"])
        # 引擎 B 行统一标 sleeve；启用态按 offense_scale 缩进 offense 预算
        oscale = lg.get("offense_scale", 1.0)
        for r in rows:
            r.setdefault("sleeve", "offense_B")
            if acfg.enabled and oscale != 1.0 and r.get("action") in ("buy", "add", "hold"):
                r["tgt_weight"] = round(float(r.get("tgt_weight") or 0.0) * oscale, 4)
                if r.get("shares_delta"):
                    r["shares_delta"] = _round_lot(float(r["shares_delta"]) * oscale)
        for r in watch_rows:
            r.setdefault("sleeve", "offense_B")
        arb_rows = lg.get("arb_rows", [])
        # sleeve_target 落库（观察态也写，看板可见）
        if persist and lg.get("sleeve_rows"):
            try:
                from invest_model.repositories.arb_repo import LedgerRepo
                sr = pd.DataFrame(lg["sleeve_rows"])
                LedgerRepo(engine).save(sr)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"sleeve_target 落库失败（不阻断）：{e}")
    except Exception as e:  # noqa: BLE001 - 套利账本失败绝不阻断主计划
        logger.warning(f"套利资金账本构建失败（跳过，回退纯引擎B）：{e}")

    account = {
        "plan_date": dt, "equity": round(equity, 2),
        "invested_pct": round(sum(mv.values()) / equity, 4),
        "cash_pct": round(max(0.0, cash) / equity, 4),
        "n_holdings": len(held_codes),
        "unrealized_pnl_pct": round(unreal, 4),
        "gross_target": round(gross, 4),
        # 注：实盘缺账户峰值，用「持仓整体浮亏」近似账户级回撤(rc.account_dd_stop)风控提示
        "risk_off": bool(rc.enabled and rc.account_dd_stop and unreal <= -rc.account_dd_stop),
        "model_ic_mean": m_ic_mean, "model_ic_ir": m_ic_ir, "model_hit": m_hit,
        "model_conf_label": _conf_label(model_trust, m_ic_ir),
        "risk_hints": " | ".join(hints) if hints else None,
        **ledger_extra,
    }

    rows = rows + arb_rows + watch_rows
    try:
        etf_watch = _etf_watch_rows(loop, dt, set(held_codes))
    except Exception:  # noqa: BLE001 — ETF 观察段失败不阻断计划
        etf_watch = []
    plan = ActionPlan(plan_date=dt, rows=rows, account=account, etf_watch=etf_watch,
                      footer=_build_data_footer(loop, dt))
    if persist and rows:
        cols = ["plan_date", "code", "name", "action", "cur_weight", "tgt_weight",
                "shares_delta", "reason", "stop_price", "ref_price", "grade",
                "trigger_hint", "model_rank", "model_view", "sleeve"]
        df = pd.DataFrame(rows)
        if "sleeve" not in df.columns:
            df["sleeve"] = "offense_B"
        df["sleeve"] = df["sleeve"].fillna("offense_B")
        df["trigger_hint"] = df["trigger"]  # trigger 为 MySQL 保留字，落库改名
        loop.repo.upsert("action_plan", df[cols], ["plan_date", "code"])
        try:
            acct = {**account, "risk_off": int(account["risk_off"])}
            loop.repo.upsert("action_plan_account", pd.DataFrame([acct]), ["plan_date"])
        except Exception as e:  # noqa: BLE001 - 账户元数据落库失败不阻断计划生成
            print(f"WARN action_plan_account 落库失败：{e}")
    return plan


def _entry_reason(grade, meta: dict) -> str:
    src = (meta or {}).get("source")
    if src == "advisor" and grade:
        return f"投顾{grade}级推荐"
    return "量化补充" if src == "quant" else "目标加配"


def _return_sources(expo: pd.DataFrame) -> dict[str, str]:
    """收益三来源定位（买前定位赚哪种钱——业绩成长/估值修复/分红）。

    出处：价投批判篇（收益三来源框架）。由因子暴露（截面 zscore）推断：
    成长=盈利增速高分位；修复=便宜（EP/BP 高）且增速不高；红利=股息率高分位。
    影子候选 dividend_yield 无数据时红利档自然缺省。
    """
    if expo is None or expo.empty:
        return {}
    z = lambda col: pd.to_numeric(expo.get(col, pd.Series(np.nan, index=expo.index)),  # noqa: E731
                                  errors="coerce")
    growth = z("profit_yoy")
    cheap = pd.concat([z("ep"), z("bp")], axis=1).max(axis=1)
    div = z("dividend_yield")
    out: dict[str, str] = {}
    for c in expo.index:
        g, ch, dv = growth.get(c), cheap.get(c), div.get(c)
        if pd.notna(dv) and dv >= 1.0 and (pd.isna(g) or g < 0.5):
            out[str(c)] = "红利"
        elif pd.notna(g) and g >= 0.5:
            out[str(c)] = "成长"
        elif pd.notna(ch) and ch >= 0.5:
            out[str(c)] = "修复"
    return out


def _express_alerts(loop: ClosedLoop, dt: str, codes: list[str],
                    names: dict[str, str], back_days: int = 7,
                    drop_pp: float = 20.0) -> list[str]:
    """戴维斯双杀预警：近 N 日新披露的业绩快报/预告显示净利增速转负或骤降。

    出处：财报#1（快报是最后逃生窗口；增速失速→成长股 PE 重估跌 78%，
    验证 growth-deceleration-davis-killer）。口径：快报/预告的累计净利同比 vs
    最近定期报告的累计同比，转负或降幅 > drop_pp 即预警。只提示不动仓。
    """
    if not codes or not loop.repo.table_exists("fina_express"):
        return []
    back = (pd.to_datetime(dt) - pd.Timedelta(days=back_days)).strftime("%Y%m%d")
    ex = loop.repo.read_sql(
        "SELECT code, ann_date, report_date, kind, profit_yoy FROM fina_express "
        "WHERE ann_date>:b AND ann_date<=:d", {"b": back, "d": dt})
    if ex.empty:
        return []
    ex = ex[ex["code"].isin(set(codes))]
    if ex.empty:
        return []
    ex["profit_yoy"] = pd.to_numeric(ex["profit_yoy"], errors="coerce")
    ex = ex.dropna(subset=["profit_yoy"]).sort_values("ann_date").groupby("code").tail(1)
    # 对照基准：该票最近一期定期报告的累计净利同比
    stale = (pd.to_datetime(dt) - pd.Timedelta(days=540)).strftime("%Y%m%d")
    fi = loop.repo.read_sql(
        "SELECT code, ann_date, report_date, profit_yoy FROM stock_fina_indicator "
        "WHERE ann_date<=:d AND ann_date>=:lo", {"d": dt, "lo": stale})
    base: dict[str, float] = {}
    if not fi.empty:
        fi = fi[fi["code"].isin(set(ex["code"]))]
        fi = fi.sort_values(["code", "ann_date", "report_date"]).groupby("code").tail(1)
        base = {str(r["code"]): float(pd.to_numeric(r["profit_yoy"], errors="coerce"))
                for _, r in fi.iterrows() if pd.notna(pd.to_numeric(r["profit_yoy"], errors="coerce"))}
    kind_cn = {"express": "快报", "forecast": "预告"}
    alerts: list[str] = []
    for _, r in ex.iterrows():
        c, now = str(r["code"]), float(r["profit_yoy"])
        prev = base.get(c)
        slump = now < 0 or (prev is not None and prev - now > drop_pp)
        if not slump:
            continue
        prev_s = f"（上期 {prev:+.0f}%）" if prev is not None else ""
        alerts.append(
            f"戴维斯双杀预警: {names.get(c, c)} {kind_cn.get(str(r['kind']), '快报')}净利同比 "
            f"{now:+.0f}%{prev_s}——增速{'转负' if now < 0 else '骤降'}，成长股第一时间重估"
            f"（快报是最后逃生窗口，仅提示不自动动仓）")
    return alerts


_ACTION_CN = {"buy": "买入", "add": "加仓", "trim": "减仓", "sell": "清仓",
              "hold": "持有", "watch": "观察"}


def _f(x):
    """安全转 float；非数/NaN 返回 None。"""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if np.isfinite(v) else None


def _model_trust(ic_ir) -> float:
    """模型层置信度(0..1)：由交叉验证 IC_IR 映射。IC_IR≥0.6≈满信任，≤0 视为失效。"""
    v = _f(ic_ir)
    if v is None:
        return 0.0
    return float(min(1.0, max(0.0, v / 0.6)))


def _conf_label(trust: float, ic_ir) -> str:
    v = _f(ic_ir)
    if v is None:
        return "无（模型未就绪）"
    if v <= 0:
        return "失效（IC≤0，勿依赖）"
    return "高" if trust >= 0.66 else ("中" if trust >= 0.33 else "低")


def _model_verdict(mr: float) -> str:
    if mr >= 0.85:
        return "看多"
    if mr >= 0.65:
        return "偏多"
    if mr >= 0.45:
        return "中性"
    if mr >= 0.25:
        return "偏空"
    return "看空"


def _model_view(mr, trust: float, top_factors: str | None = None) -> str:
    """单票模型研判：方向(看多/偏多/中性/偏空/看空) + 全市场分位 + 置信★(决断度×模型信任)
    + 因子归因（top3 贡献因子，如 ep↑ mom60↑——决策可解释，出处见 rulebook）。"""
    v = _f(mr)
    if v is None:
        return "—"                                # 无模型覆盖（如 ETF）
    top = (1.0 - v) * 100.0
    conviction = abs(v - 0.5) * 2.0               # 分位越极端越决断(0..1)
    c = conviction * trust
    stars = "★★★" if c >= 0.55 else ("★★" if c >= 0.28 else "★")
    base = f"{_model_verdict(v)} 前{top:.0f}% {stars}"
    attr = _fmt_attr(top_factors)
    return f"{base} · {attr}" if attr else base


# 因子代码 → 中文（面向用户展示；↑=该维度抬升排名，↓=拖累排名）
_FACTOR_CN = {
    "ep": "低PE", "bp": "低PB", "sp": "低PS",
    "roe": "净资产收益", "roa": "总资产收益", "gross_margin": "毛利率",
    "rev_yoy": "营收增速", "profit_yoy": "利润增速",
    "mom_60": "中期动量", "mom_120": "长期动量", "reversal_5": "短期反转",
    "lowvol_20": "低波动", "small_size": "小市值", "low_turnover": "低换手",
    "nb_ratio_chg_20": "北向加仓", "adv_stance": "投顾立场",
    "growth_accel": "增速加速", "bp_ex_goodwill": "扣商誉低PB",
    "dividend_yield": "股息率", "insider_conviction": "高管增持",
}


def _fmt_attr(top_factors) -> str:
    """"ep+0.82|mom_60+1.15" → "低PE↑、中期动量↑"（中文因子名+推拉方向）。"""
    if not top_factors or not isinstance(top_factors, str):
        return ""
    parts = []
    for seg in top_factors.split("|")[:3]:
        seg = seg.strip()
        i = max(seg.rfind("+"), seg.rfind("-"))
        if i <= 0:
            continue
        name = _FACTOR_CN.get(seg[:i], seg[:i])
        parts.append(f"{name}{'↑' if seg[i] == '+' else '↓'}")
    return "、".join(parts)


def _table(lines: list[str], rows: list[dict]) -> None:
    lines.append("| 代码 | 名称 | 动作 | 现权重→目标 | 约股数 | 买点/挂单价 | 理由 | 止损价 | 现价 | 分级 | 模型研判 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        sd = int(r["shares_delta"])
        sd_s = f"+{sd}" if sd > 0 else (str(sd) if sd < 0 else "—")
        lines.append(
            f"| {r['code']} | {r['name']} | {_ACTION_CN.get(r['action'], r['action'])} | "
            f"{r['cur_weight']:.1%}→{r['tgt_weight']:.1%} | {sd_s} | {r.get('trigger', '—')} | {r['reason']} | "
            f"{r['stop_price'] if r['stop_price'] is not None else '—'} | "
            f"{r['ref_price'] if r['ref_price'] is not None else '—'} | {r['grade'] or '—'} | "
            f"{r.get('model_view', '—')} |")


def render_markdown(plan: ActionPlan) -> str:
    a = plan.account
    lines = [f"# 操作计划 — {plan.plan_date}", ""]
    lines.append(
        f"- 总权益: {a.get('equity')} | 持仓占比: {a.get('invested_pct', 0):.0%} | "
        f"现金占比: {a.get('cash_pct', 0):.0%} | 目标仓位: {a.get('gross_target', 0):.0%}")
    lines.append(
        f"- 持仓数: {a.get('n_holdings')} | 整体浮盈亏: {a.get('unrealized_pnl_pct', 0):+.1%} | "
        f"账户风控: {'⚠️ 账户回撤超限，建议降仓' if a.get('risk_off') else '正常'}")
    mir = a.get("model_ic_ir")
    if mir is not None:
        lines.append(f"- 🔬 模型置信度: **{a.get('model_conf_label')}**（★越多越可信）")
    lines.append("- 标的由投顾定，模型只做参谋+时机+风控；名词解释与规则出处见 `docs/rulebook.md`。")
    # 提示行（投顾风向/参谋异议/集中度/清仓未执行等）——此前只落库(action_plan_account.risk_hints)
    # 供网站读、却漏渲染进 issue 计划；2026-07-20 修复：逐条随计划头输出，issue 与网站口径一致。
    for _h in (a.get("risk_hints") or "").split(" | "):
        if _h.strip():
            lines.append(f"- 📌 {_h.strip()}")

    # 套利 sleeve 行（defense_A/alpha）单列，不混入引擎 B 的买入/持仓/观察
    arb = [r for r in plan.rows if r.get("sleeve") in ("defense_A", "alpha")]
    core = [r for r in plan.rows if r.get("sleeve") not in ("defense_A", "alpha")]
    held = [r for r in core if r["cur_weight"] > 1e-6]
    buys = [r for r in core if r["action"] in ("buy", "add") and r["cur_weight"] <= 1e-6]
    watch = [r for r in core if r["action"] == "watch"]
    held.sort(key=lambda r: ({"sell": 0, "trim": 1, "hold": 2}.get(r["action"], 3), -r["cur_weight"]))
    buys.sort(key=lambda r: -r["tgt_weight"])
    watch.sort(key=lambda r: (r["grade"] or "Z", r["code"]))

    lines += ["", f"## 一、建议买入（买点触发，共 {len(buys)} 只）"]
    if buys:
        _table(lines, buys)
    else:
        lines.append("（今日无买点触发——观察池均在等待回踩/突破信号）")
    lines += ["", f"## 二、当前持仓·风控动作（{len(held)} 只）"]
    _table(lines, held) if held else lines.append("（无持仓）")
    lines += ["", f"## 三、观察池·等买点（{len(watch)} 只，方向确认、待时机）"]
    if watch:
        _table(lines, watch)
    else:
        lines.append("（观察池为空）")
    if plan.etf_watch:
        lines += ["", f"## 四、ETF 观察·趋势/买点位（{len(plan.etf_watch)} 只，watch_etf.txt）",
                  "| 代码 | 最新价 | MA20 | 距MA20 | 趋势 | 状态 | 备注 |",
                  "|---|---:|---:|---:|---|---|---|"]
        for e in plan.etf_watch:
            last = f"{e['last']:.3f}" if e["last"] == e["last"] else "—"
            ma20 = f"{e['ma20']:.3f}" if e["ma20"] == e["ma20"] else "—"
            dev = f"{e['dev']:+.1%}" if e["dev"] == e["dev"] else "—"
            note = (e["note"][:28] + "…") if len(e["note"]) > 29 else e["note"]
            lines.append(f"| {e['code']} | {last} | {ma20} | {dev} | {e['trend']} "
                         f"| {e['state']} | {note} |")
    _render_ledger(lines, a, arb)
    if plan.footer:
        lines += ["", plan.footer]
    return "\n".join(lines)


def _render_ledger(lines: list[str], a: dict, arb_rows: list[dict]) -> None:
    """套利统一资金账本段。观察态（ARB_ENABLED=0，无 arb 行）不渲染——
    面向用户的计划只在账本真正启用（E14 过关+签核）后才出现该段，避免噪音。"""
    if not arb_rows:
        return
    lines += ["", "## 四、套利/守恒 sleeve 账本"]
    dpct = a.get("defense_pct"); opct = a.get("offense_pct"); apct = a.get("alpha_pct")
    if opct is not None:
        ok = "✅零杠杆" if a.get("ledger_ok") else "⚠️超100%已收缩"
        fear = a.get("fear_score")
        fear_s = f" | 恐慌指数 {fear:.0f}{'（恐慌弹药↑进攻/α）' if fear and fear >= 75 else ''}" if fear is not None else ""
        lines.append(
            f"- 资金池分配：防守A **{(dpct or 0):.0%}** / 进攻B **{(opct or 0):.0%}** / "
            f"盲区α **{(apct or 0):.0%}** / 现金 **{max(0.0, 1-(dpct or 0)-(opct or 0)-(apct or 0)):.0%}** "
            f"（{ok}，Σ={a.get('sleeve_gross', 0):.0%}）{fear_s}")
        ce = a.get("carry_expected")
        if ce:
            lines.append(f"- 防守底盘预期 carry（加权年化）：约 {ce:.2%}")
        lines.append("- 红线：全程自有资金·零杠杆；α 小仓位对赔率、单笔亏得起；跟水不跟价（逻辑止损）。")
    lines += ["", "| sleeve | 标的 | 动作 | 目标权重 | 参考价 | 逻辑 |",
              "|---|---|---|---:|---:|---|"]
    for r in sorted(arb_rows, key=lambda x: (x.get("sleeve", ""), -float(x.get("tgt_weight") or 0))):
        lines.append(
            f"| {r.get('sleeve')} | {r.get('name')}({r.get('code')}) | {r.get('action')} "
            f"| {float(r.get('tgt_weight') or 0):.1%} | {r.get('ref_price', '—')} | {r.get('reason', '')} |")
