"""FaaS 定时任务实现——原 GitHub Actions 定时 workflow 的等价迁移。

每个 job 都是无状态单次执行：读写生产 MySQL，结果以 GitHub Issue 评论推送（→邮件），
与原 workflow 行为一一对应：

  job_live_watch            ← live-watch.yml     盘中盯盘（每 3 分钟无状态扫描）
  job_snapshot_remind       ← snapshot-remind.yml 持仓快照提醒（交易日 15:20）
  job_ingest_etf            ← ingest-etf.yml     ETF 前复权日线入库（交易日 16:50）
  job_daily_update_plan     ← data-update.yml(update档) + plan-notify.yml
                              增量更新后链式出计划（17:00 起，天然保证先数据后计划）
  job_weekly_rebuild_review ← data-update.yml(周六all档) + P4影子回测 + review.yml
                              周六全量重建 → 影子回测（不阻断）→ 复盘推送

可写目录只有 /tmp（FC 代码目录只读），所有 --out 都落 /tmp。
环境变量见 faas/scheduler_handler.py 模块注释。
"""

from __future__ import annotations

import os
import shlex
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from faas import gh_notify  # noqa: E402

# 与原 workflow 相同的默认参数（可用环境变量覆盖）
_PIPELINE_START = os.getenv("PIPELINE_START", "20250101")
_ETF_START = os.getenv("ETF_START", "20230101")
_DEFAULT_PLAN_ARGS = ("--advisor-led --risk --trend-filter "
                      "--concentration medium --time-stop-days 8")


def _run_cli(main_func, argv: list[str]) -> None:
    """以指定 argv 调用脚本的 main()（脚本都是 argparse CLI，不改脚本本身）。"""
    old_argv = sys.argv
    sys.argv = argv
    try:
        main_func()
    finally:
        sys.argv = old_argv


def _is_trade_day(day: str) -> bool:
    """trade_calendar 守卫（同 snapshot-remind.yml 内联 python）：查询失败按交易日处理。"""
    try:
        from invest_model.data import make_engine
        from invest_model.repositories.base import BaseRepository
        repo = BaseRepository(make_engine())
        if repo.table_exists("trade_calendar"):
            df = repo.read_sql(
                "SELECT is_open FROM trade_calendar WHERE cal_date=:d", {"d": day})
            if not df.empty:
                return int(df["is_open"].iloc[0]) == 1
    except Exception as e:  # noqa: BLE001
        print(f"trade_calendar 守卫查询失败，按交易日处理：{e}")
    return True


# ── 盯盘（live-watch）────────────────────────────────────────────────

def job_live_watch() -> dict:
    """盘中盯盘单次无状态扫描；时段/节假日守卫在脚本内，毫秒级退出。"""
    from scripts.live_check import run_once
    return run_once()


# ── 持仓快照提醒（snapshot-remind）───────────────────────────────────

def job_snapshot_remind() -> dict:
    day = gh_notify.bj_now().strftime("%Y%m%d")
    if not _is_trade_day(day):
        return {"job": "snapshot_remind", "skipped": "non-trading-day", "day": day}
    res = gh_notify.post_issue_comment(
        "📸 持仓快照提醒",
        seed_body="本 issue 每交易日收盘后提醒上传当天券商持仓截图（快照入库用）。",
        comment_body=(f"## {day} 收盘\n\n"
                      "📸 **请把今天的券商「持仓+现金」截图发到会话**，我转成快照入库。\n"
                      "赶在 **17:00 的盘后操作计划**前完成，计划的风控段就用上你今天的真实持仓。"),
        dedupe_prefix=f"## {day} 收盘",
    )
    return {"job": "snapshot_remind", "day": day, **res}


# ── ETF 日线入库（ingest-etf）────────────────────────────────────────

def job_ingest_etf() -> dict:
    from scripts.ingest_etf_daily import main as etf_main
    _run_cli(etf_main, ["ingest_etf_daily.py", "--start", _ETF_START])
    return {"job": "ingest_etf", "ok": True}


# ── 盘后增量更新 + 操作计划（data-update + plan-notify 链）──────────────

def _latest_snapshot_cash() -> float:
    """最新 account_snapshot 的现金（best-effort：异常/无数据回 0，不阻断出计划）。"""
    try:
        from invest_model.data import make_engine
        from invest_model.repositories.base import BaseRepository
        repo = BaseRepository(make_engine())
        df = repo.read_sql(
            "SELECT cash FROM account_snapshot ORDER BY snapshot_date DESC LIMIT 1")
        if not df.empty and df["cash"].iloc[0] is not None:
            return float(df["cash"].iloc[0])
    except Exception as e:  # noqa: BLE001
        print(f"WARN 读取 account_snapshot 现金失败，回退 0：{e}")
    return 0.0


def _build_and_post_plan() -> dict:
    """生成三段式操作计划并推送（等价 plan-notify.yml 的两个步骤）。"""
    bj = gh_notify.bj_now()
    if bj.weekday() >= 5:  # 周末不发计划（同原 github-script 守卫）
        return {"plan": "skipped-weekend"}
    plan_args = shlex.split(os.getenv("PLAN_ARGS") or _DEFAULT_PLAN_ARGS)
    # 现金：ACCOUNT_CASH 环境变量优先（现网行为不变）；未配置时读最新
    # account_snapshot（此前默认 0 → 权益分母漏现金、买入折股偏差）
    env_cash = os.getenv("ACCOUNT_CASH")
    cash = env_cash if env_cash not in (None, "") else str(_latest_snapshot_cash())
    out = "/tmp/action_plan.md"
    from scripts.build_action_plan import main as plan_main
    _run_cli(plan_main,
             ["build_action_plan.py", *plan_args, "--cash", cash, "--out", out])
    with open(out, encoding="utf-8") as f:
        body = f.read()
    today = bj.strftime("%Y-%m-%d")
    res = gh_notify.post_issue_comment(
        "📈 每日操作计划",
        seed_body="本 issue 由 FC 定时函数每个交易日盘后追加操作计划评论。",
        comment_body=f"## {today} 盘后操作计划\n\n{body}",
        dedupe_prefix=f"## {today} 盘后操作计划",
    )
    return {"plan": res}


def _persist_fear_daily() -> str:
    """恐慌指数按日落库（仪表盘历史曲线）；失败不阻断出计划。

    改为**显式告警**：原来异常只 print WARN 到 FC 日志（没人看），导致「仪表盘恐慌值
    一直不变」无从排查。现在两类根因都推送到「⚠️ FaaS 定时任务告警」issue（→邮件）：
      ① 落库/计算异常；② 恐慌值未推进到新交易日（stock_daily 未更新，仪表盘看着卡住）。
    """
    try:
        # 导入放进 try：万一打包漏带 scripts/fear_gauge（历史踩过）也只降级返回 WARN，
        # 绝不让 ImportError 逃逸中止整个 daily job、连累后面的账户快照落库。
        from invest_model.data import make_engine
        from invest_model.repositories.base import BaseRepository
        from invest_model.signals.fear import fear_gauge
        from scripts.fear_gauge import persist_fear
        engine = make_engine()
        repo = BaseRepository(engine)
        prev = repo.read_sql("SELECT MAX(trade_date) d FROM fear_daily")["d"].iloc[0]
        g = fear_gauge(engine)
        persist_fear(engine, g)
        cur = str(g.get("date"))
        if prev is not None and str(prev) == cur:
            # 行情未推进到新交易日 → fear_daily 仍是同一天，仪表盘看起来「卡住」。
            gh_notify.alert("fear_daily", RuntimeError(
                f"恐慌指数未推进：最新交易日仍为 {cur}（stock_daily 未更新到新交易日？"
                f"请检查 daily_update_plan 的数据更新步骤是否成功）"))
            return f"stale:{cur}"
        return f"ok:{cur}"
    except Exception as e:  # noqa: BLE001
        print(f"WARN fear_daily 落库失败：{e}")
        try:
            gh_notify.alert("fear_daily", e)
        except Exception:  # noqa: BLE001 — 告警本身失败不再级联
            pass
        return f"WARN: {e}"


def _ingest_and_build_arb() -> str:
    """套利：自动算三水表 + 可选 α CSV 录入 + 构建 flow_score（best-effort，不阻断出计划）。

    三水表默认走市场资金流自动构建（build_watermeter_auto，北向+两融按行业）；
    config/watermeter_*.csv 仅作可选人工覆盖（模板文件跳过）。盲区 α 仍走 curated CSV。
    """
    try:
        import glob

        from invest_model.arb.watermeter import build_flow_scores
        from invest_model.arb.watermeter_auto import build_watermeter_auto
        from invest_model.data import make_engine
        from scripts.ingest_watermeter import ingest_alpha, ingest_watermeter
        import pandas as pd

        engine = make_engine()
        root = _ROOT
        from invest_model.repositories.base import BaseRepository
        d = BaseRepository(engine).read_sql("SELECT MAX(trade_date) m FROM stock_daily")
        end = str(d["m"].iloc[0]) if not d.empty and d["m"].iloc[0] else None

        # 三水表：市场资金流自动构建（主路径）
        n_auto = build_watermeter_auto(engine, end) if end else 0
        # 可选人工覆盖（非模板 CSV；默认没有则跳过）
        n_wm = n_a = 0
        for f in sorted(glob.glob(os.path.join(root, "config", "watermeter_*.csv"))):
            if "template" in f:
                continue
            try:
                df = pd.read_csv(f, dtype=str).where(lambda x: x.notna(), None)
                n_wm += ingest_watermeter(engine, df)
            except Exception as e:  # noqa: BLE001
                print(f"WARN 水表 {f} 录入跳过：{e}")
        for f in sorted(glob.glob(os.path.join(root, "config", "alpha_*.csv"))):
            if "template" in f:
                continue
            try:
                df = pd.read_csv(f, dtype=str).where(lambda x: x.notna(), None)
                n_a += ingest_alpha(engine, df)
            except Exception as e:  # noqa: BLE001
                print(f"WARN α {f} 录入跳过：{e}")
        if end:
            build_flow_scores(engine, end)
        return f"ok(auto_wm={n_auto},csv_wm={n_wm},alpha={n_a})"
    except Exception as e:  # noqa: BLE001
        print(f"WARN 套利信号构建失败（不阻断）：{e}")
        return f"err:{repr(e)[:60]}"


def job_daily_update_plan() -> dict:
    """增量数据更新后链式出计划。

    更新失败时（数据源不可用/IP 超限/Tushare 抖动等）**不出计划**（避免旧数据误导），
    但仍按库内已有数据重估账户快照 + 落恐慌——这两步只读 stock_daily 收盘与 current_holding、
    不依赖 Tushare，故数据源挂掉时账户总资产/净值曲线仍每日推进（符合「没有外部触发也每天
    按当天已有收盘×持仓自算」）。更新 CLI 内部失败已自行推送 issue 告警，这里不重复告警。
    """
    from scripts.run_pipeline import main as pipe_main
    update_ok = True
    try:
        _run_cli(pipe_main, ["run_pipeline.py", "--mode", "update",
                             "--start", _PIPELINE_START])
    except BaseException as e:  # noqa: BLE001 — 含 SystemExit(1)（数据源不可用/IP 超限）
        update_ok = False
        print(f"WARN 数据更新失败（仍按已有数据重估快照/恐慌，本日跳过出计划）：{e}")

    # 不依赖数据源、只读库内数据的两步——无论更新成败都要跑，保证快照每日推进
    fear = _persist_fear_daily()
    acct = _persist_account_snapshot_daily()

    if not update_ok:
        return {"job": "daily_update_plan", "update": "failed",
                "fear": fear, "account": acct, "plan": "skipped:update-failed"}

    sc = _build_signal_scorecard()
    arb = _ingest_and_build_arb()
    return {"job": "daily_update_plan", "update": "ok", "fear": fear, "account": acct,
            "scorecard": sc, "arb": arb, **_build_and_post_plan()}


def _holding_market_value_at(repo, ch, codes: list[str], dt: str) -> float:
    """按 dt 收盘价重估 current_holding 的 stock+etf 市值（不含转债/现金）。
    停牌/缺 dt 收盘 → 回退 dt 及之前最近有效收盘；再回退最近快照 last_price
    （此前直接按 0 估值，总资产当日凭空缩水）。dt 作时间上限保证 point-in-time。
    """
    ph = ",".join(f":c{i}" for i in range(len(codes)))
    params = {f"c{i}": c for i, c in enumerate(codes)}
    params["d"] = str(dt)
    px = repo.read_sql(
        f"SELECT code, close FROM stock_daily WHERE trade_date=:d AND code IN ({ph})", params)
    pxmap = {str(r["code"]): float(r["close"]) for _, r in px.iterrows()
             if r["close"] is not None}
    for c in codes:                       # 停牌/缺当日收盘 → dt 及之前最近有效收盘
        if c in pxmap:
            continue
        fb = repo.read_sql(
            "SELECT close FROM stock_daily WHERE code=:c AND trade_date<=:d "
            "AND close IS NOT NULL ORDER BY trade_date DESC LIMIT 1",
            {"c": c, "d": str(dt)})
        if not fb.empty and fb["close"].iloc[0] is not None:
            pxmap[c] = float(fb["close"].iloc[0])
            continue
        fb = repo.read_sql(                # 再回退最近快照的券商现价
            "SELECT last_price FROM holding_snapshot WHERE code=:c "
            "AND last_price IS NOT NULL ORDER BY snapshot_date DESC LIMIT 1", {"c": c})
        if not fb.empty and fb["last_price"].iloc[0] is not None:
            pxmap[c] = float(fb["last_price"].iloc[0])
        else:
            print(f"WARN account_snapshot 重估：{c}@{dt} 无任何可用价格，按 0 计入")
    mv = 0.0
    for _, r in ch.iterrows():
        p = pxmap.get(str(r["code"]))
        if p:
            mv += float(r["shares"] or 0) * p
    return mv


def _persist_account_snapshot_daily() -> str:
    """按收盘价重估当前持仓 → 写 account_snapshot（总资产/净值曲线不再卡在上次上传日）。

    **自愈补齐**：不只写最新交易日，而是补齐「最近一次快照日 → 最新交易日」之间所有
    缺失的交易日（净值曲线不断档；一次数据卡顿后恢复能自动填平中间日，无需人工回填）。
    前提：无新交易（现金/转债市值沿用最近一次快照），符合「持仓没动」场景。

    防覆盖：某交易日已有 account_snapshot（用户手动上传=权威）则跳过该日，不用重估价盖掉。
    重估 stock+etf（current_holding 里的，见 _holding_market_value_at 的停牌回退）；
    转债按最近快照 bond 市值固定并入（与手动上传口径对齐，消除净值锯齿）。
    失败/无持仓不阻断当日计划。
    """
    import os as _os
    from invest_model.data import make_engine
    from invest_model.repositories.base import BaseRepository
    import pandas as _pd
    try:
        engine = make_engine()
        repo = BaseRepository(engine)
        latest = repo.read_sql("SELECT MAX(trade_date) d FROM stock_daily")["d"].iloc[0]
        if latest is None:
            return "skip:no-data"
        ch = repo.read_sql("SELECT code, shares FROM current_holding")
        if ch.empty:
            return "skip:no-holding"
        codes = [str(c) for c in ch["code"]]
        # 待补交易日：最近快照日之后、到最新交易日之间、库里有行情的交易日（自愈补断档）。
        last_snap = repo.read_sql(
            "SELECT MAX(snapshot_date) d FROM account_snapshot")["d"].iloc[0]
        if last_snap is not None:
            days = repo.read_sql(
                "SELECT DISTINCT trade_date d FROM stock_daily "
                "WHERE trade_date>:s AND trade_date<=:e ORDER BY trade_date",
                {"s": str(last_snap), "e": str(latest)})
            target_dates = [str(x) for x in days["d"].tolist()] if not days.empty \
                else [str(latest)]
        else:
            target_dates = [str(latest)]
        # 防御：极端断档（如快照卡在数月前）只补最近 90 个交易日，避免一次巨量回填拖垮日更。
        if len(target_dates) > 90:
            target_dates = target_dates[-90:]
        # 现金/转债市值沿用最近一次快照（无新交易假设），补断档各日相同口径。
        last = repo.read_sql(
            "SELECT cash FROM account_snapshot ORDER BY snapshot_date DESC LIMIT 1")
        cash = (float(last["cash"].iloc[0]) if not last.empty and last["cash"].iloc[0] is not None
                else float(_os.getenv("ACCOUNT_CASH", "0") or 0))
        bond_df = repo.read_sql(
            "SELECT SUM(market_value) v FROM holding_snapshot "
            "WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM holding_snapshot) "
            "AND LOWER(asset_type)='bond'")
        bond = float(bond_df["v"].iloc[0]) if not bond_df.empty \
            and bond_df["v"].iloc[0] is not None else 0.0
        filled: list[str] = []
        skipped: list[str] = []
        for d in target_dates:
            exist = repo.read_sql(
                "SELECT 1 FROM account_snapshot WHERE snapshot_date=:d LIMIT 1", {"d": d})
            if not exist.empty:
                skipped.append(d)             # 已有快照（手动上传=权威）不覆盖
                continue
            mv = _holding_market_value_at(repo, ch, codes, d) + bond
            repo.upsert("account_snapshot", _pd.DataFrame([{
                "snapshot_date": d, "cash": round(cash, 2),
                "market_value": round(mv, 2), "total_asset": round(mv + cash, 2),
            }]), ["snapshot_date"])
            filled.append(d)
        if not filled:
            # 最新交易日已有快照是最常见的「无需补」情形，保持原 skip:manual-exists 语义
            if str(latest) in skipped:
                return f"skip:manual-exists:{latest}"
            return f"skip:no-target:{latest}"
        return f"ok:{filled[-1]}" if len(filled) == 1 else f"ok:{filled[-1]}(+{len(filled)}补断档)"
    except Exception as e:  # noqa: BLE001 — 不阻断当日计划
        print(f"WARN account_snapshot 日更重估失败：{e}")
        return f"WARN: {e}"


def _build_signal_scorecard() -> str:
    """投顾信号实战战绩记分卡（best-effort，失败不阻断当日任务）。"""
    try:
        from scripts.build_signal_scorecard import main as sc_main
        _run_cli(sc_main, ["build_signal_scorecard.py"])
        return "ok"
    except Exception as e:  # noqa: BLE001
        print(f"WARN signal_scorecard 落库失败：{e}")
        return f"err:{repr(e)[:60]}"


# ── 三水表更新提醒（watermeter-remind）──────────────────────────────

def job_watermeter_remind() -> dict:
    """每周提醒人工刷新三水表（信贷/财政/政策资本）CSV —— 套利模块引擎 B/α 的输入。"""
    day = gh_notify.bj_now().strftime("%Y%m%d")
    res = gh_notify.post_issue_comment(
        "💧 水表更新提醒",
        seed_body="本 issue 每周提醒刷新三水表（信贷/财政/政策资本）与盲区α curated CSV。",
        comment_body=(f"## {day} 水表巡检\n\n"
                      "💧 **请更新三水表信号 CSV**（`config/watermeter_*.csv`）：\n"
                      "- 信贷水表：社融结构·企业中长期贷款投向·结构性货币工具\n"
                      "- 财政水表：超长期特别国债/专项债投向·大基金·以旧换新清单\n"
                      "- 政策资本水表：国家队/汇金 ETF 增持方向·险资社保·市值管理考核\n\n"
                      "录入：`python scripts/ingest_watermeter.py --kind watermeter --csv <file>`\n"
                      "**证伪铁律：剥离股价，只看产业侧的钱到没到。** 只有故事没有真金白银 = 幻觉，不碰。"),
        dedupe_prefix=f"## {day} 水表巡检",
    )
    return {"job": "watermeter_remind", "day": day, **res}


def _build_arb_scorecard_and_backtest() -> dict:
    """套利影子回测 + 战绩记分卡（best-effort，默认观察态，失败不阻断复盘）。"""
    out: dict = {}
    from scripts.run_pipeline import main as pipe_main
    try:
        _run_cli(pipe_main, ["run_pipeline.py", "--mode", "arb",
                             "--start", _PIPELINE_START, "--version", "ic_v1"])
        out["arb_backtest"] = "ok"
    except BaseException as e:  # noqa: BLE001
        out["arb_backtest"] = f"WARN: {e}"
        print(f"WARN 套利账本回测失败（不阻断）：{e}")
    try:
        from scripts.build_arb_scorecard import main as arb_sc_main
        _run_cli(arb_sc_main, ["build_arb_scorecard.py"])
        out["arb_scorecard"] = "ok"
    except BaseException as e:  # noqa: BLE001
        out["arb_scorecard"] = f"WARN: {e}"
        print(f"WARN 套利记分卡失败（不阻断）：{e}")
    return out


# ── 周六全量重建 + P4 影子回测 + 复盘（data-update all档 + review 链）────

def job_weekly_rebuild_review() -> dict:
    from scripts.run_pipeline import main as pipe_main
    out: dict = {"job": "weekly_rebuild_review"}

    # 1) 全量刷新 universe→因子→IC→预测→回测（失败告警但不阻断复盘，
    #    对齐原架构里 review.yml 独立于 data-update 运行的行为）
    try:
        _run_cli(pipe_main, ["run_pipeline.py", "--mode", "all",
                             "--start", _PIPELINE_START])
        out["rebuild"] = "ok"
    except BaseException as e:  # noqa: BLE001 — 含 SystemExit
        out["rebuild"] = f"FAIL: {e}"
        gh_notify.alert("weekly_rebuild(all)", e)

    # 2) P4 影子对照回测（原 workflow 即「失败不阻断」）
    try:
        _run_cli(pipe_main, ["run_pipeline.py", "--mode", "backtest",
                             "--start", _PIPELINE_START, "--version", "pf_v2",
                             "--scheme", "inv_vol", "--hold-buffer", "1.5"])
        out["p4_shadow"] = "ok"
    except BaseException as e:  # noqa: BLE001
        out["p4_shadow"] = f"WARN: {e}"
        print(f"WARN P4 影子回测失败（不阻断）：{e}")

    # 2b) 套利统一资金账本影子回测 + 记分卡（观察态，不阻断）
    out.update(_build_arb_scorecard_and_backtest())

    # 3) 复盘（纯读 DB）+ 推送
    review_out = "/tmp/review.md"
    from scripts.review import main as review_main
    _run_cli(review_main, ["review.py", "--out", review_out])
    with open(review_out, encoding="utf-8") as f:
        body = f.read()
    today = gh_notify.bj_now().strftime("%Y-%m-%d")
    out["review"] = gh_notify.post_issue_comment(
        "🔍 复盘报告",
        seed_body="本 issue 由 FC 定时函数每周追加复盘报告（投顾/模型/持仓/纪律 与真实收益对账）。",
        comment_body=f"## {today} 复盘\n\n{body}",
        dedupe_prefix=f"## {today} 复盘",
    )
    return out


JOBS = {
    "live_watch": job_live_watch,
    "snapshot_remind": job_snapshot_remind,
    "ingest_etf": job_ingest_etf,
    "daily_update_plan": job_daily_update_plan,
    "weekly_rebuild_review": job_weekly_rebuild_review,
    "watermeter_remind": job_watermeter_remind,
}
