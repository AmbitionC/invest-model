"""生成合成 A 股数据集到本地 SQLite，用于在无外网/无生产库环境下验证整套系统。

设计要点：每只股票有静态潜在 alpha，价值(EP)、质量(ROE)、动量与该 alpha 正相关，
因此因子流水线应能侦测到正 IC，Top-N 组合应跑赢市值加权基准——以此验证：
  (1) 端到端流水线跑通；(2) 因子确有信号；(3) 组合近满仓、~30 只、无空仓陷阱。

用法：
    python scripts/gen_synthetic_sample.py --db sqlite:///./data/local.db \
        --n-stocks 250 --start 20210101 --end 20260613
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.logger import get_logger  # noqa: E402

logger = get_logger()

INDUSTRIES = [
    "电子", "医药", "食品饮料", "银行", "非银金融", "化工", "机械", "汽车",
    "电力设备", "计算机", "传媒", "有色金属", "建筑", "家电", "农林牧渔", "国防军工",
]
BENCHMARK = "000300.SH"


def _trading_days(start: str, end: str) -> list[str]:
    rng = pd.bdate_range(pd.to_datetime(start), pd.to_datetime(end))
    return [d.strftime("%Y%m%d") for d in rng]


def generate(db_url: str, n_stocks: int, start: str, end: str, seed: int = 42) -> None:
    rng = np.random.default_rng(seed)
    engine = make_engine(db_url)
    create_schema(engine)

    dates = _trading_days(start, end)
    n_days = len(dates)
    logger.info(f"合成：{n_stocks} 只 × {n_days} 交易日 ({start}~{end})")

    # ── 标的元信息 ──
    codes, names, inds, list_dates, markets = [], [], [], [], []
    for i in range(n_stocks):
        ex = "SH" if i % 2 == 0 else "SZ"
        num = (600000 + i) if ex == "SH" else (1 + i)
        code = f"{num:06d}.{ex}"
        codes.append(code)
        ind = INDUSTRIES[i % len(INDUSTRIES)]
        inds.append(ind)
        # 部分票设为次新（上市晚）/ ST（名字带 ST），用于验证 filters
        is_new = i < int(0.04 * n_stocks)
        ld = "20251201" if is_new else f"201{rng.integers(0, 9)}0{rng.integers(1, 9)}15"
        list_dates.append(ld)
        nm = ("ST" if (i % 37 == 0) else "") + f"合成{i:03d}"
        names.append(nm)
        markets.append("主板")
    info = pd.DataFrame({
        "ts_code": codes, "symbol": [c[:6] for c in codes], "name": names,
        "area": "NA", "industry": inds, "market": markets, "list_date": list_dates,
    })

    # ── 潜在结构 ──
    alpha = rng.normal(0, 0.00035, n_stocks)          # 静态日 alpha（截面 spread，温和）
    beta = rng.uniform(0.7, 1.3, n_stocks)
    ind_idx = np.array([INDUSTRIES.index(x) for x in inds])
    n_ind = len(INDUSTRIES)
    base_price = rng.uniform(5, 80, n_stocks)
    shares = rng.uniform(2e8, 5e9, n_stocks)          # 总股本（股）
    float_ratio = rng.uniform(0.5, 1.0, n_stocks)

    # ── 行情：市场 + 行业 + 个股 ──
    mkt = rng.normal(0.0004, 0.010, n_days)           # 市场日收益（温和上行）
    ind_ret = rng.normal(0, 0.005, (n_days, n_ind))
    idio = rng.normal(0, 0.013, (n_days, n_stocks))
    daily_ret = (mkt[:, None] * beta[None, :]
                 + ind_ret[:, ind_idx]
                 + alpha[None, :]
                 + idio)
    daily_ret = np.clip(daily_ret, -0.095, 0.095)     # 近似涨跌停约束

    log_path = np.cumsum(np.log1p(daily_ret), axis=0)
    price = base_price[None, :] * np.exp(log_path)    # (n_days, n_stocks)
    pre = np.vstack([base_price[None, :], price[:-1, :]])

    # ── 写行情 ──
    daily_rows = []
    for j, code in enumerate(codes):
        # 次新票上市前无数据
        ld = list_dates[j]
        valid = np.array([d >= ld for d in dates])
        c = price[:, j]
        p = pre[:, j]
        o = p * (1 + rng.normal(0, 0.004, n_days))
        hi = np.maximum.reduce([o, c, p]) * (1 + np.abs(rng.normal(0, 0.006, n_days)))
        lo = np.minimum.reduce([o, c, p]) * (1 - np.abs(rng.normal(0, 0.006, n_days)))
        vol = (shares[j] * float_ratio[j] * rng.uniform(0.002, 0.02, n_days)) / 100.0  # 手
        amount = vol * c * 100 / 1000.0  # 千元
        pct = (c - p) / p * 100
        df = pd.DataFrame({
            "code": code, "trade_date": dates,
            "open": o.round(3), "high": hi.round(3), "low": lo.round(3),
            "close": c.round(3), "pre_close": p.round(3),
            "change": (c - p).round(3), "pct_chg": pct.round(4),
            "volume": vol.round(2), "amount": amount.round(3),
        })
        df = df[valid]
        daily_rows.append(df)
    daily = pd.concat(daily_rows, ignore_index=True)

    # ── 估值（daily_basic）：EP/质量与 alpha 正相关 → 低 PE = 高 alpha ──
    # circ_mv 随价格变动；pe 由静态盈利能力 + alpha 决定
    fund_rows = []
    earn_yield = 0.045 + alpha * 22.0 + rng.normal(0, 0.004, n_stocks)  # 盈利收益率 E/P
    earn_yield = np.clip(earn_yield, 0.005, 0.18)
    pe_static = 1.0 / earn_yield
    bp_static = (0.12 + alpha * 14.0 + rng.normal(0, 0.03, n_stocks))
    bp_static = np.clip(bp_static, 0.05, 1.5)
    dv_base = np.clip(rng.uniform(0.0, 5.0, n_stocks), 0.0, None)   # 静态股息率(%)
    problem = np.array([i % 41 == 0 for i in range(n_stocks)])      # ~2.5% 排雷"问题公司"
    for j, code in enumerate(codes):
        ld = list_dates[j]
        valid = np.array([d >= ld for d in dates])
        c = price[:, j]
        total_mv = c * shares[j] / 10000.0            # 万元
        circ_mv = total_mv * float_ratio[j]
        pe = pe_static[j] * (c / base_price[j])        # 价格涨 → PE 抬升
        pb = (1.0 / bp_static[j]) * (c / base_price[j])
        ps = pe * rng.uniform(0.3, 0.8)
        turn = rng.uniform(0.5, 6.0, n_days)
        dv = dv_base[j] * (base_price[j] / c)          # 价格涨 → 股息率降
        df = pd.DataFrame({
            "code": code, "trade_date": dates,
            "pe_ttm": pe.round(4), "pb": pb.round(4), "ps_ttm": ps.round(4),
            "total_mv": total_mv.round(4), "circ_mv": circ_mv.round(4),
            "turnover_rate": turn.round(4), "turnover_rate_f": (turn * 1.2).round(4),
            "dv_ratio": dv.round(4), "dv_ttm": dv.round(4),
        })
        fund_rows.append(df[valid])
    fundamental = pd.concat(fund_rows, ignore_index=True)

    # ── 季度财务：ROE 与 alpha 正相关 ──
    periods = [d for d in ["20201231", "20210331", "20210630", "20210930", "20211231",
                           "20220331", "20220630", "20220930", "20221231",
                           "20230331", "20230630", "20230930", "20231231",
                           "20240331", "20240630", "20240930", "20241231",
                           "20250331", "20250630", "20250930", "20251231",
                           "20260331"]]
    roe_base = np.clip(9.0 + alpha * 7000.0 + rng.normal(0, 2.5, n_stocks), -5, 35)
    fina_rows, ext_rows = [], []
    for j, code in enumerate(codes):
        q_prev = float(np.clip(alpha[j] * 11000 + rng.normal(5, 18), -50, 120))
        fee_prev = float(rng.uniform(0.12, 0.25))
        for p in periods:
            ann = (pd.to_datetime(p) + pd.Timedelta(days=45)).strftime("%Y%m%d")
            profit_yoy = float(np.clip(alpha[j] * 11000 + rng.normal(5, 18), -50, 120))
            # 单季增速带惯性（AR1），使增速二阶导（growth_accel）有可检测结构
            q_now = float(np.clip(0.6 * q_prev + 0.4 * profit_yoy + rng.normal(0, 8), -60, 150))
            revenue = round(float(rng.uniform(1e8, 5e10)), 4)
            net_profit = round(float(rng.uniform(1e7, 5e9)), 4)
            fina_rows.append({
                "code": code, "report_date": p, "ann_date": ann,
                "eps": round(float(rng.uniform(0.1, 2.0)), 4),
                "bps": round(float(rng.uniform(2, 12)), 4),
                "roe": round(float(roe_base[j] + rng.normal(0, 1.0)), 4),
                "roa": round(float(roe_base[j] * 0.5 + rng.normal(0, 0.8)), 4),
                "gross_margin": round(float(np.clip(20 + alpha[j] * 5000 + rng.normal(0, 5), 5, 70)
                                            + (25 if problem[j] else 0)), 4),
                "debt_to_asset": round(float(rng.uniform(20, 65)), 4),
                "revenue_yoy": round(float(np.clip(alpha[j] * 9000 + rng.normal(5, 12), -40, 80)), 4),
                "profit_yoy": round(profit_yoy, 4),
                "revenue": revenue,
                "net_profit": net_profit,
                "ocfps": round(float(rng.uniform(0.1, 3.0)), 4),
                "q_sales_yoy": round(float(np.clip(alpha[j] * 9000 + rng.normal(5, 15), -50, 100)), 4),
                "q_profit_yoy": round(q_now, 4),
            })
            q_prev = q_now
            # 报表扩展项（排雷原料）：problem 股高应收/高商誉/归母异常/现金背离/三费骤降
            eq = float(rng.uniform(1e9, 2e10))
            fee_now = fee_prev - (0.08 if problem[j] else float(rng.normal(0, 0.01)))
            fee_now = float(np.clip(fee_now, 0.02, 0.4))
            three_fee = revenue * fee_now
            ext_rows.append({
                "code": code, "report_date": p, "ann_date": ann,
                "goodwill": round(eq * (rng.uniform(0.4, 0.7) if problem[j]
                                        else rng.uniform(0.0, 0.15)), 4),
                "minority_int": round(eq * float(rng.uniform(0.0, 0.2)), 4),
                "eq_exc_min": round(eq, 4),
                "accounts_receiv": round(revenue * (rng.uniform(0.6, 0.9) if problem[j]
                                                    else rng.uniform(0.05, 0.3)), 4),
                "revenue": revenue,
                "n_income": net_profit,
                "n_income_attr_p": round(net_profit * (rng.uniform(1.8, 2.5) if problem[j]
                                                       else rng.uniform(0.85, 1.0)), 4),
                "sell_exp": round(three_fee * 0.4, 4),
                "admin_exp": round(three_fee * 0.4, 4),
                "fin_exp": round(three_fee * 0.2, 4),
                "n_cashflow_act": round(net_profit * (rng.uniform(0.0, 0.2) if problem[j]
                                                      else rng.uniform(0.8, 1.2)), 4),
            })
            fee_prev = fee_now
    fina = pd.DataFrame(fina_rows)
    fina_ext = pd.DataFrame(ext_rows)

    # ── 高管增减持（跟庄信号 insider_conviction 数据源）：~10% 标的有零星记录 ──
    ht_rows = []
    for j, code in enumerate(codes):
        if j % 10 != 3:
            continue
        for _ in range(int(rng.integers(1, 4))):
            d = dates[int(rng.integers(60, n_days))]
            ht_rows.append({
                "code": code, "ann_date": d,
                "holder_name": f"高管{int(rng.integers(1, 9))}号",
                "holder_type": "G" if rng.random() < 0.7 else "P",
                "in_de": "IN" if rng.random() < 0.65 else "DE",
                "change_vol": round(float(rng.uniform(1e4, 5e5)), 2),
                "change_ratio": round(float(rng.uniform(0.01, 0.5)), 4),
                "after_ratio": round(float(rng.uniform(0.1, 5.0)), 4),
                "avg_price": round(float(rng.uniform(5, 80)), 4),
            })
    holder = pd.DataFrame(ht_rows).drop_duplicates(
        ["code", "ann_date", "holder_name", "in_de"]) if ht_rows else pd.DataFrame()

    # ── 业绩快报（时效层）：问题公司末期披露增速转负 → 戴维斯双杀预警可测 ──
    exp_rows = []
    last_p = periods[-1]
    for j, code in enumerate(codes):
        if not problem[j]:
            continue
        ann = (pd.to_datetime(last_p) + pd.Timedelta(days=20)).strftime("%Y%m%d")
        exp_rows.append({
            "code": code, "report_date": last_p, "kind": "express", "ann_date": ann,
            "revenue": round(float(rng.uniform(1e8, 5e10)), 4),
            "n_income": round(float(rng.uniform(1e6, 1e9)), 4),
            "profit_yoy": round(float(rng.uniform(-60, -10)), 4),
            "forecast_type": None,
        })
    express = pd.DataFrame(exp_rows) if exp_rows else pd.DataFrame()

    # ── 指数（基准）：市值加权日收益 ──
    idx_ret = (daily_ret * (shares * float_ratio)[None, :]).sum(axis=1) / (shares * float_ratio).sum()
    idx_close = 3000.0 * np.exp(np.cumsum(np.log1p(idx_ret)))
    idx_pre = np.concatenate([[3000.0], idx_close[:-1]])
    index_df = pd.DataFrame({
        "code": BENCHMARK, "trade_date": dates,
        "open": idx_pre.round(3), "high": (idx_close * 1.003).round(3),
        "low": (idx_close * 0.997).round(3), "close": idx_close.round(3),
        "pre_close": idx_pre.round(3), "change": (idx_close - idx_pre).round(3),
        "pct_chg": ((idx_close - idx_pre) / idx_pre * 100).round(4),
        "volume": 0, "amount": 0,
    })

    # ── 交易日历 ──
    cal = pd.DataFrame({"cal_date": dates, "is_open": 1,
                        "pretrade_date": [dates[0]] + dates[:-1]})

    # ── 落库（to_sql append，schema 已建）──
    def _write(df: pd.DataFrame, table: str) -> None:
        df.to_sql(table, engine, if_exists="append", index=False,
                  method="multi", chunksize=400)
        logger.info(f"  {table}: {len(df)} 行")

    logger.info("写入 SQLite ...")
    _write(cal, "trade_calendar")
    _write(info, "stock_info")
    _write(daily, "stock_daily")
    _write(fundamental, "stock_fundamental")
    _write(fina, "stock_fina_indicator")
    _write(fina_ext, "stock_fina_ext")
    if not holder.empty:
        _write(holder, "holder_trade")
    if not express.empty:
        _write(express, "fina_express")
    _write(index_df, "index_daily")
    logger.info("合成数据生成完成。")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="sqlite:///./data/local.db")
    ap.add_argument("--n-stocks", type=int, default=250)
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="20260613")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate(args.db, args.n_stocks, args.start, args.end, args.seed)


if __name__ == "__main__":
    main()
