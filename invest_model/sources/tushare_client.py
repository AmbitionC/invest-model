"""Tushare 数据源客户端（主数据源，5000+ 积分）"""

import os
import time
from functools import wraps

import pandas as pd
import requests
import tushare as ts

from invest_model.config import get_env, load_config
from invest_model.logger import get_logger
from invest_model.sources.base import BaseSource

logger = get_logger()


# 全局限频时钟：所有接口方法共享。若每个方法各自计时，多方法混合调用时
# 总 QPS 会超过 rate_limit_per_min 配置的上限。
_LAST_CALL = [0.0]


def _rate_limit(func):
    """限频装饰器：根据配置控制调用频率（跨方法全局共享）"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        cfg = load_config()
        rpm = cfg.get("sources", {}).get("tushare", {}).get("rate_limit_per_min", 480)
        min_interval = 60.0 / rpm
        elapsed = time.time() - _LAST_CALL[0]
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        _LAST_CALL[0] = time.time()
        return func(self, *args, **kwargs)

    return wrapper


_TOKEN_ERROR_KEYWORDS = (
    "token不对", "token错误", "token无效", "请确认",
    "token invalid", "invalid token", "token error",
    # tushare 返回 "token expired" / "token已过期" 时也走 token 错误分支，
    # 避免被外层 _retry 当作普通异常重试 N 次再抛出（已过期重试不会成功）。
    "token expired", "token已过期", "token expire", "expired token",
)


def _is_token_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw.lower() in msg for kw in _TOKEN_ERROR_KEYWORDS)


def _retry(func):
    """重试装饰器（token 错误直接抛出，不重试）"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        cfg = load_config()
        ts_cfg = cfg.get("sources", {}).get("tushare", {})
        retries = ts_cfg.get("retry_times", 3)
        delay = ts_cfg.get("retry_delay", 2)

        for attempt in range(1, retries + 1):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                if _is_token_error(e):
                    raise RuntimeError(
                        f"Tushare Token 无效/已过期或未使用文档要求的接口地址（原始报错: {e}）。"
                        "请登录 https://tushare.pro → 用户中心 → Token，重新生成后写入 .env 的 "
                        "TUSHARE_TOKEN；若 token 正确仍报错，请在 config.yaml 的 "
                        "sources.tushare.http_url 或环境变量 TUSHARE_HTTP_URL 中配置与 tushare.pro "
                        "文档一致的 http 基地址。"
                    ) from e
                if attempt == retries:
                    logger.error(f"Tushare 调用失败（已重试 {retries} 次）: {e}")
                    raise
                logger.warning(f"Tushare 调用失败（第 {attempt} 次），{delay}s 后重试: {e}")
                time.sleep(delay)

    return wrapper


def _resolve_tushare_http_url() -> str | None:
    """环境变量 TUSHARE_HTTP_URL 优先，否则使用 config.yaml sources.tushare.http_url。"""
    load_config()
    env_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    if env_url:
        return env_url.rstrip("/") + "/"
    cfg = load_config()
    yaml_url = (cfg.get("sources", {}).get("tushare", {}) or {}).get("http_url") or ""
    yaml_url = str(yaml_url).strip()
    if yaml_url:
        return yaml_url.rstrip("/") + "/"
    return None


def _use_keepalive_session() -> None:
    """让 tushare 的所有 HTTP 请求复用同一条 keep-alive 长连接。

    tushare 的 DataApi 每次 query 都用模块级 requests.post 新建 TCP 连接；
    在公网出口 IP 不固定的环境（如阿里云 FC 的 SNAT 出口）表现为同一 token
    秒级内从多个 IP 调用，触发数据服务「ip超限，请不要在多个ip同时使用」。
    换成全局 Session 后连接池复用同一条连接 → 同一个出口 IP。
    对固定 IP 环境（本机/GH Actions）也是纯收益（省去每次握手）。
    """
    from tushare.pro import client as _pro_client
    if not isinstance(getattr(_pro_client, "requests", None), requests.Session):
        _pro_client.requests = requests.Session()


class TushareClient(BaseSource):
    """Tushare Pro 数据源"""

    def __init__(self):
        _use_keepalive_session()
        token = get_env("TUSHARE_TOKEN")
        if not token or not token.strip():
            raise RuntimeError("TUSHARE_TOKEN 未设置，请在 .env 文件中配置")

        self.pro = ts.pro_api(token)

        # HTTP 超时：防止单次请求挂住导致整条流水线无限等待（config sources.tushare.timeout，默认30s）。
        try:
            ts_cfg = (load_config().get("sources", {}) or {}).get("tushare", {}) or {}
            timeout = int(ts_cfg.get("timeout", 30))
            self.pro._DataApi__timeout = timeout
        except Exception:  # noqa: BLE001 — 不同 tushare 版本属性名差异时不阻断初始化
            timeout = 30

        http_url = _resolve_tushare_http_url()
        if http_url:
            self.pro._DataApi__http_url = http_url
            logger.info(f"Tushare 使用自定义接口基地址: {http_url}（超时 {timeout}s）")

        try:
            df = self.pro.trade_cal(exchange="SSE", start_date="20250101", end_date="20250101")
            if df is None or df.empty:
                raise RuntimeError("Token 验证返回空数据")
        except Exception as e:
            if _is_token_error(e):
                raise RuntimeError(
                    f"Tushare Token 校验失败（原始报错: {e}）。常见原因：\n"
                    "  1. Token 已过期 → 登录 https://tushare.pro → 用户中心 → Token，重新生成后写入 .env\n"
                    "  2. Token 错误 → 检查 .env 中 TUSHARE_TOKEN 是否被截断/带了引号/有多余空格\n"
                    "  3. 接口地址不对 → 在 config.yaml 的 sources.tushare.http_url "
                    "或环境变量 TUSHARE_HTTP_URL 中配置官方文档给出的 http 镜像地址"
                ) from e
            raise

        logger.info("Tushare 客户端初始化完成（Token 验证通过）")

    @_retry
    @_rate_limit
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.trade_cal(
            exchange="SSE", start_date=start_date, end_date=end_date,
            fields="cal_date,is_open,pretrade_date"
        )
        return df

    @_retry
    @_rate_limit
    def get_stock_list(self) -> pd.DataFrame:
        """全量股票列表：在市(L) + 退市(D) + 暂停上市(P)。

        只拉 L 会让退市股在 stock_info 缺元信息（name/industry/list_date 全空），
        导致 ST/次新过滤失效、行业中性化落入 NA 桶——幸存者偏差的数据源头。
        """
        frames = []
        for status in ("L", "D", "P"):
            df = self.pro.stock_basic(
                exchange="", list_status=status,
                fields="ts_code,symbol,name,area,industry,market,list_date"
            )
            if df is not None and not df.empty:
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).drop_duplicates("ts_code")

    @_retry
    @_rate_limit
    def get_stock_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.daily(
            ts_code=code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "vol": "volume"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_daily_bulk(self, trade_date: str) -> pd.DataFrame:
        """按交易日一次拉取全市场日线 OHLCV（1 次调用覆盖 ~5000 只）。"""
        df = self.pro.daily(trade_date=trade_date)
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "vol": "volume"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_fina_indicator_bulk(self, period: str) -> pd.DataFrame:
        """按报告期一次拉取全市场财务指标（fina_indicator_vip，需 VIP 权限）。

        period 形如 ``20240930``。无 VIP 权限时调用方应回退到逐票
        :meth:`get_stock_fundamental`。
        """
        df = self.pro.fina_indicator_vip(
            period=period,
            fields="ts_code,ann_date,end_date,eps,bps,roe,roa,"
                   "grossprofit_margin,debt_to_assets,or_yoy,netprofit_yoy,"
                   "q_sales_yoy,q_netprofit_yoy,ocfps",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={
                "ts_code": "code",
                "end_date": "report_date",
                "grossprofit_margin": "gross_margin",
                "debt_to_assets": "debt_to_asset",
                "or_yoy": "revenue_yoy",
                "netprofit_yoy": "profit_yoy",
                "q_netprofit_yoy": "q_profit_yoy",
            })
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_balancesheet_bulk(self, period: str) -> pd.DataFrame:
        """按报告期一次拉全市场资产负债表关键项（balancesheet_vip，需 VIP）。

        排雷/扣商誉估值原料：商誉、少数股东权益、归母净资产、应收账款。
        """
        df = self.pro.balancesheet_vip(
            period=period,
            fields="ts_code,ann_date,end_date,goodwill,minority_int,"
                   "total_hldr_eqy_exc_min_int,accounts_receiv",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={
                "ts_code": "code", "end_date": "report_date",
                "total_hldr_eqy_exc_min_int": "eq_exc_min",
            })
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_income_bulk(self, period: str) -> pd.DataFrame:
        """按报告期一次拉全市场利润表关键项（income_vip，需 VIP）。

        排雷原料：净利润 vs 归母净利润（少数股东洗澡探针）、三费。
        """
        df = self.pro.income_vip(
            period=period,
            fields="ts_code,ann_date,end_date,revenue,n_income,"
                   "n_income_attr_p,sell_exp,admin_exp,fin_exp",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "end_date": "report_date"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_cashflow_bulk(self, period: str) -> pd.DataFrame:
        """按报告期一次拉全市场现金流量表关键项（cashflow_vip，需 VIP）。

        排雷原料：经营现金流净额（利润-现金背离探针）。
        """
        df = self.pro.cashflow_vip(
            period=period,
            fields="ts_code,ann_date,end_date,n_cashflow_act",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "end_date": "report_date"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_express_by_date(self, ann_date: str) -> pd.DataFrame:
        """按公告日拉业绩快报（时效层：预报<快报<定期报告）。"""
        df = self.pro.express(
            ann_date=ann_date,
            fields="ts_code,ann_date,end_date,revenue,n_income,yoy_net_profit",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={
                "ts_code": "code", "end_date": "report_date",
                "yoy_net_profit": "profit_yoy",
            })
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_forecast_by_date(self, ann_date: str) -> pd.DataFrame:
        """按公告日拉业绩预告；profit_yoy 取预告区间中值（与累计同比可比）。"""
        df = self.pro.forecast(
            ann_date=ann_date,
            fields="ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "end_date": "report_date",
                                    "type": "forecast_type"})
            lo = pd.to_numeric(df.get("p_change_min"), errors="coerce")
            hi = pd.to_numeric(df.get("p_change_max"), errors="coerce")
            df["profit_yoy"] = ((lo + hi) / 2.0).fillna(lo).fillna(hi)
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_holder_trade_by_date(self, ann_date: str) -> pd.DataFrame:
        """按公告日全市场拉重要股东/高管增减持（跟庄信号数据源）。"""
        df = self.pro.stk_holdertrade(
            ann_date=ann_date,
            fields="ts_code,ann_date,holder_name,holder_type,in_de,"
                   "change_vol,change_ratio,after_ratio,avg_price",
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_namechange(self) -> pd.DataFrame:
        """全量历史名称变更（point-in-time ST 识别用）。表小（~2万行），分页拉全。"""
        frames, offset, page = [], 0, 5000
        while offset <= 100000:
            df = self.pro.namechange(
                limit=page, offset=offset,
                fields="ts_code,name,start_date,end_date,change_reason")
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < page:
                break
            offset += page
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @_retry
    @_rate_limit
    def get_hk_hold(self, trade_date: str) -> pd.DataFrame:
        """北向（沪深股通）个股持股快照，按交易日全市场一次拉取。

        候选因子 nb_ratio_chg_20 的数据源；港股通闭市日返回空属正常。
        """
        df = self.pro.hk_hold(
            trade_date=trade_date,
            fields="ts_code,trade_date,name,vol,ratio,exchange")
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_index_weight(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """指数成分与权重（月度快照）。基准/可选 universe 用。"""
        df = self.pro.index_weight(
            index_code=index_code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"con_code": "code", "index_code": "index_code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_index_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.index_daily(
            ts_code=code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "vol": "volume"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_stock_fundamental(self, code: str, period: str) -> pd.DataFrame:
        df = self.pro.fina_indicator(
            ts_code=code, period=period,
            fields="ts_code,ann_date,end_date,eps,bps,roe,roa,"
                   "grossprofit_margin,debt_to_assets,op_yoy,netprofit_yoy,"
                   "revenue,net_profit,ocfps"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={
                "ts_code": "code",
                "end_date": "report_date",
                "grossprofit_margin": "gross_margin",
                "debt_to_assets": "debt_to_asset",
                "op_yoy": "revenue_yoy",
                "netprofit_yoy": "profit_yoy",
            })
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_daily_basic(self, trade_date: str) -> pd.DataFrame:
        """获取每日指标（PE/PB/PS 等），按日期拉取全市场"""
        df = self.pro.daily_basic(
            trade_date=trade_date,
            fields="ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,"
                   "turnover_rate,turnover_rate_f,dv_ratio,dv_ttm"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_cashflow(self, code: str, trade_date: str) -> pd.DataFrame:
        df = self.pro.moneyflow(
            ts_code=code, trade_date=trade_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_holder_trade(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.stk_holdertrade(
            ts_code=code, start_date=start_date, end_date=end_date,
            fields="ts_code,ann_date,holder_name,holder_type,in_de,"
                   "change_vol,change_ratio,after_share,after_ratio,"
                   "avg_price,begin_date,close_date"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_holder_count(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.stk_holdernumber(
            ts_code=code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_margin(self, trade_date: str) -> pd.DataFrame:
        df = self.pro.margin(trade_date=trade_date)
        if df is not None and not df.empty:
            df = df.rename(columns={"exchange_id": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_margin_detail(self, trade_date: str) -> pd.DataFrame:
        """个股两融明细（融资余额 by 股票）→ 聚合行业得信贷水表。"""
        df = self.pro.margin_detail(
            trade_date=trade_date,
            fields="ts_code,trade_date,rzye,rqye,rzmre,rzche")
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_etf_list(self) -> pd.DataFrame:
        df = self.pro.fund_basic(
            market="E", status="L",
            fields="ts_code,name,management,custodian,fund_type,"
                   "found_date,due_date,list_date,delist_date,"
                   "invest_type,type,market"
        )
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_etf_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.pro.fund_daily(
            ts_code=code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code", "vol": "volume"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_etf_holding(self, code: str, report_date: str) -> pd.DataFrame:
        """ETF 重仓股（fund_portfolio）"""
        df = self.pro.fund_portfolio(
            ts_code=code, end_date=report_date
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_hsgt_flow(self, trade_date: str) -> pd.DataFrame:
        """沪深港通每日资金流向（北向+南向汇总）"""
        df = self.pro.moneyflow_hsgt(trade_date=trade_date)
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_hsgt_top10(self, trade_date: str, market_type: str = "1") -> pd.DataFrame:
        """沪深港通十大成交股（market_type: 1=沪股通 3=深股通）"""
        df = self.pro.hsgt_top10(
            trade_date=trade_date, market_type=market_type,
            fields="trade_date,ts_code,name,close,change,rank,"
                   "market_type,amount,net_amount,buy,sell"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    # ── 宏观经济数据 ──────────────────────────────────────

    @_retry
    @_rate_limit
    def get_money_supply(self, start_m: str, end_m: str) -> pd.DataFrame:
        """货币供应量 M0/M1/M2（月度，格式 YYYYMM）"""
        df = self.pro.cn_m(start_m=start_m, end_m=end_m)
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_pmi(self, start_m: str, end_m: str) -> pd.DataFrame:
        """PMI（月度）：制造业/非制造业/综合"""
        df = self.pro.cn_pmi(start_m=start_m, end_m=end_m)
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_cpi(self, start_m: str, end_m: str) -> pd.DataFrame:
        """CPI（月度）"""
        df = self.pro.cn_cpi(start_m=start_m, end_m=end_m)
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_ppi(self, start_m: str, end_m: str) -> pd.DataFrame:
        """PPI（月度）"""
        df = self.pro.cn_ppi(start_m=start_m, end_m=end_m)
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_lpr(self, start_date: str, end_date: str) -> pd.DataFrame:
        """贷款市场报价利率 LPR（日度）"""
        df = self.pro.cb_lpr(start_date=start_date, end_date=end_date)
        return df if df is not None else pd.DataFrame()

    # ── 套利模块（arbitrage）数据源 ──────────────────────────
    # 交易所国债逆回购代码：GC001/007/014/028…（沪）R-001…（深）。close=年化利率。
    REVERSE_REPO_CODES = (
        "204001.SH", "204002.SH", "204003.SH", "204004.SH",
        "204007.SH", "204014.SH", "204028.SH", "204091.SH", "204182.SH",
        "131810.SZ", "131811.SZ", "131800.SZ", "131809.SZ",
        "131801.SZ", "131802.SZ", "131803.SZ", "131805.SZ", "131806.SZ",
    )

    @_retry
    @_rate_limit
    def get_reverse_repo_daily(self, trade_date: str) -> pd.DataFrame:
        """国债逆回购日行情：走 pro.daily 拉逆回购代码，close 即年化利率(%)。

        权限缺失/接口异常由外层 update.py 的 try/except 跳过（优雅降级）。
        """
        df = self.pro.daily(trade_date=trade_date)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df[df["ts_code"].isin(self.REVERSE_REPO_CODES)].copy()
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"ts_code": "code", "vol": "volume"})
        df["rate"] = df["close"]
        return df

    @_retry
    @_rate_limit
    def get_cb_basic(self) -> pd.DataFrame:
        """可转债基础信息（cb_basic，静态全量）。"""
        df = self.pro.cb_basic(
            fields="ts_code,bond_short_name,stk_code,list_date,delist_date,"
                   "conv_price,maturity_date,remain_size,call_status"
        )
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_cb_daily(self, trade_date: str) -> pd.DataFrame:
        """可转债日行情（cb_daily）。"""
        df = self.pro.cb_daily(
            trade_date=trade_date,
            fields="ts_code,trade_date,open,high,low,close,pre_close,"
                   "pct_chg,vol,amount,cb_value,cb_over_rate"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={"ts_code": "code"})
        return df if df is not None else pd.DataFrame()

    @_retry
    @_rate_limit
    def get_dividend(self, ann_date: str) -> pd.DataFrame:
        """分红事件（dividend，按公告日拉取，仅取已实施 div_proc='实施'）。"""
        df = self.pro.dividend(
            ann_date=ann_date,
            fields="ts_code,ann_date,end_date,div_proc,stk_div,cash_div,"
                   "cash_div_tax,record_date,ex_date,pay_date"
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"ts_code": "code"})
        df = df[(df["div_proc"] == "实施") & df["ex_date"].notna() & (df["ex_date"] != "")]
        return df.copy()
