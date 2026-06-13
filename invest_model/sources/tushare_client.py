"""Tushare 数据源客户端（主数据源，5000+ 积分）"""

import os
import time
from functools import wraps

import pandas as pd
import tushare as ts

from invest_model.config import get_env, load_config
from invest_model.logger import get_logger
from invest_model.sources.base import BaseSource

logger = get_logger()


def _rate_limit(func):
    """限频装饰器：根据配置控制调用频率"""
    last_call = [0.0]

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        cfg = load_config()
        rpm = cfg.get("sources", {}).get("tushare", {}).get("rate_limit_per_min", 480)
        min_interval = 60.0 / rpm
        elapsed = time.time() - last_call[0]
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        last_call[0] = time.time()
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


class TushareClient(BaseSource):
    """Tushare Pro 数据源"""

    def __init__(self):
        token = get_env("TUSHARE_TOKEN")
        if not token or not token.strip():
            raise RuntimeError("TUSHARE_TOKEN 未设置，请在 .env 文件中配置")

        self.pro = ts.pro_api(token)

        http_url = _resolve_tushare_http_url()
        if http_url:
            self.pro._DataApi__http_url = http_url
            logger.info(f"Tushare 使用自定义接口基地址: {http_url}")

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
        df = self.pro.stock_basic(
            exchange="", list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_date"
        )
        return df

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
                   "turnover_rate,turnover_rate_f"
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
