"""R1 数据可得性探测：宏观/利率类 tushare 接口。只读，不落库。"""
import sys, os
sys.path.insert(0, "/home/user/invest-model")
os.chdir("/home/user/invest-model")
import pandas as pd
from invest_model.sources.tushare_client import TushareClient

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)

c = TushareClient()
pro = c.pro

def probe(name, fn):
    try:
        df = fn()
        if df is None or len(df) == 0:
            print(f"[EMPTY] {name}")
            return None
        print(f"[OK]    {name}  rows={len(df)}  cols={list(df.columns)}")
        print(df.head(3).to_string())
        print(df.tail(2).to_string())
        print()
        return df
    except Exception as e:
        print(f"[FAIL]  {name}: {type(e).__name__}: {e}\n")
        return None

probe("cn_m (M0/M1/M2)", lambda: pro.cn_m(start_m="200501", end_m="202607"))
probe("cn_cpi", lambda: pro.cn_cpi(start_m="200501", end_m="202607"))
probe("cn_ppi", lambda: pro.cn_ppi(start_m="200501", end_m="202607"))
probe("cn_sf (社融)", lambda: pro.cn_sf(start_m="200501", end_m="202607"))
probe("sf_month", lambda: pro.sf_month(start_m="200501", end_m="202607"))
probe("cn_gdp", lambda: pro.cn_gdp(start_q="2005Q1", end_q="2026Q2"))
probe("cn_pmi", lambda: pro.cn_pmi(start_m="200501", end_m="202607"))
probe("yc_cb 国债收益率曲线", lambda: pro.yc_cb(ts_code="1001.CB", curve_type="0", start_date="20050101", end_date="20260731"))
probe("shibor", lambda: pro.shibor(start_date="20250101", end_date="20260731"))
probe("shibor_lpr", lambda: pro.shibor_lpr(start_date="20190101", end_date="20260731"))
probe("cb_lpr(client)", lambda: c.get_lpr("20190101", "20260731"))
probe("us_tycr 美债", lambda: pro.us_tycr(start_date="20050101", end_date="20260731"))
probe("fx_daily USDCNY", lambda: pro.fx_daily(ts_code="USDCNY.FXCM", start_date="20200101", end_date="20260731"))
probe("libor", lambda: pro.libor(start_date="20250101", end_date="20260731"))
probe("hibor", lambda: pro.hibor(start_date="20250101", end_date="20260731"))
