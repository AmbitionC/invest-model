"""连通性冒烟检查：验证 DB 与 Tushare 凭证是否可用（不写库、不拉大数据）。

用途：
- 本地：配好 .env / 环境变量后 `python scripts/smoke_check.py` 快速自检。
- CI：GitHub Actions 的 smoke-test 工作流调用，验证 Secrets 配置是否正确。

退出码 0=全部通过；非 0=有检查失败（CI 会因此标红）。不打印任何凭证明文。
"""

from __future__ import annotations

import sys


def check_db() -> bool:
    try:
        from sqlalchemy import text

        from invest_model.data.engine import make_engine, resolve_db_url

        url = resolve_db_url(None)
        backend = url.split("://", 1)[0]  # 只显示后端类型，不泄露主机/账号
        eng = make_engine()
        with eng.connect() as c:
            one = c.execute(text("SELECT 1")).scalar()
            db = c.execute(text("SELECT DATABASE()")).scalar() if backend.startswith("mysql") else "(sqlite)"
        ok = one == 1
        print(f"[DB]      {'OK ' if ok else 'FAIL'} backend={backend} database={db}")
        return ok
    except Exception as e:  # noqa: BLE001
        print(f"[DB]      FAIL {type(e).__name__}: {e}")
        return False


def check_tushare() -> bool:
    try:
        from invest_model.sources.tushare_client import TushareClient

        pro = TushareClient().pro
        df = pro.query("trade_cal", exchange="SSE", start_date="20240102", end_date="20240105")
        n = 0 if df is None else len(df)
        ok = n > 0
        print(f"[Tushare] {'OK ' if ok else 'FAIL'} trade_cal 返回 {n} 行")
        return ok
    except Exception as e:  # noqa: BLE001
        print(f"[Tushare] FAIL {type(e).__name__}: {e}")
        return False


def main() -> None:
    print("== invest-model 连通性冒烟检查 ==")
    db_ok = check_db()
    ts_ok = check_tushare()
    if db_ok and ts_ok:
        print("== 全部通过 ==")
        sys.exit(0)
    print("== 存在失败项，请检查上面 FAIL 行对应的 Secrets/网络 ==")
    sys.exit(1)


if __name__ == "__main__":
    main()

# re-run trigger: 验证建库后 DB 连通（无害注释）
