"""阿里云函数计算（FC）盯盘入口：定时触发器每 2~5 分钟调用一次 handler。

架构：无状态单次扫描（scripts/live_check.py 的 --once 模式）——
  取实时价 → 持仓风控/观察池买点评估 → 分级推送 → 退出（几秒）。
去重集从当日 GitHub issue「📟 盘中盯盘预警」的评论标记恢复，跨调用不重复报警；
卖出类风控每次触发即推，买点/提示类按 DIGEST_WINDOW 分钟的墙钟边界合并推。
非交易时段/节假日调用毫秒级退出（不打外部接口）。

函数环境变量（必需）：
  INVEST_DB_URL       mysql+pymysql://user:pass@host:3306/invest?charset=utf8mb4
  TUSHARE_TOKEN       Tushare token（rt_k 实时价）
  TUSHARE_HTTP_URL    如 https://minitick.top/
  GITHUB_TOKEN        fine-grained PAT，仅本仓库 Issues: Read and write
  GITHUB_REPOSITORY   AmbitionC/invest-model
可选：LIVE_HARD_STOP=0.08  LIVE_PULLBACK=0.03  LIVE_BUY_WEIGHT=0.05
     DIGEST_WINDOW=20（分钟，0=每次都推）  ONCE_STEP=3（=定时器间隔分钟数）

定时触发器（FC cron 为 UTC）：
  0 0/3 1-7 ? * MON-FRI     # 每 3 分钟，北京 09:00-15:59；时段守卫在脚本内

部署与打包见 docs/scheduled_update.md「方案 A'：FaaS 定时盯盘」。
"""

import os
import sys

# 仓库根目录加入 path（代码包结构：仓库整体打包，本文件位于 faas/ 下）
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def handler(event, context):  # noqa: ARG001 — FC 标准签名
    from scripts.live_check import run_once
    res = run_once()
    print(f"live-watch once: {res}")
    return str(res)


if __name__ == "__main__":   # 本地验证：python faas/live_watch_handler.py
    print(handler(None, None))
