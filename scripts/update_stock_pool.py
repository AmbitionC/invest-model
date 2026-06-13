#!/usr/bin/env python3
"""更新股票池：移除旧持仓，新增当前持仓。

用法：
    python3 scripts/update_stock_pool.py

变更说明（2026-06-13）：
  移除: 比亚迪(002594.SZ), 润泽科技(300442.SZ), 潞化科技(600691.SH)
  新增: 新金路(000510.SZ), 麦格米特(002851.SZ), 宁德时代(300750.SZ),
        中国卫星(600118.SH), 巨化股份(600160.SH)
  保留: 粤桂股份(000833.SZ), 卫星化学(002648.SZ), 化工ETF富国(516120.SH)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from invest_model.db import get_engine
from invest_model.repositories.stock_pool_repo import StockPoolRepository

REMOVE_CORE = ["002594.SZ", "300442.SZ", "600691.SH"]

ADD_CORE = [
    {"code": "000510.SZ", "name": "新金路",  "pool_group": "core", "tags": "化工,新能源"},
    {"code": "002851.SZ", "name": "麦格米特", "pool_group": "core", "tags": "电气设备,电力电子"},
    {"code": "300750.SZ", "name": "宁德时代", "pool_group": "core", "tags": "新能源,锂电池"},
    {"code": "600118.SH", "name": "中国卫星", "pool_group": "core", "tags": "航天,卫星"},
    {"code": "600160.SH", "name": "巨化股份", "pool_group": "core", "tags": "化工"},
]


def main():
    engine = get_engine()
    repo = StockPoolRepository(engine)

    print("=" * 50)
    print("股票池更新开始")
    print("=" * 50)

    # 显示当前池
    current = repo.get_pool()
    print(f"\n当前股票池（{len(current)} 只）：")
    for _, row in current.iterrows():
        print(f"  [{row['pool_group']:6}] {row['code']}  {row.get('name', '')}")

    # 移除旧标的
    print(f"\n移除 {len(REMOVE_CORE)} 只标的...")
    for code in REMOVE_CORE:
        try:
            repo.remove_from_pool(code, pool_group="core")
            print(f"  ✅ 移除: {code}")
        except Exception as e:
            print(f"  ⚠️  移除 {code} 失败: {e}")

    # 新增标的
    print(f"\n新增 {len(ADD_CORE)} 只标的...")
    try:
        df = pd.DataFrame(ADD_CORE)
        repo.batch_add_to_pool(df)
        for stock in ADD_CORE:
            print(f"  ✅ 新增: {stock['code']}  {stock['name']}")
    except Exception as e:
        print(f"  ❌ 批量新增失败: {e}")
        sys.exit(1)

    # 验证结果
    updated = repo.get_pool()
    print(f"\n更新后股票池（{len(updated)} 只）：")
    for _, row in updated.iterrows():
        print(f"  [{row['pool_group']:6}] {row['code']}  {row.get('name', '')}")

    core_codes = repo.get_pool_codes("core")
    etf_codes = repo.get_pool_codes("etf")
    print(f"\ncore 组: {len(core_codes)} 只 → {core_codes}")
    print(f"etf  组: {len(etf_codes)} 只 → {etf_codes}")
    print("\n✅ 股票池更新完成")


if __name__ == "__main__":
    main()
