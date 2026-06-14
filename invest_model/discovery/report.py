"""discovery_candidates 表的写入与管理。

提供：
  save_candidates(candidates, engine)           — 批量写入，跳过重复
  promote_candidate(code, pool_group, engine)   — 将候选升级到 stock_pool
  dismiss_candidate(code, engine)               — 将候选标记为已忽略
  list_pending(engine, scan_date)               — 查询当前有效候选
"""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

if TYPE_CHECKING:
    from invest_model.discovery.stock_screener import StockCandidate
    from invest_model.discovery.etf_rotator import ETFCandidate

logger = get_logger()


def save_candidates(
    candidates: list,  # list[StockCandidate | ETFCandidate]
    engine: Engine,
    code_name_map: dict[str, str] | None = None,
) -> int:
    """批量写入 discovery_candidates，已存在（同 code + scan_date + source）则跳过。

    Returns
    -------
    int
        实际新增条数。
    """
    if not candidates:
        return 0

    code_name_map = code_name_map or {}
    repo = BaseRepository(engine)

    rows = []
    for c in candidates:
        name = code_name_map.get(c.code, c.name or "")
        rows.append(
            {
                "code": c.code,
                "name": name,
                "source": c.source,
                "score": round(float(c.score), 4) if c.score is not None else None,
                "reason": c.reason or "",
                "status": "pending",
                "expire_date": c.expire_date,
                "scan_date": c.scan_date,
            }
        )

    df = pd.DataFrame(rows)

    # 查询已存在记录（同 code + scan_date + source），避免重复插入
    existing = _load_existing_keys(engine, df)

    new_rows = []
    for _, row in df.iterrows():
        key = (row["code"], row["scan_date"], row["source"])
        if key not in existing:
            new_rows.append(row.to_dict())

    if not new_rows:
        logger.info("[Discovery] 无新候选需要写入（全部已存在）")
        return 0

    new_df = pd.DataFrame(new_rows)
    repo.insert_df("discovery_candidates", new_df)
    logger.info(f"[Discovery] 写入 {len(new_rows)} 条新候选")
    return len(new_rows)


def promote_candidate(
    code: str,
    pool_group: str,
    engine: Engine,
    name: str = "",
    tags: str = "",
) -> bool:
    """将候选升级到 stock_pool，并将 discovery_candidates 状态改为 'promoted'。

    Returns
    -------
    bool
        是否成功（若已在池内则返回 False）
    """
    repo = BaseRepository(engine)

    # 检查是否已在池内
    existing = repo.read_sql(
        "SELECT code FROM stock_pool WHERE code = :code LIMIT 1",
        {"code": code},
    )
    if not existing.empty:
        logger.warning(f"[Discovery] {code} 已在 stock_pool，跳过 promote")
        return False

    # 若未提供名称，从 discovery_candidates 取
    if not name:
        row = repo.read_sql(
            "SELECT name FROM discovery_candidates WHERE code = :code LIMIT 1",
            {"code": code},
        )
        if not row.empty:
            name = row.iloc[0]["name"] or ""

    # 写入 stock_pool
    repo.insert_df(
        "stock_pool",
        pd.DataFrame(
            [{"code": code, "name": name, "pool_group": pool_group, "tags": tags}]
        ),
    )

    # 更新状态
    with engine.begin() as conn:
        conn.execute(
            __import__("sqlalchemy").text(
                "UPDATE discovery_candidates SET status='promoted' WHERE code=:code AND status='pending'"
            ),
            {"code": code},
        )

    logger.info(f"[Discovery] {code} 已升级到 stock_pool ({pool_group})")
    return True


def dismiss_candidate(code: str, engine: Engine) -> None:
    """将候选标记为 dismissed（已忽略）。"""
    import sqlalchemy

    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(
                "UPDATE discovery_candidates SET status='dismissed' WHERE code=:code AND status='pending'"
            ),
            {"code": code},
        )
    logger.info(f"[Discovery] {code} 已标记为 dismissed")


def list_pending(
    engine: Engine,
    trade_date: str | None = None,
) -> pd.DataFrame:
    """返回当前有效（pending 且未过期）的候选列表。"""
    repo = BaseRepository(engine)
    td = trade_date or "99999999"
    return repo.read_sql(
        """
        SELECT code, name, source, score, reason, scan_date, expire_date
        FROM discovery_candidates
        WHERE status = 'pending' AND expire_date >= :td
        ORDER BY score DESC, scan_date DESC
        """,
        {"td": td},
    )


# ── 内部工具 ─────────────────────────────────────────

def _load_existing_keys(engine: Engine, df: pd.DataFrame) -> set[tuple]:
    """批量查询已存在的 (code, scan_date, source) 三元组，避免重复插入。"""
    if df.empty:
        return set()
    repo = BaseRepository(engine)
    codes = df["code"].unique().tolist()
    try:
        existing = repo.read_sql(
            "SELECT code, scan_date, source FROM discovery_candidates WHERE code IN :codes",
            {"codes": tuple(codes) if len(codes) > 1 else (codes[0],)},
        )
        return {(r.code, r.scan_date, r.source) for r in existing.itertuples()}
    except Exception:
        return set()
