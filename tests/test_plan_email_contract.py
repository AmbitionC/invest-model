"""计划邮件呈现契约（owner 2026-08-08）回归测试。

契约：邮件给人读的要点版——
① 头部不再有「模型置信度」「标的由投顾定…」两行（model_conf_label 仍落库供网站）；
② 投顾风向计数行撤下，仅环境闸收紧时出一句人话；
③ 宽基块收敛为一条「人话汇总」行：位置+要不要动+离动作点多远，零策略编号；
④ 乖离率未进近十年前 4 不进邮件；触发行为人话（无 E 编号）；
⑤ 财务预警并入表格「财务预警」列，不再单独占提示行；
⑥ 所有 hint 无 markdown 强调符、无 ASCII " | "（前端按它切分 risk_hints）。
"""
import re

from invest_model.orchestration.action_plan import (
    ActionPlan,
    _and_leverage_hint,
    _bias_extreme_hint,
    _broad_digest_hint,
    _table,
    render_markdown,
)

_STS = [
    {"name": "沪深300", "etf": "510300", "last": 4694.0, "med": 3437.0,
     "buy_mul": 1.0, "sell_mul": 1.30, "state": "sell", "fear": 26.0,
     "dd": None, "ladder_line": None, "data_date": "20260807"},
    {"name": "创业板", "etf": "159915", "last": 2308.0, "med": 2200.0,
     "buy_mul": 1.0, "sell_mul": 1.43, "state": "hold", "fear": 26.0,
     "dd": None, "ladder_line": None, "data_date": "20260801"},   # 陈旧腿
    {"name": "科创50", "etf": "588000", "last": 1010.0, "med": 990.0,
     "buy_mul": 1.0, "sell_mul": 1.30, "state": "hold", "fear": 26.0,
     "dd": -0.38, "ladder_line": 820.0, "data_date": "20260807"},
    {"name": "红利", "etf": "515080", "last": 1450.0, "med": 1500.0,
     "buy_mul": 1.0, "sell_mul": 1.30, "state": "buy", "fear": 26.0,
     "dd": None, "ladder_line": None, "data_date": "20260807"},
]
_LEV_OFF = {"active": False, "low": False, "panic": False, "close": 4694.0,
            "median": 3437.0, "gap": 0.366, "fear": 26.0, "p28_count": 0}


def _mk_plan(risk_hints: str | None = None, rows: list | None = None) -> ActionPlan:
    return ActionPlan(
        plan_date="20260807",
        rows=rows or [],
        account={"plan_date": "20260807", "equity": 100000.0, "invested_pct": 0.5,
                 "cash_pct": 0.5, "n_holdings": 1, "unrealized_pnl_pct": 0.01,
                 "gross_target": 0.6, "risk_off": False,
                 "model_ic_mean": 0.02, "model_ic_ir": 0.5, "model_hit": 0.55,
                 "model_conf_label": "中", "risk_hints": risk_hints},
        etf_watch=[], footer=None)


def _row(code: str, action: str = "watch", fin: str | None = None) -> dict:
    r = {"plan_date": "20260807", "code": code, "name": "测试股", "action": action,
         "cur_weight": 0.0, "tgt_weight": 0.0, "shares_delta": 0.0, "reason": "观察",
         "stop_price": None, "ref_price": 10.0, "grade": "B", "trigger": "—",
         "model_rank": None, "model_view": "—"}
    if fin:
        r["fin_alert"] = fin
    return r


def test_header_dropped_lines():
    md = render_markdown(_mk_plan())
    assert "模型置信度" not in md
    assert "标的由投顾定" not in md
    assert "rulebook" not in md
    # 头两行基础账户信息仍在
    assert "总权益" in md and "持仓占比" in md


def test_digest_hint_plain_language():
    h = _broad_digest_hint(_STS, _LEV_OFF, "20260807")
    assert h is not None
    # 零策略编号 / 零验证编号
    assert not re.search(r"[PE]\d+", h)
    # 无 markdown 强调符、无 ASCII 分隔（前端按 " | " 切分 risk_hints）
    assert "**" not in h and " | " not in h
    # 四腿位置齐全 + 各状态的人话动作
    assert "沪深300 4694" in h and "只卖不买" in h and "每月减 5%" in h
    assert "跌破 2200 才买" in h and "涨过 3146 才减" in h
    assert "距历史最高点 -38%" in h and "跌破 820" in h
    assert "已低于买点 1500" in h and "可分批买" in h
    # 陈旧腿必须可见（创业板价格停在 0801）
    assert "⚠️价格还停在20260801" in h
    # 恐慌与杠杆状态并入行尾
    assert "恐慌指数 26" in h and "加杠杆信号未触发" in h


def test_digest_renders_into_email():
    h = _broad_digest_hint(_STS, _LEV_OFF, "20260807")
    md = render_markdown(_mk_plan(risk_hints=h))
    assert "宽基指数今天的位置" in md
    # 旧术语长行不再出现
    for gone in ("P26·提示", "P27 v2", "容错自检（P51", "宽基不动（P52", "AND 共振"):
        assert gone not in md


def test_bias_hint_only_on_extreme():
    calm = [{"name": "创业板", "bias60": -0.153, "rank_low": 13, "rank_high": 900,
             "win": 2500, "extreme": "", "date": "20260807"}]
    assert _bias_extreme_hint(calm, "20260807") is None
    hot = [{"name": "创业板", "bias60": -0.253, "rank_low": 2, "rank_high": 2400,
            "win": 2500, "extreme": "low", "date": "20260807"}]
    h = _bias_extreme_hint(hot, "20260807")
    assert h and h.startswith("🚨") and "近十年第 2 低" in h
    assert not re.search(r"E\d+", h)          # 验证编号不进邮件
    assert "不是买卖信号" in h                 # 裁决与读数同屏（治理红线保留）


def test_leverage_hint_active_plain():
    st = {"active": True, "low": True, "panic": True, "close": 3270.0,
          "median": 3437.0, "gap": -0.05, "fear": 80.0, "p28_count": 2}
    h = _and_leverage_hint(st)
    assert h and h.startswith("🚨🚨") and "30%" in h and "手动" in h
    assert "P30" not in h and "AND 共振" not in h


def test_fin_alert_column():
    # 有预警：加「财务预警」列且值就位
    lines: list[str] = []
    _table(lines, [_row("000001.SZ", fin="⚠️3项：应收激增"), _row("000002.SZ")])
    assert lines[0].endswith("| 模型研判 | 财务预警 |")
    assert "⚠️3项：应收激增 |" in lines[2]
    assert lines[3].rstrip().endswith("| — |")
    # 无预警：保持 11 列，不加空列
    lines2: list[str] = []
    _table(lines2, [_row("000001.SZ")])
    assert lines2[0].endswith("| 模型研判 |")
    assert "财务预警" not in lines2[0]
