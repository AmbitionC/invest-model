"""
数据质量校验器

校验规则：
1. 缺失天数检测 - 对比交易日历，发现遗漏的交易日
2. 数据连续性 - 检查日期是否有断层
3. 涨跌幅异常 - 超过 ±22%（含 20% 涨跌停 + 2% 容差）
4. 价格合理性 - 零价、负价、收盘价在最高最低之间
5. 成交量异常 - 极低成交量预警
6. 交叉验证 - 多数据源对比（可选）
"""

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from invest_model.config import load_config
from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.calendar_repo import CalendarRepository

logger = get_logger()


@dataclass
class ValidationResult:
    """单项校验结果"""
    code: str
    check_name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class ValidationReport:
    """校验报告"""
    run_time: str
    total_checks: int = 0
    passed: int = 0
    warnings: int = 0
    errors: int = 0
    results: list[ValidationResult] = field(default_factory=list)

    def add(self, result: ValidationResult):
        self.total_checks += 1
        if result.passed:
            self.passed += 1
        else:
            self.warnings += 1
        self.results.append(result)

    def summary(self) -> str:
        lines = [
            f"=== 数据质量报告 ({self.run_time}) ===",
            f"总检查项: {self.total_checks}",
            f"通过: {self.passed}",
            f"警告: {self.warnings}",
            "",
        ]
        for r in self.results:
            status = "PASS" if r.passed else "WARN"
            lines.append(f"  [{status}] {r.code} - {r.check_name}: {r.message}")
        return "\n".join(lines)

    def to_dataframe(self) -> pd.DataFrame:
        records = []
        for r in self.results:
            records.append({
                "code": r.code,
                "check": r.check_name,
                "passed": r.passed,
                "message": r.message,
            })
        return pd.DataFrame(records)


class DataValidator:
    """数据质量校验器"""

    def __init__(self, engine):
        self.engine = engine
        self.cfg = load_config().get("validation", {})
        self.base_repo = BaseRepository(engine)
        self.cal_repo = CalendarRepository(engine)

    def validate_stock_daily(self, codes: list[str],
                              start_date: str = "20210101",
                              end_date: str | None = None) -> ValidationReport:
        """对 stock_daily 表做全面校验"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        report = ValidationReport(run_time=datetime.now().strftime("%Y-%m-%d %H:%M"))

        for code in codes:
            self._check_missing_days(report, code, "stock_daily", start_date, end_date)
            self._check_price_validity(report, code, start_date, end_date)
            self._check_pct_chg(report, code, start_date, end_date)
            self._check_volume(report, code, start_date, end_date)

        return report

    def validate_all_tables(self) -> ValidationReport:
        """对所有表做基础校验（行数统计 + 时间范围）"""
        report = ValidationReport(run_time=datetime.now().strftime("%Y-%m-%d %H:%M"))

        tables = [
            "trade_calendar", "stock_info", "stock_pool",
            "stock_daily", "stock_technical", "stock_fundamental",
            "stock_fina_indicator", "stock_cashflow",
            "stock_holder_trade", "stock_holder_count",
            "stock_margin", "index_daily",
            "etf_info", "etf_daily", "etf_holding",
        ]

        for table in tables:
            try:
                if not self.base_repo.table_exists(table):
                    report.add(ValidationResult(
                        code=table, check_name="表存在性",
                        passed=False, message="表不存在",
                    ))
                    continue

                count = self.base_repo.get_row_count(table)
                report.add(ValidationResult(
                    code=table, check_name="数据量",
                    passed=count > 0,
                    message=f"{count} 行" if count > 0 else "空表",
                    details={"row_count": count},
                ))
            except Exception as e:
                report.add(ValidationResult(
                    code=table, check_name="数据量",
                    passed=False, message=f"查询失败: {e}",
                ))

        return report

    def _check_missing_days(self, report: ValidationReport, code: str,
                            table: str, start_date: str, end_date: str):
        """检查缺失交易日"""
        max_missing = self.cfg.get("max_missing_days", 5)
        missing = self.cal_repo.get_missing_trade_dates(table, code, start_date, end_date)

        passed = len(missing) <= max_missing
        report.add(ValidationResult(
            code=code, check_name="缺失交易日",
            passed=passed,
            message=f"缺失 {len(missing)} 天" + (f" (前5: {missing[:5]})" if missing else ""),
            details={"missing_count": len(missing), "missing_dates": missing[:20]},
        ))

    def _check_price_validity(self, report: ValidationReport, code: str,
                               start_date: str, end_date: str):
        """检查价格合理性"""
        df = self.base_repo.read_sql(
            "SELECT trade_date, open, high, low, close FROM stock_daily "
            "WHERE code = :code AND trade_date BETWEEN :s AND :e",
            {"code": code, "s": start_date, "e": end_date},
        )
        if df.empty:
            return

        # 零价/负价
        zero_mask = (df["close"] <= 0) | (df["open"] <= 0)
        zero_count = zero_mask.sum()

        # close 应在 low-high 之间
        range_mask = (df["close"] > df["high"]) | (df["close"] < df["low"])
        range_err = range_mask.sum()

        issues = zero_count + range_err
        report.add(ValidationResult(
            code=code, check_name="价格合理性",
            passed=issues == 0,
            message=f"零/负价: {zero_count}, 范围异常: {range_err}",
            details={"zero_price": int(zero_count), "range_error": int(range_err)},
        ))

    def _check_pct_chg(self, report: ValidationReport, code: str,
                        start_date: str, end_date: str):
        """检查涨跌幅异常"""
        max_pct = self.cfg.get("max_pct_chg", 22.0)
        df = self.base_repo.read_sql(
            "SELECT trade_date, pct_chg FROM stock_daily "
            "WHERE code = :code AND trade_date BETWEEN :s AND :e AND ABS(pct_chg) > :max_pct",
            {"code": code, "s": start_date, "e": end_date, "max_pct": max_pct},
        )
        report.add(ValidationResult(
            code=code, check_name="涨跌幅异常",
            passed=len(df) == 0,
            message=f"超过 ±{max_pct}% 的记录: {len(df)} 条",
            details={"abnormal_count": len(df)},
        ))

    def _check_volume(self, report: ValidationReport, code: str,
                       start_date: str, end_date: str):
        """检查成交量异常"""
        min_vol = self.cfg.get("min_volume_warn", 100)
        df = self.base_repo.read_sql(
            "SELECT trade_date, volume FROM stock_daily "
            "WHERE code = :code AND trade_date BETWEEN :s AND :e AND volume < :min_vol AND volume > 0",
            {"code": code, "s": start_date, "e": end_date, "min_vol": min_vol},
        )
        report.add(ValidationResult(
            code=code, check_name="低成交量",
            passed=len(df) <= 5,
            message=f"成交量 < {min_vol} 手的记录: {len(df)} 条",
            details={"low_volume_count": len(df)},
        ))
