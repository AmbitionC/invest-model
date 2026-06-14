"""MySQL 全部表的 DDL 定义"""

DDL_STATEMENTS: dict[str, str] = {
    "trade_calendar": """
        CREATE TABLE IF NOT EXISTS trade_calendar (
            cal_date VARCHAR(8) NOT NULL PRIMARY KEY,
            is_open TINYINT NOT NULL DEFAULT 0,
            pretrade_date VARCHAR(8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "stock_info": """
        CREATE TABLE IF NOT EXISTS stock_info (
            ts_code VARCHAR(16) NOT NULL PRIMARY KEY,
            symbol VARCHAR(10),
            name VARCHAR(32),
            area VARCHAR(16),
            industry VARCHAR(32),
            market VARCHAR(16),
            list_date VARCHAR(8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "stock_pool": """
        CREATE TABLE IF NOT EXISTS stock_pool (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(16) NOT NULL,
            name VARCHAR(32),
            pool_group VARCHAR(32) NOT NULL DEFAULT 'default',
            tags VARCHAR(256),
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_group (code, pool_group)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "stock_daily": """
        CREATE TABLE IF NOT EXISTS stock_daily (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            open DECIMAL(10,2),
            high DECIMAL(10,2),
            low DECIMAL(10,2),
            close DECIMAL(10,2),
            pre_close DECIMAL(10,2),
            `change` DECIMAL(10,2),
            pct_chg DECIMAL(10,4),
            volume DECIMAL(16,2),
            amount DECIMAL(16,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    "stock_fundamental": """
        CREATE TABLE IF NOT EXISTS stock_fundamental (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            pe_ttm DECIMAL(20,6),
            pb DECIMAL(20,6),
            ps_ttm DECIMAL(20,6),
            total_mv DECIMAL(20,4),
            circ_mv DECIMAL(20,4),
            turnover_rate DECIMAL(10,4),
            turnover_rate_f DECIMAL(10,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每日估值指标（daily_basic）'
    """,

    "stock_fina_indicator": """
        CREATE TABLE IF NOT EXISTS stock_fina_indicator (
            code VARCHAR(16) NOT NULL,
            ann_date VARCHAR(8),
            report_date VARCHAR(8) NOT NULL,
            eps DECIMAL(10,4),
            bps DECIMAL(10,4),
            roe DECIMAL(10,4),
            roa DECIMAL(10,4),
            gross_margin DECIMAL(10,4),
            debt_to_asset DECIMAL(10,4),
            revenue_yoy DECIMAL(10,4),
            profit_yoy DECIMAL(10,4),
            revenue DECIMAL(20,4),
            net_profit DECIMAL(20,4),
            ocfps DECIMAL(10,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, report_date),
            INDEX idx_ann_date (ann_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='季度财务指标（fina_indicator）'
    """,

    "stock_cashflow": """
        CREATE TABLE IF NOT EXISTS stock_cashflow (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            buy_sm_vol DECIMAL(16,2),
            sell_sm_vol DECIMAL(16,2),
            buy_md_vol DECIMAL(16,2),
            sell_md_vol DECIMAL(16,2),
            buy_lg_vol DECIMAL(16,2),
            sell_lg_vol DECIMAL(16,2),
            buy_elg_vol DECIMAL(16,2),
            sell_elg_vol DECIMAL(16,2),
            net_mf_vol DECIMAL(16,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资金流向（moneyflow）'
    """,

    "stock_northbound_flow": """
        CREATE TABLE IF NOT EXISTS stock_northbound_flow (
            trade_date VARCHAR(8) NOT NULL,
            ggt_ss DECIMAL(16,4) COMMENT '港股通(上海)净流入(亿)',
            ggt_sz DECIMAL(16,4) COMMENT '港股通(深圳)净流入(亿)',
            hgt DECIMAL(16,4) COMMENT '沪股通净流入(亿)',
            sgt DECIMAL(16,4) COMMENT '深股通净流入(亿)',
            north_money DECIMAL(16,4) COMMENT '北向合计净流入(亿)',
            south_money DECIMAL(16,4) COMMENT '南向合计净流入(亿)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='沪深港通每日资金流向'
    """,

    "stock_holder_trade": """
        CREATE TABLE IF NOT EXISTS stock_holder_trade (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(16) NOT NULL,
            ann_date VARCHAR(8),
            holder_name VARCHAR(128),
            holder_type VARCHAR(16),
            trade_type VARCHAR(16),
            change_vol DECIMAL(16,2),
            change_ratio DECIMAL(10,4),
            after_share DECIMAL(16,2),
            after_ratio DECIMAL(10,4),
            avg_price DECIMAL(10,2),
            begin_date VARCHAR(8),
            close_date VARCHAR(8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_holder (code, holder_name, begin_date, close_date),
            INDEX idx_code (code),
            INDEX idx_ann_date (ann_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股东增减持'
    """,

    "stock_holder_count": """
        CREATE TABLE IF NOT EXISTS stock_holder_count (
            code VARCHAR(16) NOT NULL,
            ann_date VARCHAR(8),
            end_date VARCHAR(8) NOT NULL,
            holder_num INT,
            holder_num_change DECIMAL(10,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, end_date),
            INDEX idx_ann_date (ann_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股东户数'
    """,

    "stock_margin": """
        CREATE TABLE IF NOT EXISTS stock_margin (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            rzye DECIMAL(20,4),
            rqye DECIMAL(20,4),
            rzmre DECIMAL(20,4),
            rqyl DECIMAL(16,2),
            rzche DECIMAL(20,4),
            rqchl DECIMAL(16,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='融资融券'
    """,

    "index_daily": """
        CREATE TABLE IF NOT EXISTS index_daily (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            open DECIMAL(10,2),
            high DECIMAL(10,2),
            low DECIMAL(10,2),
            close DECIMAL(10,2),
            pre_close DECIMAL(10,2),
            `change` DECIMAL(10,2),
            pct_chg DECIMAL(10,4),
            volume DECIMAL(20,2),
            amount DECIMAL(20,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数日线'
    """,

    "etf_info": """
        CREATE TABLE IF NOT EXISTS etf_info (
            ts_code VARCHAR(16) NOT NULL PRIMARY KEY,
            name VARCHAR(64),
            management VARCHAR(64),
            fund_type VARCHAR(32),
            found_date VARCHAR(8),
            list_date VARCHAR(8),
            invest_type VARCHAR(32),
            market VARCHAR(16),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF基础信息'
    """,

    "etf_daily": """
        CREATE TABLE IF NOT EXISTS etf_daily (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            open DECIMAL(10,4),
            high DECIMAL(10,4),
            low DECIMAL(10,4),
            close DECIMAL(10,4),
            pre_close DECIMAL(10,4),
            `change` DECIMAL(10,4),
            pct_chg DECIMAL(10,4),
            volume DECIMAL(16,2),
            amount DECIMAL(16,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF日线'
    """,

    "stock_technical": """
        CREATE TABLE IF NOT EXISTS stock_technical (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            boll_upper DECIMAL(10,4),
            boll_mid DECIMAL(10,4),
            boll_lower DECIMAL(10,4),
            macd_dif DECIMAL(10,4),
            macd_dea DECIMAL(10,4),
            macd_hist DECIMAL(10,4),
            rsi_6 DECIMAL(10,4),
            rsi_14 DECIMAL(10,4),
            ma60_bias DECIMAL(10,6),
            vol_ratio DECIMAL(10,4),
            momentum_20 DECIMAL(10,6),
            volatility_20 DECIMAL(10,6),
            ma5 DECIMAL(10,4),
            ma10 DECIMAL(10,4),
            ma20 DECIMAL(10,4),
            ma60 DECIMAL(10,4),
            ma120 DECIMAL(10,4),
            ma250 DECIMAL(10,4),
            is_limit_up TINYINT DEFAULT 0,
            is_limit_down TINYINT DEFAULT 0,
            is_st TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='技术指标'
    """,

    "etf_holding": """
        CREATE TABLE IF NOT EXISTS etf_holding (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(16) NOT NULL,
            report_date VARCHAR(8) NOT NULL,
            stock_code VARCHAR(16),
            stock_name VARCHAR(32),
            holding_amount DECIMAL(16,2),
            holding_mv DECIMAL(16,4),
            holding_ratio DECIMAL(10,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_holding (code, report_date, stock_code),
            INDEX idx_code (code),
            INDEX idx_report_date (report_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF重仓股持仓'
    """,

    "stock_signal_snapshot": """
        CREATE TABLE IF NOT EXISTS stock_signal_snapshot (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            signal_name VARCHAR(64) NOT NULL,
            category VARCHAR(32) NOT NULL,
            direction VARCHAR(16) NOT NULL,
            score DECIMAL(10,6) NOT NULL,
            strength VARCHAR(16) NOT NULL,
            label VARCHAR(255),
            indicator_values JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date, signal_name),
            INDEX idx_trade_date (trade_date),
            INDEX idx_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='单信号快照'
    """,

    "stock_composite_score": """
        CREATE TABLE IF NOT EXISTS stock_composite_score (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            tech_score DECIMAL(10,6),
            tech_rev_score DECIMAL(10,6),
            fund_score DECIMAL(10,6),
            flow_score DECIMAL(10,6),
            sent_score DECIMAL(10,6),
            composite DECIMAL(10,6) NOT NULL,
            rank_pct DECIMAL(10,6),
            summary TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date),
            INDEX idx_composite (trade_date, composite)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='综合评分'
    """,

    "backtest_run": """
        CREATE TABLE IF NOT EXISTS backtest_run (
            run_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(128) NOT NULL,
            strategy VARCHAR(64) NOT NULL,
            start_date VARCHAR(8) NOT NULL,
            end_date VARCHAR(8) NOT NULL,
            rebalance_days INT DEFAULT 5,
            top_k INT,
            params JSON,
            metrics JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_name (name),
            INDEX idx_created (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回测运行元信息'
    """,

    "backtest_nav": """
        CREATE TABLE IF NOT EXISTS backtest_nav (
            run_id INT NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            nav DECIMAL(16,6) NOT NULL,
            ret DECIMAL(10,6),
            turnover DECIMAL(10,6),
            position_count INT,
            PRIMARY KEY (run_id, trade_date),
            INDEX idx_run (run_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回测净值曲线'
    """,

    "backtest_trades": """
        CREATE TABLE IF NOT EXISTS backtest_trades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_id INT NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            code VARCHAR(16) NOT NULL,
            action VARCHAR(8) NOT NULL,
            weight DECIMAL(10,6),
            price DECIMAL(10,4),
            INDEX idx_run (run_id),
            INDEX idx_run_date (run_id, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回测换仓记录'
    """,

    "stock_advisor_signal": """
        CREATE TABLE IF NOT EXISTS stock_advisor_signal (
            code VARCHAR(16) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            action VARCHAR(16) NOT NULL,
            confidence INT NOT NULL,
            position_pct DECIMAL(5,4),
            composite DECIMAL(8,5),
            tech_score DECIMAL(8,5),
            fund_score DECIMAL(8,5),
            flow_score DECIMAL(8,5),
            sent_score DECIMAL(8,5),
            triggers JSON,
            attribution TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, trade_date),
            INDEX idx_trade_date (trade_date),
            INDEX idx_confidence (trade_date, confidence DESC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='逐票置信度操作建议'
    """,

    "stock_advisor_calibration": """
        CREATE TABLE IF NOT EXISTS stock_advisor_calibration (
            code VARCHAR(16) NOT NULL,
            calibrated_at VARCHAR(8) NOT NULL,
            window_days INT,
            composite_mean DECIMAL(8,5),
            composite_std DECIMAL(8,5),
            composite_p75 DECIMAL(8,5),
            composite_p90 DECIMAL(8,5),
            composite_p95 DECIMAL(8,5),
            abs_composite_values JSON,
            action_threshold INT DEFAULT 60,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='[deprecated] 标的置信度校准画像（已被 ml_model_registry 替代）'
    """,

    "ml_model_registry": """
        CREATE TABLE IF NOT EXISTS ml_model_registry (
            code VARCHAR(16) NOT NULL,
            horizon INT NOT NULL,
            version VARCHAR(32) NOT NULL,
            train_start VARCHAR(8) NOT NULL,
            train_end VARCHAR(8) NOT NULL,
            n_samples INT NOT NULL,
            n_features INT NOT NULL,
            feature_cols JSON,
            cv_avg_ic DECIMAL(8,5),
            cv_avg_rmse DECIMAL(8,5),
            cv_hit_rate DECIMAL(6,4),
            cv_metrics JSON,
            model_path VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, horizon),
            INDEX idx_version (version)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ML 模型注册表（XGBoost 多 horizon）'
    """,

    "model_health_log": """
        CREATE TABLE IF NOT EXISTS model_health_log (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            code        VARCHAR(16)  NOT NULL,
            version     VARCHAR(32)  NOT NULL,
            check_date  VARCHAR(8)   NOT NULL,
            status      ENUM('ok','degraded','missing') NOT NULL,
            cv_avg_ic   DECIMAL(8,5),
            checked_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_code_version_date (code, version, check_date),
            INDEX idx_code_version (code, version, check_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='模型健康状态日志（每日 IC 检查记录）'
    """,

    "macro_money_supply": """
        CREATE TABLE IF NOT EXISTS macro_money_supply (
            period_month VARCHAR(6)   NOT NULL PRIMARY KEY COMMENT 'YYYYMM',
            m0           DECIMAL(16,2) COMMENT 'M0余额（亿元）',
            m1           DECIMAL(16,2) COMMENT 'M1余额（亿元）',
            m2           DECIMAL(16,2) COMMENT 'M2余额（亿元）',
            m0_yoy       DECIMAL(8,4)  COMMENT 'M0同比增速(%)',
            m1_yoy       DECIMAL(8,4)  COMMENT 'M1同比增速(%)',
            m2_yoy       DECIMAL(8,4)  COMMENT 'M2同比增速(%)',
            m0_mom       DECIMAL(8,4),
            m1_mom       DECIMAL(8,4),
            m2_mom       DECIMAL(8,4),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='货币供应量M0/M1/M2月度数据'
    """,

    "macro_pmi": """
        CREATE TABLE IF NOT EXISTS macro_pmi (
            period_month       VARCHAR(6)  NOT NULL PRIMARY KEY COMMENT 'YYYYMM',
            pmi_mfg            DECIMAL(6,1) COMMENT '制造业PMI',
            pmi_mfg_new_orders DECIMAL(6,1),
            pmi_mfg_output     DECIMAL(6,1),
            pmi_mfg_emp        DECIMAL(6,1),
            pmi_mfg_input_price DECIMAL(6,1),
            pmi_service        DECIMAL(6,1) COMMENT '服务业PMI（非制造业）',
            pmi_composite      DECIMAL(6,1) COMMENT '综合PMI',
            created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='PMI月度数据'
    """,

    "macro_cpi_ppi": """
        CREATE TABLE IF NOT EXISTS macro_cpi_ppi (
            period_month VARCHAR(6)  NOT NULL PRIMARY KEY COMMENT 'YYYYMM',
            cpi_yoy      DECIMAL(6,2) COMMENT 'CPI同比(%)',
            cpi_mom      DECIMAL(6,2) COMMENT 'CPI环比(%)',
            cpi_nt_yoy   DECIMAL(6,2) COMMENT '全国CPI同比',
            ppi_yoy      DECIMAL(6,2) COMMENT 'PPI:工业品出厂同比(%)',
            ppi_mp_yoy   DECIMAL(6,2) COMMENT 'PPI:生产资料同比',
            ppi_raw_yoy  DECIMAL(6,2) COMMENT 'PPI:采掘工业同比',
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CPI/PPI月度数据'
    """,

    "macro_interest_rate": """
        CREATE TABLE IF NOT EXISTS macro_interest_rate (
            pub_date    VARCHAR(8)  NOT NULL PRIMARY KEY COMMENT 'YYYYMMDD',
            lpr_1y      DECIMAL(6,4) COMMENT '1年期LPR(%)',
            lpr_5y      DECIMAL(6,4) COMMENT '5年期LPR(%)',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='LPR利率'
    """,

    "northbound_top10": """
        CREATE TABLE IF NOT EXISTS northbound_top10 (
            trade_date   VARCHAR(8)   NOT NULL,
            market_type  VARCHAR(4)   NOT NULL COMMENT '1=沪股通 3=深股通',
            code         VARCHAR(16)  NOT NULL,
            name         VARCHAR(32),
            close        DECIMAL(10,2),
            rank         INT,
            net_amount   DECIMAL(16,2) COMMENT '净买入额（万元）',
            buy          DECIMAL(16,2),
            sell         DECIMAL(16,2),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, market_type, code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='北向资金每日十大成交股'
    """,
}

ALL_TABLES = list(DDL_STATEMENTS.keys())


_STOCK_FUNDAMENTAL_WIDEN_SQL = [
    "ALTER TABLE `stock_fundamental` MODIFY COLUMN `pe_ttm` DECIMAL(20,6) NULL",
    "ALTER TABLE `stock_fundamental` MODIFY COLUMN `pb` DECIMAL(20,6) NULL",
    "ALTER TABLE `stock_fundamental` MODIFY COLUMN `ps_ttm` DECIMAL(20,6) NULL",
    "ALTER TABLE `stock_fundamental` MODIFY COLUMN `total_mv` DECIMAL(20,4) NULL",
    "ALTER TABLE `stock_fundamental` MODIFY COLUMN `circ_mv` DECIMAL(20,4) NULL",
]

_STOCK_FUNDAMENTAL_ADD_TURNOVER_SQL = [
    "ALTER TABLE `stock_fundamental` ADD COLUMN `turnover_rate` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_fundamental` ADD COLUMN `turnover_rate_f` DECIMAL(10,4) NULL",
]

_STOCK_TECHNICAL_ADD_MA_AND_LIMITS_SQL = [
    "ALTER TABLE `stock_technical` ADD COLUMN `ma5` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_technical` ADD COLUMN `ma10` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_technical` ADD COLUMN `ma20` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_technical` ADD COLUMN `ma60` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_technical` ADD COLUMN `ma120` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_technical` ADD COLUMN `ma250` DECIMAL(10,4) NULL",
    "ALTER TABLE `stock_technical` ADD COLUMN `is_limit_up` TINYINT DEFAULT 0",
    "ALTER TABLE `stock_technical` ADD COLUMN `is_limit_down` TINYINT DEFAULT 0",
    "ALTER TABLE `stock_technical` ADD COLUMN `is_st` TINYINT DEFAULT 0",
]


def migrate_stock_fundamental_widen(engine) -> None:
    """
    放宽 stock_fundamental 数值列精度。旧库若曾用过过窄的 DECIMAL（如 ps 仅 5,4），
    会出现 1264 Out of range；CREATE TABLE IF NOT EXISTS 不会自动升级列类型。
    """
    from sqlalchemy import text

    with engine.begin() as conn:
        for sql in _STOCK_FUNDAMENTAL_WIDEN_SQL:
            conn.execute(text(sql))


def _safe_alter(engine, statements: list[str]) -> None:
    """逐条执行 ALTER TABLE，忽略"列已存在"错误（Duplicate column name）。"""
    from sqlalchemy import text

    for sql in statements:
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
        except Exception as e:
            if "Duplicate column name" in str(e):
                continue
            raise


def migrate_stock_fundamental_add_turnover(engine) -> None:
    """为 stock_fundamental 追加 turnover_rate / turnover_rate_f 两列。"""
    _safe_alter(engine, _STOCK_FUNDAMENTAL_ADD_TURNOVER_SQL)


def migrate_stock_technical_add_ma_and_limits(engine) -> None:
    """为 stock_technical 追加 MA 均线(6列) + 涨跌停标记(3列)。"""
    _safe_alter(engine, _STOCK_TECHNICAL_ADD_MA_AND_LIMITS_SQL)


def create_all_tables(engine) -> list[str]:
    """创建全部表，返回创建的表名列表"""
    from sqlalchemy import text
    created = []
    with engine.begin() as conn:
        for table_name, ddl in DDL_STATEMENTS.items():
            conn.execute(text(ddl))
            created.append(table_name)
    migrate_stock_fundamental_widen(engine)
    migrate_stock_fundamental_add_turnover(engine)
    migrate_stock_technical_add_ma_and_limits(engine)
    return created
