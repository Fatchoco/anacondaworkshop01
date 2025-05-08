import sqlalchemy.engine.base

import conf
import pandas as pd
from typing import ClassVar

from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine, text
from datetime import datetime


class Util(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    engine: ClassVar = create_engine(r"duckdb:///" + conf.DUCKDB_FILEPATH)
    input_path: ClassVar = conf.PATH_INPUT
    output_path: ClassVar = conf.PATH_OUTPUT
    target_table: ClassVar = "raw_fund_eom_report"

    @staticmethod
    def load_all_files() -> None:
        Util._truncate_table()
        for date in conf.DATES:
            for fund, pattern in conf.FUNDS.items():
                Util._extract_and_load(fund_name=fund, report_date=date, file_pattern=pattern)

    @staticmethod
    def _truncate_table() -> None:
        table_name = Util.target_table
        with Util.engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            connection.commit()
            print(f"Table '{table_name}' has been truncated.")

    @staticmethod
    def _extract_and_load(file_pattern: str, fund_name: str, report_date: str) -> None:
        print(f"Processing: {fund_name} - {report_date} - {file_pattern}")
        df = Util._extract_file(file_pattern, fund_name, report_date)
        df.to_sql(Util.target_table, Util.engine, if_exists="append", index=False)

    @staticmethod
    def _extract_file(file_pattern, fund_name, report_date) -> pd.DataFrame:
        date_format = "%Y-%m-%d"
        date_obj = datetime.strptime(report_date, date_format)
        file_name = date_obj.strftime(file_pattern)
        df = pd.read_csv(f"{Util.input_path}{file_name}", dtype=str)
        df["REPORT DATE"] = report_date
        df["FUND NAME"] = fund_name
        df["_file_name"] = file_name
        return df

    @staticmethod
    def transform() -> None:
        with Util.engine.connect() as connection:
            Util._create_view(connection, "vw_fund_eom_report", """
                SELECT
                    "REPORT DATE",
                    "FUND NAME",
                    _file_name,
                    "FINANCIAL TYPE",
                    "SYMBOL",
                    "SECURITY NAME",
                    "SEDOL",
                    "ISIN",
                    CAST("PRICE" AS NUMERIC) as "PRICE",
                    CAST("QUANTITY" AS NUMERIC) as "QUANTITY",
                    CAST("REALISED P/L" AS NUMERIC) as "REALISED P/L",
                    CAST("MARKET VALUE" AS NUMERIC) as "MARKET VALUE",
                    _created_at
                FROM
                    raw_fund_eom_report
            """)

            Util._create_view(connection, "vw_bond_prices", """
                SELECT
                    strptime("DATETIME", '%Y-%m-%d')::DATE as "DATETIME",
                    "ISIN",
                    "PRICE"
                FROM
                    bond_prices
            """)

            Util._create_view(connection, "vw_equity_prices", """
                SELECT
                    strptime("DATETIME", '%m/%d/%Y')::DATE as "DATETIME",
                    "SYMBOL",
                    "PRICE"
                FROM
                    equity_prices
            """)

            Util._create_view(connection, "rpt01_reconciliation", """
                WITH fund AS (
                  SELECT * FROM vw_fund_eom_report WHERE "FINANCIAL TYPE" NOT IN ('CASH')
                ),
                base_equity AS (
                  -- Calculate the MAX DATETIME per month in the available data
                  SELECT
                    *,
                    MAX("DATETIME") OVER (PARTITION BY DATE_TRUNC('month', "DATETIME")) AS "MAX DATETIME"
                  FROM vw_equity_prices
                ),
                equity AS (
                  -- Mark the MAX DATETIME each month with End of Month date for future joining
                  SELECT
                    *,
                    (date_trunc('MONTH', "DATETIME") + INTERVAL '1 MONTH' - INTERVAL '1 DAY'):: DATE AS "EOM REPORT DATE"
                  FROM base_equity WHERE "MAX DATETIME" = "DATETIME"
                ),
                base_bond AS (
                  -- Calculate the MAX DATETIME per month in the available data
                  SELECT
                    *,
                    MAX("DATETIME") OVER (PARTITION BY DATE_TRUNC('month', "DATETIME")) AS "MAX DATETIME"
                  FROM vw_bond_prices
                ),
                bond AS (
                  -- Mark the MAX DATETIME each month with End of Month date for future joining
                  SELECT
                    *,
                    (date_trunc('MONTH', "DATETIME") + INTERVAL '1 MONTH' - INTERVAL '1 DAY'):: DATE AS "EOM REPORT DATE"
                  FROM base_bond WHERE "MAX DATETIME" = "DATETIME"
                ),
                base_report AS (
                  SELECT
                    fund."REPORT DATE",
                    COALESCE(equity."DATETIME", bond."DATETIME") AS "REF DATE",
                    fund."FUND NAME",
                    fund."FINANCIAL TYPE",
                    fund."SYMBOL",
                    fund."PRICE" AS "FUND PRICE",
                    COALESCE(equity."PRICE", bond."PRICE") AS "REF PRICE"
                  FROM
                    fund
                    LEFT OUTER JOIN equity ON fund."SYMBOL" = equity."SYMBOL"
                        AND fund."REPORT DATE" = equity."EOM REPORT DATE"
                    LEFT OUTER JOIN bond ON fund."SYMBOL" = bond."ISIN"
                        AND fund."REPORT DATE" = bond."EOM REPORT DATE"
                )
                SELECT
                  *,
                  ROUND("FUND PRICE" - "REF PRICE") AS "PRICE DIFF"
                FROM
                  base_report
                ORDER BY
                  1, 3, 4, 5
            """)

            Util._create_view(connection, "rpt02_bestfund", """
                with base_monthly AS (
                  SELECT
                    "REPORT DATE",
                    "FUND NAME",
                    ROUND(SUM("MARKET VALUE"), 2) as "Fund_MV",
                    ROUND(SUM("REALISED P/L"), 2) as "REALISED P/L"
                  FROM
                    vw_fund_eom_report
                  GROUP BY
                    1, 2
                ), metrics AS (
                SELECT 
                    "REPORT DATE",
                    "FUND NAME",
                    LAG("Fund_MV") OVER (PARTITION BY "FUND NAME" ORDER BY "REPORT DATE") as "Fund_MV_start",
                    "Fund_MV" as "Fund_MV_end",
                    (("Fund_MV_end" - "Fund_MV_start" + "REALISED P/L") / "Fund_MV_start") as "Rate of Return"
                FROM base_monthly
                )
                SELECT
                    "REPORT DATE",
                    "FUND NAME",
                    "Fund_MV_start",
                    "Fund_MV_end",
                    "Rate of Return",
                    RANK() OVER (PARTITION BY "REPORT DATE" ORDER BY "Rate of Return" DESC) as "Rank"
                FROM metrics
                WHERE "Rate of Return" IS NOT NULL
                QUALIFY "Rank" = 1
                ORDER BY "REPORT DATE", "Rank"
            """)

    @staticmethod
    def _create_view(connection: sqlalchemy.engine.base.Connection, view_name: str, select_statement: str) -> None:
        create_text = f"CREATE OR REPLACE VIEW {view_name} AS"
        connection.execute(text(f"""
                {create_text}
                {select_statement}
            """))
        connection.commit()
        print(f"View '{view_name}' has been created or replaced.")

    @staticmethod
    def export_reports() -> None:
        with Util.engine.connect() as connection:
            for report in ["rpt01_reconciliation", "rpt02_bestfund"]:
                output_file = f"{Util.output_path}{report}.xlsx"
                query = text(f"SELECT * FROM {report}")
                df = pd.read_sql(query, connection)
                df.to_excel(output_file, index=False)
                print(f"View '{report}' has been exported to '{output_file}'.")
