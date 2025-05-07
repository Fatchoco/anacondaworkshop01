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
