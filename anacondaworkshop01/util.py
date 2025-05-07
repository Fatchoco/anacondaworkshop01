from typing import ClassVar

from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine

class Util(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    engine: ClassVar = create_engine(r"duckdb:///../db/dbfile.db")

    @staticmethod
    def extract_load(fund_name: str, report_date: str, file_pattern: str) -> None:
        print(f"{fund_name} {report_date} {file_pattern}")
