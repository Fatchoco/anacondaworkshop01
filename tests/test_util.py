from typing import ClassVar

from sqlalchemy import create_engine, inspect
from anacondaworkshop01.util import Util
from pydantic import BaseModel, ConfigDict

class Helper(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    engine: ClassVar = create_engine(r"duckdb:///:memory:")
    connection: ClassVar = engine.connect()

    @staticmethod
    def is_view_exist(table_name):
        inspector = inspect(Helper.engine)
        if not inspector.has_table(table_name):
            raise Exception(f"Table {table_name} does not exist. Terminating process.")
        return True


def test_true():
    assert True

def test__extract_file(mocker):
    report_date = "2023-02-28"
    fund_name = "Testfund"
    file_pattern = "Testfund.%d-%m-%Y breakdown.csv"
    mocker.patch("anacondaworkshop01.util.Util.input_path", 'tests/files/')

    df = Util._extract_file(file_pattern, fund_name, report_date)

    assert 'REPORT DATE' in df.columns
    assert 'FUND NAME' in df.columns
    assert '_file_name' in df.columns
    assert df['REPORT DATE'].iloc[0] == report_date
    assert df['FUND NAME'].iloc[0] == fund_name

def test__create_view():
    Util._create_view(Helper.connection, "vw_test", "SELECT 'abc' as col_1")
    assert Helper.is_view_exist("vw_test")
