import pytest

from anacondaworkshop01.util import Util

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
