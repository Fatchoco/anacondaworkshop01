```shell
# Install project
poetry install

## make sure to activate virtualenv

# Testing
pytest

    ======================================================================================================= test session starts =======================================================================================================
    platform win32 -- Python 3.11.9, pytest-8.3.5, pluggy-1.5.0
    rootdir: C:\Users\djasmeie\Documents\localrepo\anacondaworkshop01
    configfile: pyproject.toml
    plugins: mock-3.14.0
    collected 3 items                                                                                                                                                                                                                  
    
    tests\test_util.py ...                                                                                                                                                                                                       [100%]


# Execute
cd anacondaworkshop01
python -m main.py

    Table 'raw_fund_eom_report' has been truncated.
    Processing: Applebead - 2022-08-31 - Applebead.%d-%m-%Y breakdown.csv
    Processing: Belaware - 2022-08-31 - Belaware.%d_%m_%Y.csv
    Processing: Fund Whitestone - 2022-08-31 - Fund Whitestone.%d-%m-%Y - details.csv
    ...
    Processing: Wallington - 2023-08-31 - mend-report Wallington.%d_%m_%Y.csv
    Processing: Catalysm - 2023-08-31 - rpt-Catalysm.%Y-%m-%d.csv
    View 'vw_fund_eom_report' has been created or replaced.
    View 'vw_bond_prices' has been created or replaced.
    View 'vw_equity_prices' has been created or replaced.
    View 'rpt01_reconciliation' has been created or replaced.
    View 'rpt02_bestfund' has been created or replaced.
    View 'rpt01_reconciliation' has been exported to '../output_report/rpt01_reconciliation.xlsx'.
    View 'rpt02_bestfund' has been exported to '../output_report/rpt02_bestfund.xlsx'.

```
