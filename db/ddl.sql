-- Table to store Fund report
DROP TABLE IF EXISTS "raw_fund_eom_report";
CREATE TABLE IF NOT EXISTS "raw_fund_eom_report" (
    "REPORT DATE" DATE,
    "FUND NAME" TEXT,
    _file_name TEXT,
    "FINANCIAL TYPE" TEXT,
    "SYMBOL" TEXT,
    "SECURITY NAME" TEXT,
    "SEDOL" TEXT,
    "ISIN" TEXT,
    "PRICE" TEXT,
    "QUANTITY" TEXT,
    "REALISED P/L" TEXT,
    "MARKET VALUE" TEXT,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_fund_eom_report ON raw_fund_eom_report("REPORT DATE", "FUND NAME");
