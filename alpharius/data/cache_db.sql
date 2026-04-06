CREATE TABLE IF NOT EXISTS time_range (
    symbol TEXT PRIMARY KEY,
    time_range TEXT
);

CREATE TABLE IF NOT EXISTS chart (
    symbol TEXT,
    date TEXT,
    time TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    UNIQUE (symbol, time)
);

CREATE INDEX IF NOT EXISTS idx_chart_symbol ON chart (symbol);
CREATE INDEX IF NOT EXISTS idx_chart_date ON chart (date);
CREATE INDEX IF NOT EXISTS idx_chart_symbol_date ON chart (symbol, date);
