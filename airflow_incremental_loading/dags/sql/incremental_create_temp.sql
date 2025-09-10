-- Incremental Data Loading

DROP TABLE IF EXISTS stage_date CASCADE;
DROP TABLE IF EXISTS stage_stock CASCADE;

CREATE TABLE stage_date(
    date TIMESTAMP NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    dayofweek VARCHAR(10),
    PRIMARY KEY (date)
);

CREATE TABLE stage_stock(
    date TIMESTAMP NOT NULL,
    ticker_id INTEGER NOT NULL,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    PRIMARY KEY (date, ticker_id),
    FOREIGN KEY (date) REFERENCES stage_date (date)
);