-- Drop tables

DROP TABLE IF EXISTS fact_stock_price;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_ticker;

-- Create tables

CREATE TABLE dim_date(
    date TIMESTAMP NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    dayofweek VARCHAR(10),
    CONSTRAINT pk_date PRIMARY KEY (date)
);

CREATE TABLE dim_ticker(
    ticker_id SERIAL,
    ticker_symbol VARCHAR(10) NOT NULL,
    CONSTRAINT pk_ticker PRIMARY KEY (ticker_id)
);

CREATE TABLE fact_stock_price(
    date TIMESTAMP NOT NULL,
    ticker_id INTEGER NOT NULL,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    CONSTRAINT pk_date_ticker PRIMARY KEY (date, ticker_id),
    FOREIGN KEY (date) REFERENCES dim_date (date),
    FOREIGN KEY (ticker_id) REFERENCES dim_ticker (ticker_id)
);



