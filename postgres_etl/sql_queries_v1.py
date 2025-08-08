# Create tables in PostgreSQL
# Fact table and Dimension tables

table_drop = "DROP TABLE IF EXISTS {}"

fact_table_create = ("""

CREATE TABLE {table} (
    date TIMESTAMP NOT NULL,
    ticker_id INTEGER NOT NULL,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    CONSTRAINT pk_date_ticker PRIMARY KEY (date, ticker_id),
    FOREIGN KEY (date) REFERENCES {date_table} (date),
    FOREIGN KEY (ticker_id) REFERENCES {ticker_table} (ticker_id)
);

""")
                
dim_date_table_create = ("""

CREATE TABLE {table} (
    date TIMESTAMP NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    dayofweek VARCHAR(10),
    CONSTRAINT pk_date PRIMARY KEY (date)
)
""")

dim_ticker_table_create = ("""

CREATE TABLE {table} (
    ticker_id SERIAL,
    ticker_symbol VARCHAR(10) NOT NULL,
    CONSTRAINT pk_ticker PRIMARY KEY (ticker_id)
)
""")

# Incremental Data Loading 

fact_temptable_create = ("""

CREATE TEMPORARY TABLE {table} (
    date TIMESTAMP NOT NULL,
    ticker_id INTEGER NOT NULL,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    CONSTRAINT pk_date_ticker PRIMARY KEY (date, ticker_id),
    FOREIGN KEY (date) REFERENCES {date_table} (date)
);

""")

dim_tempdate_create = ("""

CREATE TEMPORARY TABLE {table} (
    date TIMESTAMP NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    dayofweek VARCHAR(10),
    CONSTRAINT pk_date PRIMARY KEY (date)
)
""")

merge_table_stock = ("""

MERGE INTO public.fact_stock_price AS source 
USING public.tempstock AS temp
    ON source.date = temp.date AND source.ticker_id = temp.ticker_id
WHEN NOT MATCHED THEN
    INSERT (date, ticker_id, open, high, low, close, volume) 
    VALUES (temp.date, temp.ticker_id, temp.open, temp.high, temp.low, temp.close, temp.volume)
WHEN MATCHED THEN 
    UPDATE SET 
    open = temp.open, 
    high = temp.high, 
    low = temp.low, 
    close = temp.close, 
    volume= temp.volume;
                     
""") 

merge_table_date = ("""

MERGE INTO public.dim_date AS source 
USING public.tempdate AS temp
    ON source.date = temp.date
WHEN NOT MATCHED THEN
    INSERT (date, year, month, dayofweek) 
    VALUES (temp.date, temp.year, temp.month, temp.dayofweek)
WHEN MATCHED THEN 
    UPDATE SET 
    year = temp.year, 
    month = temp.month, 
    dayofweek = temp.dayofweek;
                    
""") 

