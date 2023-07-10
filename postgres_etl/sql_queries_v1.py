# Create tables in postgresql 
# Fact tables and Dimension tables (datetime)

table_drop = "DROP TABLE IF EXISTS {}"

table_create = ("""

CREATE TABLE {table} (
    date Timestamp NOT NULL,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    adjclose REAL NOT NULL,
    volume Integer NOT NULL,
    CONSTRAINT {constraint} PRIMARY KEY (date),
    FOREIGN KEY (date) REFERENCES {date_table} (date)
);

""")
                
date_table_create = ("""

CREATE TABLE {table}(
    date Timestamp NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    dayofweek VARCHAR(10),
    CONSTRAINT date_key PRIMARY KEY (date)
)
""")
                     
merge_table_stock = ("""

MERGE INTO {table} source 
USING tempstock temp
    ON source.date = temp.date

/* new records */
WHEN NOT MATCHED THEN
    INSERT (date, open, high, low, close, adjclose, volume) 
    VALUES (temp.date, temp.open, temp.high, temp.low, temp.close, temp.adjclose, temp.volume)

/* matching records ('inner match') */
WHEN MATCHED THEN 
    UPDATE SET open = temp.open, 
    high = temp.high, 
    low = temp.low, 
    close = temp.close, 
    adjclose = temp.adjclose , 
    volume= temp.volume
;

/* CREATE INDEX stock_index ON {table} (date); */

""") 

merge_table_date = ("""

MERGE INTO {table} source 
USING tempdate temp
    ON source.date = temp.date

/* new records */
WHEN NOT MATCHED THEN
    INSERT (date, year, month, dayofweek) 
    VALUES (temp.date, temp.year, temp.month, temp.dayofweek)

/* matching records ('inner match') */
WHEN MATCHED THEN 
    UPDATE SET year = temp.year, 
    month = temp.month, 
    dayofweek = temp.dayofweek
;

/* CREATE INDEX index ON {table}(date); */
REINDEX TABLE {table};

""") 

