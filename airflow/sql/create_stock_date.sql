-- Drop tables

DROP TABLE IF EXISTS aapl;
DROP TABLE IF EXISTS spy;
DROP TABLE IF EXISTS date_dim;

-- Create tables

CREATE TABLE date_dim(
    date Timestamp PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day_of_week VARCHAR(10)
);

CREATE TABLE aapl(
    date Timestamp PRIMARY KEY,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    adj_close REAL NOT NULL,
    volume Integer NOT NULL,
    FOREIGN KEY (date) REFERENCES date_dim (date)
);

CREATE TABLE spy(
    date Timestamp PRIMARY KEY,
    open REAL NOT NULL, 
    high REAL NOT NULL, 
    low REAL NOT NULL, 
    close REAL NOT NULL,
    adjclose REAL NOT NULL,
    volume INTEGER NOT NULL,
    FOREIGN KEY (date) REFERENCES date_dim (date)
);

