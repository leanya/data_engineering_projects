from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import time
import yfinance as yf

from sql_queries_v1 import fact_temptable_create, dim_tempdate_create , table_drop, merge_table_stock, merge_table_date
from etl import download_table, extract_date_dim, retrieve_ticker_id

def create_temp_table(cur, conn):

    # Create temp tables for the date dimension and and stock fact table 

    for table in ['tempstock', 'tempdate']:
        cur.execute(sql.SQL(table_drop).format(sql.Identifier(table)))
        conn.commit()

    cur.execute(sql.SQL(dim_tempdate_create ).format(table = sql.Identifier('tempdate')))
    conn.commit()

    cur.execute(sql.SQL(fact_temptable_create).format(table = sql.Identifier('tempstock'),
                                                  date_table = sql.Identifier('tempdate'),
                                                  ticker_table = sql.Identifier('dim_ticker')))
    conn.commit()

def get_dates(cur):

    # Extract the latest date from the date_dim table
    # Download datasets starting from latest date + 1   

    cur.execute(sql.SQL("SELECT MAX(date) FROM {};").format(sql.Identifier('dim_date')) )
    last_updated = cur.fetchone()

    last_date = datetime.today() 
    last_date = last_date.strftime(('%Y-%m-%d'))

    start_date = last_updated[0] + timedelta(days=1)
    start_date = start_date.strftime(('%Y-%m-%d'))

    return start_date, last_date

def merge_data(cur, conn, first_date, last_date, companies, engine, ticker_df):

    # download data and load to the temp tables in postgresql
    # merge the temp tables with their corresponding tables in postgresql 
     
    data = yf.download(companies, start=first_date, end=last_date)
    ticker_map = dict(zip(ticker_df['ticker_symbol'], ticker_df['ticker_id']))
    cols = ['date', 'ticker_id', 'open', 'high', 'low', 'close', 'volume']

    date_df = extract_date_dim(data)
    date_df.to_sql('tempdate', engine, if_exists= 'replace', index=False)
    conn.commit()
    time.sleep(5)
    cur.execute(merge_table_date)
    conn.commit()

    for company in companies.split(' '):
        df = download_table(data, company)
        df['ticker_id'] = ticker_map[company]
        df = df[cols]
        df.to_sql('tempstock', engine, if_exists= 'replace', index=False)
        conn.commit()
        time.sleep(5)
        cur.execute(merge_table_stock)
        conn.commit()

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=stockdb user=student password=student")
    cur = conn.cursor()

    create_temp_table(cur, conn)

    first_date, last_date = get_dates(cur)
    print(first_date, last_date)
    companies = "SPY AAPL NVDA"

    engine = create_engine('postgresql://student:student@localhost:5432/stockdb')
    ticker_df = retrieve_ticker_id(engine)
    merge_data(cur, conn, first_date, last_date, companies, engine, ticker_df)
    engine.dispose()

    conn.close()
    print("Completed updating the tables!")

if __name__ == "__main__":
    main()
