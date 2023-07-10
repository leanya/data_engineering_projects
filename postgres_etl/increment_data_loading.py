from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import sql
import yfinance as yf

from sql_queries_v1 import table_create, date_table_create, table_drop, merge_table_stock, merge_table_date
from etl import extract_table, extract_date_dim

def create_temp_table(cur, conn, table_drop, table_create, date_table_create):

    # create temp tables for the date dimension and and stock table 
    
    for table in ['tempstock', 'tempdate']:
        cur.execute(sql.SQL(table_drop).format(sql.Identifier(table)))
        conn.commit()

    cur.execute(sql.SQL(date_table_create).format(table = sql.Identifier('tempdate')))
    conn.commit()

    cur.execute(sql.SQL(table_create).format(table = sql.Identifier('tempstock'),
                                             date_table = sql.Identifier('tempdate'),
                                             constraint= sql.Identifier("stock_key_tempstock" )))
    conn.commit()

def get_dates(cur):

    # extract the latest date from the date_dim table
    # we download datasets starting from latest date + 1   

    cur.execute(sql.SQL("SELECT MAX(date) FROM {};").format(sql.Identifier('date_dim')) )
    last_updated = cur.fetchone()

    last_date = datetime.today() 
    last_date = last_date.strftime(('%Y-%m-%d'))

    start_date = last_updated[0] + timedelta(days=1)
    start_date = start_date.strftime(('%Y-%m-%d'))

    return start_date, last_date

def merge_data(cur, conn, first_date, last_date, companies, engine):

    # download data and load to the temp tables in postgresql
    # merge the temp tables with their corresponding tables in postgresql 
     
    data = yf.download(companies, start=first_date, end=last_date)

    for company in companies.split(' '):
        df = extract_table(data, company)
        df.to_sql('tempstock', engine, if_exists= 'replace')
        cur.execute( sql.SQL(merge_table_stock).format(table = sql.Identifier(company.lower())) )
        conn.commit()
        
    date_df = extract_date_dim(data)
    date_df.to_sql('tempdate', engine, if_exists= 'replace')
    cur.execute(sql.SQL(merge_table_date).format(table = sql.Identifier("date_dim")) )
    conn.commit()

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=stockdb user=student password=student")
    cur = conn.cursor()

    create_temp_table(cur, conn, table_drop, table_create, date_table_create)

    first_date, last_date = get_dates(cur)

    companies = "AAPL SPY"
    engine = create_engine('postgresql://student:student@localhost:5432/stockdb')
    merge_data(cur, conn, first_date, last_date, companies, engine)
    engine.dispose()

    conn.close()
    print("Completed updating the tables!")

if __name__ == "__main__":
    main()
