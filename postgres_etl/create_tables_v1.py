import psycopg2
from psycopg2 import sql
from sql_queries_v1 import *


def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create stockdb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS stockdb")
    cur.execute("CREATE DATABASE stockdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify stockdb 
    conn = psycopg2.connect("host=127.0.0.1 dbname=stockdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    
    # List of table names to drop
    tables = ['fact_stock_price', 'dim_date', 'dim_ticker']

    for table in tables: 
        drop_query = sql.SQL(table_drop).format(sql.Identifier(table))
        cur.execute(drop_query)
        conn.commit()
    
def create_tables(cur, conn):
    # table to store date dimension
    cur.execute(sql.SQL(dim_date_table_create).format(table=sql.Identifier('dim_date')))
    conn.commit()
    
    # table to store ticker dimension
    cur.execute(sql.SQL(dim_ticker_table_create).format(table=sql.Identifier('dim_ticker')))
    conn.commit()

    # table to store stock info
    cur.execute(sql.SQL(fact_table_create).format(table=sql.Identifier('fact_stock_price'),
                                                  date_table = sql.Identifier('dim_date'),
                                                  ticker_table = sql.Identifier('dim_ticker') ))
    conn.commit()

def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Completed creating the tables!")


if __name__ == "__main__":
    main()