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


def drop_tables(cur, conn, companies):
    # table to store date dimension
    cur.execute(sql.SQL(table_drop).format(sql.Identifier('date_dim')))
    conn.commit()

    # tables to store stock info
    for company in companies:
        cur.execute(sql.SQL(table_drop).format(sql.Identifier(company.lower() )))
        conn.commit()
    
def create_tables(cur, conn, companies):
    # table to store date dimension
    cur.execute(sql.SQL(date_table_create).format(table=sql.Identifier('date_dim' )))
    conn.commit()

    # tables to store stock info
    for company in companies:
        cur.execute(sql.SQL(table_create).format(table=sql.Identifier(company.lower() ),
                                                 date_table = sql.Identifier('date_dim'),
                                                 constraint= sql.Identifier("stock_key_"+company) ))
        conn.commit()

def main():
    cur, conn = create_database()
    companies = ['AAPL', "SPY"]
    
    drop_tables(cur, conn, companies)
    create_tables(cur, conn, companies)

    conn.close()
    print("Completed creating the tables!")


if __name__ == "__main__":
    main()