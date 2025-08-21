from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import yfinance as yf

def setup_ticker(companies):

    conn = PostgresHook(postgres_conn_id = 'postgres').get_conn()
    cur = conn.cursor()

    # Insert tickers
    for t in companies:
        cur.execute(
            "INSERT INTO dim_ticker (ticker_symbol) VALUES (%s) ON CONFLICT DO NOTHING;",(t,)
        )
    conn.commit()
    conn.close()

def retrieve_ticker(**kwargs):

    conn = PostgresHook(postgres_conn_id = 'postgres').get_conn()
    cur = conn.cursor()

    cur.execute("SELECT ticker_id, ticker_symbol FROM dim_ticker;")
    rows = cur.fetchall() 
    ticker_map = {row[1]: row[0] for row in rows}
    
    cur.close()
    conn.close()
    kwargs['ti'].xcom_push(key='ticker_map', value=ticker_map)

def download_table(data, company):
    
    # extract individual company info from data 
    # remove multi-level index and tidy up column names 
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = data.loc[:, (cols, company)].copy()
    df.columns = df.columns.droplevel(1)
    df = df.reset_index()
    df.columns = [x.replace(" ", "").lower() for x in list(df.columns)]
    return df 

def extract_date_dim(data):

    # create date dimension table 
    date_dim = pd.DataFrame(list(data.index), columns =['date'])
    date_dim["date"] = pd.to_datetime(date_dim["date"])
    date_dim["year"] = date_dim["date"].dt.year
    date_dim["month"] = date_dim["date"].dt.month
    date_dim["dayofweek"] = date_dim["date"].dt.day_name()
    # date_dim.columns = [x.lower() for x in list(date_dim.columns)]
    return date_dim 

def download_data(first_date, last_date, companies, ticker_map):

    # download data with yfinance
    data = yf.download(companies, start=first_date, end=last_date, auto_adjust=False)
    cols = ['date', 'ticker_id', 'open', 'high', 'low', 'close', 'volume']
    # load data into postgresql tables

    date_df = extract_date_dim(data)
    date_df.to_csv('/tmp/date.csv', index = None, header = False )

    # incorporate ticker_id column
    price_df = []
    for company in companies:
        df = download_table(data, company)
        
        # debugging
        # if df is None or df.empty:
        #     print(f"No data returned for {company}")
        #     continue

        # if not ticker_map:
        #     raise ValueError("ticker_map is None — check XCom push/pull")
    
        df['ticker_id'] = ticker_map[company]

        df.to_csv(f'/tmp/price_{company}.csv', index = None, header = False )
        price_df.append(df[cols])

    price_df = pd.concat(price_df, ignore_index=True)
    price_df.to_csv('/tmp/price.csv', index = None, header = False)

    return("Completed loading data into postgresql")

def download_stocks(companies, **kwargs):
    last_date = datetime.today() - timedelta(days=7)
    first_date = datetime.today()  - timedelta(days=100)
    last_date = last_date.strftime('%Y-%m-%d')
    first_date = first_date.strftime('%Y-%m-%d')
    
    ti = kwargs['ti'] # task instance
    ticker_map = ti.xcom_pull(key='ticker_map', task_ids='retrieve_ticker_id_tid')

    download_data(first_date, last_date, companies, ticker_map)


def store_tables():

    conn = PostgresHook(postgres_conn_id = 'postgres').get_conn()
    cur = conn.cursor()

    SQL_STATEMENT = f"COPY dim_date FROM stdin WITH DELIMITER as ','"
    with open('/tmp/date.csv', 'r') as f:
        cur.copy_expert(SQL_STATEMENT, f)
        conn.commit()

    SQL_STATEMENT = f"COPY fact_stock_price FROM stdin WITH DELIMITER as ','"
    with open(f'/tmp/price.csv', 'r') as f:
        cur.copy_expert(SQL_STATEMENT, f)
        conn.commit()
    
    conn.close()
    
    
