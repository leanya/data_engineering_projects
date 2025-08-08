from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import time
import yfinance as yf


def setup_ticker_dim(companies, engine):

    coy_list = companies.split(" ")
    coy_df = pd.DataFrame(coy_list, columns=['ticker_symbol'])
    coy_df.to_sql('dim_ticker', engine, if_exists= 'append', index=False)
    
    return coy_df

def retrieve_ticker_id(engine):
    
    query = text("SELECT * FROM dim_ticker;")
    with engine.connect() as conn:  # explicitly open connection
        ticker_df = pd.read_sql(query, conn)

    return ticker_df


def download_table(data, company):
    
    # extract individual company info from data 
    # remove multi-level index and tidy up column names 
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = data.loc[:, (cols, company)].copy()
    df.columns = df.columns.droplevel(1)
    df = df.reset_index()
    df.columns = [x.replace(" ", "").lower() for x in list(df.columns)]
    print(df.columns)
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

def download_data(first_date, last_date, companies, engine, ticker_df):

    # download data with yfinance
    data = yf.download(companies, start=first_date, end=last_date, auto_adjust=False)
    ticker_map = dict(zip(ticker_df['ticker_symbol'], ticker_df['ticker_id']))
    cols = ['date', 'ticker_id', 'open', 'high', 'low', 'close', 'volume']
    # load data into postgresql tables

    date_df = extract_date_dim(data)
    date_df.to_sql('dim_date', engine, if_exists='append', index=False)

    # incorporate ticker_id column
    for company in companies.split(' '):
        df = download_table(data, company)
        df['ticker_id'] = ticker_map[company]
        df = df[cols]
        df.to_sql('fact_stock_price', engine, if_exists= 'append', index=False)

    return("Completed loading data into postgresql")

def main():
    
    engine = create_engine('postgresql://student:student@localhost:5432/stockdb')

    companies = "SPY AAPL NVDA"
    setup_ticker_dim(companies, engine)
    time.sleep(10)

    # download approx. 1 year worth of dataset
    last_date = datetime.today() - timedelta(days=7)
    first_date = datetime.today()  - timedelta(days=366)
    last_date = last_date.strftime('%Y-%m-%d')
    first_date = first_date.strftime('%Y-%m-%d')

    ticker_df = retrieve_ticker_id(engine)
    download_data(first_date, last_date, companies, engine, ticker_df)
    engine.dispose()

    print("Completed downloading data and loading the tables!")

if __name__ == "__main__":
    main()