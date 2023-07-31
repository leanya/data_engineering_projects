from datetime import datetime, timedelta

from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import yfinance as yf

def extract_table(data, company):
    
    # extract individual company info from data 
    # remove multi-level index and tidy up column names 
    cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
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

def download_from_yfinance(first_date, last_date, companies):

    # download data with yfinance
    data = yf.download(companies, start=first_date, end=last_date)
    
    # save data into temp files  
    for company in companies.split(' '):
        df = extract_table(data, company)
        df.to_csv(f'/tmp/{company}.csv', index = None, header = False )

    date_df = extract_date_dim(data)
    date_df.to_csv('/tmp/date.csv', index = None, header = False )

def download_stocks():
    last_date = datetime.today() - timedelta(days=7)
    first_date = datetime.today()  - timedelta(days=100)
    last_date = last_date.strftime('%Y-%m-%d')
    first_date = first_date.strftime('%Y-%m-%d')

    companies = "AAPL SPY"
    download_from_yfinance(first_date, last_date, companies)
    
    
