from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import yfinance as yf

def extract_table(data, company):
    
    # extract individual company info from data 
    # remove multi-level index and tidy up column names 
    cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
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

def download_data(first_date, last_date, companies, engine):

    # download data with yfinance
    data = yf.download(companies, start=first_date, end=last_date)

    # load data into postgresql tables 
    for company in companies.split(' '):
        df = extract_table(data, company)
        df.to_sql(company.lower(), engine, if_exists= 'replace')

    date_df = extract_date_dim(data)
    date_df.to_sql('date_dim', engine, if_exists= 'replace')

def main():
    # download approx. 1 year worth of dataset
    last_date = datetime.today() - timedelta(days=7)
    first_date = datetime.today()  - timedelta(days=366)
    last_date = last_date.strftime('%Y-%m-%d')
    first_date = first_date.strftime('%Y-%m-%d')

    companies = "AAPL SPY"
    engine = create_engine('postgresql://student:student@localhost:5432/stockdb')
    download_data(first_date, last_date, companies, engine)
    engine.dispose()

    print("Completed downloading data and loading the tables!")

if __name__ == "__main__":
    main()