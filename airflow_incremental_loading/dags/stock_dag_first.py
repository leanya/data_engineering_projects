from datetime import datetime, timedelta
from airflow import DAG 
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helper.download import setup_ticker, retrieve_ticker, download_stocks, store_tables


with DAG ("stock_first", start_date = datetime(2025, 1, 1), schedule='@once', catchup = False ) as dag: 

    create_table = SQLExecuteQueryOperator(
        task_id = "create_table_tid",
        conn_id = "postgres",
        sql="sql/create_stock_date.sql"
    )

    setup_ticker_dim = PythonOperator(
        task_id = 'setup_ticker_dim_tid',
        python_callable = setup_ticker,
        op_kwargs={'companies': ['AAPL', 'SPY', 'NVDA']}
    )

    retrieve_ticker_id = PythonOperator(
        task_id = 'retrieve_ticker_id_tid',
        python_callable = retrieve_ticker,
        op_kwargs={}
    )

    download_data = PythonOperator(
        task_id = 'download_data_tid',
        python_callable = download_stocks,
        op_kwargs={'companies': ['AAPL', 'SPY', 'NVDA'],
                   'days_back': 100,
                   'days_back_start': 1}
    )

    load_tables = PythonOperator(
        task_id = 'load_tables_tid',
        python_callable = store_tables,
        op_kwargs={'date_table': 'dim_date',
                   'stock_table': 'fact_stock_price'}
    )

    create_table >> setup_ticker_dim >> retrieve_ticker_id >> download_data >> load_tables 


