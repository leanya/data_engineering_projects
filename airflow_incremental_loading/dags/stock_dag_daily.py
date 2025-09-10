from datetime import datetime, timedelta
from airflow import DAG 
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helper.download import setup_ticker, retrieve_ticker, download_stocks, store_tables

with DAG ("stock_incremental", start_date = datetime(2025, 1, 1), schedule=timedelta(days=1), catchup = False ) as dag: 

    create_temp_table = SQLExecuteQueryOperator(
        task_id = "create_temp_table_tid",
        conn_id = "postgres",
        sql="sql/incremental_create_temp.sql"
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
                   'days_back': 1,
                   'days_back_start': 0}
    )

    load_tables = PythonOperator(
        task_id = 'load_tables_tid',
        python_callable = store_tables,
        op_kwargs={'date_table': 'stage_date',
                   'stock_table': 'stage_stock'}
    )

    merge_tables_task = SQLExecuteQueryOperator(
        task_id = 'merge_tables_tid',
        conn_id = "postgres",
        sql="sql/incremental_merge.sql"
    )

    create_temp_table >> retrieve_ticker_id >> download_data >> load_tables >> merge_tables_task 

