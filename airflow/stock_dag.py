from datetime import datetime 
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from python.download import download_stocks

def store_tables():
    conn = PostgresHook(postgres_conn_id = 'postgres').get_conn()
    cur = conn.cursor()

    SQL_STATEMENT = f"COPY date_dim FROM stdin WITH DELIMITER as ','"
    with open('/tmp/date.csv', 'r') as f:
        cur.copy_expert(SQL_STATEMENT, f)
        conn.commit()

    companies = companies = "AAPL SPY"
    for company in companies.split(' '):
        SQL_STATEMENT = f"COPY {company.lower()} FROM stdin WITH DELIMITER as ','"
        with open(f'/tmp/{company}.csv', 'r') as f:
            cur.copy_expert(SQL_STATEMENT, f)
            conn.commit()
    conn.close()
    

with DAG ("stock", start_date = datetime(2023, 1, 1), schedule_interval = "@daily", catchup = False ) as dag: 

    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres",
        sql="sql/create_stock_date.sql"
    )

    download_data = PythonOperator(
        task_id = 'download_data',
        python_callable = download_stocks
    )

    load_tables = PythonOperator(
        task_id = 'load_tables',
        python_callable = store_tables
    )

    create_table >> download_data >> load_tables 
