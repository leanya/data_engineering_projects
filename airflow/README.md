# Overview of the project   

1. Create tables in postgresql 
2. Download datasets using yfinance
- For illustration purpose, aapl and spy datasets are downloaded
3. Load the datasets into postgresql 
- Date dimension and fact table of the various stock data
4. Airflow to execute the tasks in above order 

Airflow is installed with Docker, https://airflow.apache.org/docs/apache-airflow/2.5.1/docker-compose.yaml

