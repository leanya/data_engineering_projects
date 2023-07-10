Overview of the project   

1. Create tables in postgresql (create_table.py)  
2. Download datasets using yfinance(etl.py)
- For illustration purpose, aapl and spy datasets are downloaded
3. Load the datasets into postgresql  (etl.py)
- Date dimension and fact table of the various stock data
4. Increment data loading (increment_data_loading.py)
- Create temp tables in postgresql
- Get the latest date from the date dimension table 
- Download dataset from latest date + 1 to today()
- Load data into the temp tables and merge with the corresponding tables in postgresql 
