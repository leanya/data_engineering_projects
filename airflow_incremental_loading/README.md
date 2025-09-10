#### Overview of the project   

This is an extension of the project postgres_etl by using Airflow to orchestrate the data pipelines 
The pipeline performs incremental loading of data and is deployed on the Oracle ARM compute instance.  

1. Create tables in PostgreSQL 
2. Download datasets using yfinance
- AAPL, SPY, NVDA tickers are downloaded
3. Load the datasets into PostgreSQL 
    - Date dimension 
    - Ticker dimension
    - Stock fact table
4. Airflow to execute the tasks in above order 

Airflow is installed with Docker, https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml

#### Running the pipeline

- Configure the following GitHub secrets to automate deployment:

| Name               | Description                                     |
|:------------------ |:------------------------------------------------|
| `DOCKER_USER`      | Docker Hub username                             |
| `DOCKER_PASSWORD`  | Docker Hub password or access token             |
| `INSTANCE_IP`      | Public IP of the VM instance                    |
| `SSH_KEY`          | Base64-encoded **private SSH key** for VM access |

- Alternatively, run the full pipeline with the following command for local development
```docker
docker compose up airflow-init
docker compose up -d
docker compose down --volumes --rmi all # To stop the services and delete containers/volume
```

#### Reference
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- https://github.com/apache/airflow/blob/6b89a3873c54ef7e116e04c7e074be1efb353251/task-sdk-tests/docker/docker-compose.yaml
- https://github.com/apache/airflow/discussions/51095
- https://github.com/orgs/community/discussions/148648