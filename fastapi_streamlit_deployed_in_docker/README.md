Deploy machine learning model with Streamlit, FastAPI, Docker Compose    

1. Frontend user interface with Streamlit
- User sends a request via Streamlit. 
- Stramlit interacts with the API via HTTP request
2. Backend service with FastAPI
- FastAPI is used to implement the API 
- The API triggers the prediction model which returns the predicted output 
3. Run the application with docker-compose 
- As we have 2 docker images - Streamlit and FastAPI, we make use of docker compose 

To run docker compose,
```
docker-compose up -d --build 
``` 
Access streamit page using 'localhost:8501'

To stop the containers,
```
docker-compose down
```

  