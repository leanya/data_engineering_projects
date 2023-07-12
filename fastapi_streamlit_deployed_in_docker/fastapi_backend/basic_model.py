from fastapi import FastAPI
import uvicorn
from model import train_model
from pydantic import BaseModel

# creating FastAPI instance
app = FastAPI()

clf_model = train_model()

class request_body(BaseModel):
    sepal_length: float = 0
    sepal_width: float = 0
    petal_length: float = 0
    petal_width: float = 0

@app.get("/")
def root():
    return {"message": "Hello!"}

# creating an endpoint to receive the data to make prediciton on 
@app.post("/predict")
def predict(data: request_body):
    test_data = [[
        data.sepal_length,
        data.sepal_width,
        data.petal_length,
        data.petal_width
    ]]
    print(test_data)
    # predicting the class 
    class_idx = clf_model.predict(test_data)[0]
    print(class_idx)

    # return the result
    return {
        "class " + str(class_idx)
        }

# To run this command
# uvicorn basic_model:app --reload
# http://127.0.0.1:8000/docs
# {"sepal_length":6.1, "sepal_width":2.8, "petal_length":4.7, "petal_width":1.2}
# ouput: 1