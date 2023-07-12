import streamlit as st 
import json
import requests

# st.title displays text in title formatting 
st.title("Basic Model App")

#taking in user inputs
st.write("")
st.write("Please fill up below info:) ")
sepal_length = st.number_input("Sepal Length")
sepal_width = st.number_input("Sepal Width")
petal_length = st.number_input("Petal Length")
petal_width = st.number_input("Petal Width")

inputs = {"sepal_length":sepal_length, "sepal_width":sepal_width, "petal_length":petal_length, "petal_width":petal_width}

# When the user clicks on button it will fetch the API
# st.button displays a button widget 
if st.button("Enter"):
    res = requests.post(url = "http://fastapi:8000/predict", data = json.dumps(inputs))

    st.subheader(f"Response from API = {res.text}")

# need both front-end an back-end servers up and running
# to avoid errors and running the app well 
# for running streamlit app 
# streamlit run stream_lit.py