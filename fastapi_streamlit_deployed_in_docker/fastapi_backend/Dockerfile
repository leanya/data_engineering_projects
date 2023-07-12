FROM python:3.7

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn scikit-learn

COPY . /app

EXPOSE 8000

CMD ["uvicorn", "basic_model:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]