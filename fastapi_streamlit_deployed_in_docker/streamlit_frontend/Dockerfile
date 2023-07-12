FROM python:3.7

WORKDIR /app

RUN pip install --no-cache-dir streamlit requests

COPY . /app

EXPOSE 8501

ENTRYPOINT ["streamlit","run"]
CMD ["stream_lit.py"]