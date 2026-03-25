FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    streamlit \
    pandas \
    plotly \
    boto3 \
    pyathena

EXPOSE 8501

CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
