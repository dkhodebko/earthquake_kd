FROM apache/airflow:2.10.4

COPY req.txt .

RUN pip install --no-cache-dir -r req.txt