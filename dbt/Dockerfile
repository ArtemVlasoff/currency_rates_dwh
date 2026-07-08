FROM python:3.11-slim

WORKDIR /dbt/cbr_marts

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

CMD ["sleep", "infinity"]