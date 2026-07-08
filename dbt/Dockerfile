FROM python:3.11-slim

WORKDIR /dbt

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

CMD ["sleep", "infinity"]