FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . /app
RUN pip install --upgrade pip
RUN pip install --no-cache-dir confluent-kafka[avro] pandas cassandra-driver fastavro attrs httpx cachetools authlib certifi

ENV SSL_CERT_FILE=/usr/local/lib/python3.12/site-packages/certifi/cacert.pem
EXPOSE 80
CMD ["python3", "consumer.py"]