FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        gcc g++ \
        libpq-dev \
        postgresql-client \
        libfreetype6-dev \
        libpng-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
        mlflow==3.6.0 \
        psycopg2-binary>=2.9 \
        boto3>=1.26
    
        
WORKDIR /mlflow
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x entrypoint.sh
ENTRYPOINT ["/mlflow/entrypoint.sh"]
