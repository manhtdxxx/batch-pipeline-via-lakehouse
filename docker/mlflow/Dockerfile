# Stage 1: Builder
FROM python:3.10-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev libfreetype6-dev libpng-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# Stage 2: Runtime
FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        git postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

WORKDIR /mlflow

COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/mlflow/entrypoint.sh"]
