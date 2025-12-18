#!/bin/bash
set -e

echo ">>> Waiting for PostgreSQL..."
until pg_isready -h "mlflow-db" -p "5432" -U "mlflow"; do
    echo "Postgres not ready yet..."
    sleep 1
done

echo ">>> Starting MLflow server..."
exec mlflow server \
    --backend-store-uri "$MLFLOW_BACKEND_STORE_URI" \
    --artifacts-destination "$MLFLOW_ARTIFACTS_DESTINATION" \
    --host 0.0.0.0 \
    --port 5000 \
    --allowed-hosts '*' \
    --cors-allowed-origins '*'