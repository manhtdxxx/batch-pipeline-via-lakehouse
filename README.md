# 📑 Table of Contents

- [📌 1. Introduction](#-1-introduction)
- [🏗 2. Architecture](#-2-architecture)
- [📂 3. Project Structure](#-3-project-structure)
- [🚀 4. Setup](#-4-setup)

---

# 📌 1. Introduction

This project demonstrates a **Lakehouse architecture** built on **Iceberg tables**. Data is progressively refined through a **medallion architecture (Bronze → Silver → Gold)** for analytics and ML model.

**Data Source:** Data fetched from **_VCI_** through **_VNStock - Python Library_**

**Key features of this project include:**

- **_Unified Data Storage_**: Combine the benefits of data lakes and data warehouses using a Lakehouse approach.
- **_Batch ELT Pipeline_**: Capture and process data using Apache Spark, orchestrated by Apache Airflow.
- **_Query & Analytics_**: Enable SQL querying on the Lakehouse using Trino and visualize insights with Power BI.
- **_Containerized Architecture_**: All services (Lakehouse stack, Spark, Airflow) are orchestrated via Docker for easy setup and reproducibility.

---

# 🏗 2. Architecture

## 2.1 Lakehouse

![Lakehouse Architecture](readme/lakehouse.png)

## 2.2 Pipeline

![Pipeline](readme/pipeline.png)

---

# 📂 3. Project Structure

```text
batch-pipeline-via-lakehouse/
│
├── docker/                            # Docker setup
│   ├── airflow/                         # Airflow Dockerfile
│   ├── hive/                            # Hive metastore configuration + Dockerfile
│   ├── init/                            # Initialization scripts in containers
│   ├── ml-app/                          # Dockerfile to run MLFlow Server, FastAPI, Streamlit UI
│   ├── spark/                           # Spark configuration + Dockerfile
│   ├── trino/                           # Trino configuration
│
├── data/                              # Raw datasets
├── notebooks/                         # Notebooks for testing pipeline, data exploration, and training model
├── src/                               # Main scripts
│   ├── dags/                            # Airflow DAG scripts
│   ├── elt/                             # ELT scripts
│       ├── bronze/                        # Bronze layer scripts – load raw data
│       ├── silver/                        # Silver layer scripts – clean/enrich data
│       ├── gold/                          # Gold layer scripts – aggregated / analytics-ready
│   ├── model/                           # Scripts for preprocessing, training, evaluation, promoting model to MLFlow
│   ├── serving/                         # Scripts for deploying model (using FastAPI & Streamlit)
│
├── readme/                            # Documentation, diagrams, notes
│
├── docker-compose-lakehouse.yml       # Docker Compose for the Lakehouse stack (MinIO, Hive Metastore, Trino)
├── docker-compose-spark.yml           # Docker Compose for Spark cluster (Spark Master, Spark Worker(s))
├── docker-compose.yml                 # Docker Compose for Airflow, MLFlow, Model, API, UI
```

---

# 🚀 4. Setup

## 4.1 Prerequisites

Before starting, please ensure you have:

- VS Code
- Git
- Docker Desktop
- DBeaver
- Power BI (downloaded from Website, not from Microsoft Store)

## 4.2 Setup & Initialization

**Step 1 (Optional):** Before running the pipeline, make sure `make` is installed. On Windows, you install Chocolatey first and then install Make:

```powershell
# Install Chocolatey (MUST run in PowerShell as Administrator)
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
# Upgrade Chocolatey (optional but recommended)
choco upgrade chocolatey
# Install Make
choco install make
# Verify installation
make --version
```

**Step 2:** Clone this repository to your local machine:

```bash
git clone https://github.com/manhtdxxx/batch-pipeline-via-lakehouse.git
```

**Step 3:** Install Hadoop & Hive to build Hive Metastore Image:

```bash
# Open project in VSCode & navigate to hive/jars folder
cd docker/hive/jars
# Run bash script to download hadoop and hive locally to build image (MUST run in Git Bash)
bash download_hadoop_and_hive.sh
```

**Step 4:** Set up the whole architecture through Docker:

```bash
# Create a Docker network "common-net" for all services to communicate with each other
docker network create common-net
# Start all services
make all-up
```

## 4.3 Service Access

### Web UI

- **MinIO UI:** http://localhost:9001
  - User: minio
  - Password: minio123
- **Trino UI:** http://localhost:8080
  - User: trino
  - Password:
- **Airflow UI:** http://localhost:8081
  - User: airflow
  - Password: airflow
- **Spark Master UI:** http://localhost:8082
- **Spark Worker 1 UI:** http://localhost:8083
- **MLFlow UI:** http://localhost:5000
- **Streamlit UI:** http://localhost:8501

### Database / SQL Client

- **Trino:** localhost:8080 (connect via DBeaver)
  - User: trino
  - Password:
- **Postgres (Hive Metastore):** localhost:5432 (connect via DBeaver)
  - User: hive
  - Password: hive
  - Database: metastore
- **Postgres (Airflow):** localhost:5433 (connect via DBeaver)
  - User: airflow
  - Password: airflow
  - Database: airflow
- **Postgres (MLFlow):** localhost:5434 (connect via DBeaver)
  - User: mlflow
  - Password: mlflow
  - Database: mlflow

## 4.4 Run the pipeline

### Step 1: Fetch data into CSV files

```bash
python data/_get_all_data.py
```

### Step 2: Initializing Schema in Lakehouse using Trino

Once Trino container is running, you can initialize the Lakehouse schema using the SQL initialization script:

```bash
make trino-init
```

### Step 3: Running pipeline through Airflow

**3.1** Before running the batch pipeline in Airflow, you need to **set up SSH connection** between the Airflow container and the Spark container:

```bash
make airflow-ssh-spark
```

**3.2** After setting up the SSH connection, access the Airflow UI to trigger the DAG:
![DAG](readme/dag.png)

💡 Once the DAG finish, you can open DBeaver and connect to Trino to query the Lakehouse and verify the result.

### Step 4: Dashboard using PBI

Power BI does not include Trino support by default; a custom connector must be set up.

- Official Docs: https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-connector-extensibility
- Custom Connector: https://github.com/CreativeDataEU/PowerBITrinoConnector

> **Here is the preview:**

![Market Cap](dashboard/market_cap.png)

![Valuation](dashboard/valuation.png)

![Risk & Return](dashboard/risk_return.png)

**⚠️ Dashboard still ongoing**

---

© 2025 manhtdxxx — All rights reserved.
