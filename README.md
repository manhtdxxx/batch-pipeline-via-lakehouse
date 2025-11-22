# 📑 Table of Contents

- [📌 1. Introduction](#-1-introduction)
- [🏗 2. Architecture](#-2-architecture)
- [📂 3. Project Structure](#-3-project-structure)
- [🚀 4. Setup](#-4-setup)

---

# 📌 1. Introduction

This project demonstrates a **Lakehouse architecture** built on **Iceberg tables**. Data is progressively refined through a **medallion architecture (Bronze → Silver → Gold)** for analytics and BI use cases.

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
│   ├── init/                            # Initialization scripts in containers
│   ├── hive/                            # Hive metastore configuration + Dockerfile
│   ├── trino/                           # Trino configuration
│   ├── spark/                           # Spark configuration + Dockerfile
│   ├── airflow/                         # Airflow Dockerfile
│   ├── superset/                        # Superset configuration + Dockerfile
│
├── data/                              # Raw datasets
├── experiments/                       # Notebooks and scripts for testing pipelines and data exploration
├── src/                               # Main scripts
│   ├── dags/                            # Airflow DAG scripts
│   ├── elt/                             # ELT scripts
│       ├── bronze/                        # Bronze layer scripts – load raw data
│       ├── silver/                        # Silver layer scripts – clean/enrich data
│       ├── gold/                          # Gold layer scripts – aggregated / analytics-ready
│
├── readme/                            # Documentation, diagrams, notes
│
├── docker-compose-lakehouse.yml       # Docker Compose for the Lakehouse stack (MinIO, Hive Metastore, Postgres, Trino)
├── docker-compose-spark.yml           # Docker Compose for Spark cluster
├── docker-compose.yml                 # Docker Compose for Airflow, Superset, ...
```

---

# 🚀 4. Setup

## 4.1 Prerequisites

Before starting, please ensure you have:

- Docker Desktop installed and running.
- VS Code installed to open project.
- DBeaver installed to connect to Trino to query data from Lakehouse.

## 4.2 Setup & Initialization

**Step 1:** Before running the pipeline, make sure `make` is installed. On Windows, you install Chocolatey first and then install Make:

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

**Step 2:** Install Hadoop & Hive to set up Hive Metastore Image:

```bash
# Navigate to hive/jars folder
cd docker/hive/jars
# Run Makefile to install (MUST run in Git Bash)
make download
```

**Step 3:** Set up the whole architecture through Docker:

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

### Database / SQL Client

- **Postgres:** localhost:5432 (connect via DBeaver)
  - User: hive
  - Password: hive
  - Database: metastore
- **Trino:** localhost:8080 (connect via DBeaver)
  - User: trino
  - Password:

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
make airflow-ssh
```

**3.2** After setting up the SSH connection, access the Airflow UI to trigger the DAG:
![DAG](readme/dag.png)

💡 Once the DAG finish, you can open DBeaver and connect to Trino to query the Lakehouse and verify the result.

### Step 4: Dashboard using PBI

Power BI does not include Trino support by default; a custom connector must be set up.

- Official Docs: https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-connector-extensibility
- Custom Connector: https://github.com/CreativeDataEU/PowerBITrinoConnector

**⚠️ Ongoing**
