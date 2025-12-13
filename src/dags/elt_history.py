# dags/elt_history.py

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


def create_spark_task(task_id: str, script_file: str, arg_list: list[str] = None):
    args = " ".join(arg_list or [])
    cmd = f"""
    export JAVA_HOME=/opt/java/openjdk && \
    export PATH=$JAVA_HOME/bin:$PATH && \
    /opt/spark/bin/spark-submit /opt/spark/app/{script_file} {args}
    """
    task = SSHOperator(
        task_id=task_id,
        ssh_conn_id="ssh_spark",
        command=cmd
    )
    return task


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_history",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    max_active_runs=1
) as dag:
    
    load_industry = create_spark_task(
        task_id="load_industry",
        script_file="bronze/load_dim.py",
        arg_list=[
            "--app_name", "LoadIndustryToBronze",
            "--bronze_table", "iceberg.bronze.industry",
            "--file_path", "/opt/spark/data/industry.csv",
        ],
    )

    load_company = create_spark_task(
        task_id="load_company",
        script_file="bronze/load_dim.py",
        arg_list=[
            "--app_name", "LoadCompanyToBronze",
            "--bronze_table", "iceberg.bronze.company",
            "--file_path", "/opt/spark/data/company.csv",
        ],
    )

    load_shareholders = create_spark_task(
        task_id="load_company_shareholders",
        script_file="bronze/load_dim.py",
        arg_list=[
            "--app_name", "LoadCompanyShareholdersToBronze",
            "--bronze_table", "iceberg.bronze.company_shareholders",
            "--file_path", "/opt/spark/data/company_shareholders.csv",
        ],
    )

    load_events = create_spark_task(
        task_id="load_company_events",
        script_file="bronze/load_fact.py",
        arg_list=[
            "--app_name", "LoadCompanyEventsToBronze",
            "--bronze_table", "iceberg.bronze.company_events",
            "--fact_type", "company_events",
            "--file_path", "/opt/spark/data/company_events.csv",
            "--options", '\'{"start_date": "2022-01-01"}\'',
        ],
    )

    load_quarterly_ratio = create_spark_task(
        task_id="load_quarterly_ratio",
        script_file="bronze/load_fact.py",
        arg_list=[
            "--app_name", "LoadQuarterlyRatioToBronze",
            "--bronze_table", "iceberg.bronze.quarterly_ratio",
            "--fact_type", "quarterly_ratio",
            "--file_path", "/opt/spark/data/quarterly_ratio.csv",
            "--options", '\'{"start_year": "2022"}\'',
        ],
    )

    load_daily_ohlcv = create_spark_task(
        task_id="load_daily_ohlcv",
        script_file="bronze/load_fact.py",
        arg_list=[
            "--app_name", "LoadDailyOhlcvToBronze",
            "--bronze_table", "iceberg.bronze.daily_ohlcv",
            "--fact_type", "daily_ohlcv",
            "--file_path", "/opt/spark/data/daily_ohlcv.csv",
            "--options", '\'{"start_date": "2022-01-01"}\'',
        ],
    )

    transform_industry = create_spark_task("transform_industry", "silver/transform_industry.py")
    transform_company = create_spark_task("transform_company", "silver/transform_company.py")
    transform_shareholders = create_spark_task("transform_company_shareholders", "silver/transform_company_shareholders.py")
    transform_events = create_spark_task("transform_company_events", "silver/transform_company_events.py")
    transform_quarterly_ratio = create_spark_task("transform_quarterly_ratio", "silver/transform_quarterly_ratio.py")
    transform_daily_ohlcv = create_spark_task("transform_daily_ohlcv", "silver/transform_daily_ohlcv.py")

    dim_company = create_spark_task("dim_company", "gold/dim_company.py")
    dim_shareholder = create_spark_task("dim_shareholder", "gold/dim_shareholder.py")
    dim_date = create_spark_task("dim_date", "gold/dim_date.py")
    fact_cash_dividend = create_spark_task("fact_cash_dividend", "gold/fact_cash_dividend.py")
    fact_share_issue = create_spark_task("fact_share_issue", "gold/fact_share_issue.py")
    fact_quarterly_ratio = create_spark_task("fact_quarterly_ratio", "gold/fact_quarterly_ratio.py")
    fact_daily_ohlcv = create_spark_task("fact_daily_ohlcv", "gold/fact_daily_ohlcv.py")

    # -------------------------
    # DAG Dependencies
    # -------------------------
    load_industry >> transform_industry
    load_company >> transform_company
    load_shareholders >> transform_shareholders
    load_events >> transform_events
    load_quarterly_ratio >> transform_quarterly_ratio
    load_daily_ohlcv >> transform_daily_ohlcv

    [transform_industry, transform_company] >> dim_company

    transform_shareholders >> dim_shareholder

    transform_quarterly_ratio >> fact_quarterly_ratio

    [transform_quarterly_ratio, transform_events] >> fact_share_issue
    transform_events >> fact_cash_dividend

    transform_daily_ohlcv >> dim_date
    transform_daily_ohlcv >> fact_daily_ohlcv