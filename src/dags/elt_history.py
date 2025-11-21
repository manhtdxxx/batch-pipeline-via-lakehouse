from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

SSH_CONN_ID = "ssh_spark"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SCRIPT_DIR = "/opt/spark/app"


with DAG(
    dag_id="elt_history",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    def create_spark_task(task_id: str, script_file: str):
        cmd = f"""
        export JAVA_HOME=/opt/java/openjdk && \
        export PATH=$JAVA_HOME/bin:$PATH && \
        {SPARK_SUBMIT} {os.path.join(SCRIPT_DIR, script_file)}
        """
        task = SSHOperator(
            task_id=task_id,
            ssh_conn_id=SSH_CONN_ID,
            command=cmd
        )
        return task

    load_dim = create_spark_task("load_dim_to_bronze", "bronze/load_dim.py")
    load_fact = create_spark_task("load_fact_to_bronze", "bronze/load_fact.py")

    transform_industry = create_spark_task("transform_industry", "silver/transform_industry.py")
    transform_company = create_spark_task("transform_company", "silver/transform_company.py")
    transform_shareholders = create_spark_task("transform_shareholders", "silver/transform_company_shareholders.py")
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
    for t in [load_dim, load_fact]:
        t >> transform_industry
        t >> transform_company
        t >> transform_shareholders
        t >> transform_events
        t >> transform_quarterly_ratio
        t >> transform_daily_ohlcv

    [transform_industry, transform_company] >> dim_company

    transform_shareholders >> dim_shareholder

    transform_quarterly_ratio >> fact_quarterly_ratio

    [transform_quarterly_ratio, transform_events] >> fact_share_issue
    transform_events >> fact_cash_dividend

    transform_daily_ohlcv >> dim_date
    transform_daily_ohlcv >> fact_daily_ohlcv