# gold/_gold_handler.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, row_number, desc, asc
from pyspark.sql.window import Window
from typing import List, Dict, Tuple
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class GoldHandler:
    def __init__(self, app_name: str, max_cores: int = 1, cores_per_executor: int = 1, memory_per_executor: str = "512m"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = self._create_spark_session(app_name, max_cores, cores_per_executor, memory_per_executor)


    def _create_spark_session(self, app_name: str, max_cores: int, cores_per_executor: int, memory_per_executor: str) -> SparkSession:
        self.logger.info(f"Creating SparkSession for Gold: {app_name} ...")
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.cores.max", max_cores)
                .config("spark.executor.cores", cores_per_executor)
                .config("spark.executor.memory", memory_per_executor)
                .getOrCreate())

    def stop(self):
        self.logger.info("Stopping SparkSession for Gold ...")
        self.spark.stop()
        self.logger.info("SparkSession stopped.")


    def read_incremental_from_silver(self, silver_table: str, gold_table: str, timestamp_col: str = "ingest_timestamp") -> DataFrame:
        is_empty = self.spark.sql(f"SELECT 1 FROM {gold_table} LIMIT 1").count() == 0

        if is_empty:
            self.logger.info(f"{gold_table} is empty. Reading all data from {silver_table} ...")
            query = f"SELECT * FROM {silver_table}"
        else:
            max_ingest_timestamp = self.spark.sql(
                f"SELECT MAX({timestamp_col}) AS max_timestamp FROM {gold_table}"
            ).collect()[0][0]

            self.logger.info( f"Reading incremental data from {silver_table} where {timestamp_col} > {max_ingest_timestamp} ...")
            query = f"SELECT * FROM {silver_table} WHERE {timestamp_col} > TIMESTAMP '{max_ingest_timestamp}'"

        return self.spark.sql(query)


    def append_to_gold(self, df: DataFrame, gold_table: str):
        row_count = df.count()
        self.logger.info(f"Appending {row_count} rows to {gold_table} ...")
        (
            df.write
            .format("iceberg")
            .mode("append")
            .saveAsTable(gold_table)
        )
        self.logger.info(f"Done all!")