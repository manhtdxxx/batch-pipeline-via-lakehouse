# gold/_gold_handler.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sha2, concat, to_date, when, lit, to_unix_timestamp
from typing import List
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


    def read_incremental_from_silver(self, silver_table: str, gold_table: str, timestamp_col: str = "ingest_timestamp", date_col: str = None) -> DataFrame:
        is_empty = self.spark.sql(f"SELECT 1 FROM {gold_table} LIMIT 1").count() == 0

        if is_empty:
            self.logger.info(f"{gold_table} is empty. Reading all data from {silver_table} ...")
            query = f"SELECT * FROM {silver_table}"
        else:
            if date_col is not None:
                col_to_check = date_col
                sql_type = "DATE"
            else:
                col_to_check = timestamp_col
                sql_type = "TIMESTAMP"

            max_val = self.spark.sql(
                f"SELECT MAX({col_to_check}) AS max_val FROM {gold_table}"
            ).collect()[0][0]

            self.logger.info( f"Reading incremental data from {silver_table} where {col_to_check} > {max_val} ...")
            query = f"SELECT * FROM {silver_table} WHERE {col_to_check} > {sql_type} '{max_val}'"

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


    def add_surrogate_key(self, df: DataFrame, key_cols: List[str], sk_col_name: str, use_hash: bool = False) -> DataFrame:
        self.logger.info(f"Adding SK {sk_col_name} key based on key columns: {key_cols}")

        processed_cols = []
        for c in key_cols:
            if "date" in c.lower() or "timestamp" in c.lower():
                processed_cols.append(to_unix_timestamp(col(c)))
            else:
                processed_cols.append(col(c).cast("string"))

        if use_hash:
            hashed_cols = [sha2(c.cast("string"), 256) for c in processed_cols]
            df = df.withColumn(sk_col_name, concat(*hashed_cols))
        else:
            raw_cols = [c.cast("string") for c in processed_cols]
            df = df.withColumn(sk_col_name, concat(*raw_cols))
        return df


    def add_date_col(self, df: DataFrame) -> DataFrame:
        self.logger.info("Adding date column from year & quarter...")
        df = df.withColumn(
            "date", 
            to_date(
                concat(
                    col("year"),
                    lit("-"),
                    when(col("quarter") == 1, lit("03-31"))
                    .when(col("quarter") == 2, lit("06-30"))
                    .when(col("quarter") == 3, lit("09-30"))
                    .when(col("quarter") == 4, lit("12-31"))
                ),
                "yyyy-MM-dd"
            )
        )
        df = df.drop("year", "quarter")
        return df
