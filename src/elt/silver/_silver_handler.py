# silver/_silver_handler.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, when, trim, row_number, desc, sha2, concat, asc, create_map, format_string
from pyspark.sql.types import StructType, IntegerType, TimestampType
from pyspark.sql.window import Window
from typing import List, Dict, Tuple
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class SilverHandler:
    def __init__(self, app_name: str, max_cores: int = 1, cores_per_executor: int = 1, memory_per_executor: str = "512m"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = self._create_spark_session(app_name, max_cores, cores_per_executor, memory_per_executor)


    def _create_spark_session(self, app_name: str, max_cores: int, cores_per_executor: int, memory_per_executor: str) -> SparkSession:
        self.logger.info(f"Creating SparkSession: {app_name} ...")
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.cores.max", max_cores)
                .config("spark.executor.cores", cores_per_executor)
                .config("spark.executor.memory", memory_per_executor)
                .getOrCreate())


    def read_incremental_from_bronze(self, bronze_table: str, silver_table: str, timestamp_col: str = "ingest_timestamp") -> DataFrame:
        is_empty = self.spark.sql(f"SELECT 1 FROM {silver_table} LIMIT 1").count() == 0

        if is_empty:
            self.logger.info(f"{silver_table} is empty. Reading all data from {bronze_table} ...")
            query = f"SELECT * FROM {bronze_table}"
        else:
            max_ingest_timestamp = self.spark.sql(f"SELECT MAX(ingest_timestamp) AS max_timestamp FROM {silver_table}").collect()[0][0]
            self.logger.info(f"Reading incremental data from {bronze_table} where {timestamp_col} > {max_ingest_timestamp} ...")
            query = f"SELECT * FROM {bronze_table} WHERE {timestamp_col} > TIMESTAMP '{max_ingest_timestamp}'"
        return self.spark.sql(query)

    
    def normalize_schema(self, df: DataFrame, schema: StructType) -> DataFrame:
        self.logger.info(f"Normalizing DataFrame schema to match Silver schema ...")

        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
            else:
                df = df.withColumn(field.name, lit(None).cast(field.dataType))  # add missing column with nulls

        # reorder columns
        silver_cols = [f.name for f in schema.fields]
        return df.select(silver_cols)


    def rename_cols(self, df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
        self.logger.info(f"Renaming columns as per mapping: {rename_map} ...")
        for old_name, new_name in rename_map.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        return df


    def trim_cols(self, df: DataFrame, cols_to_apply: List[str]) -> DataFrame:
        self.logger.info(f"Trimming columns: {cols_to_apply} ...")
        for c in cols_to_apply:
            df = df.withColumn(c, trim(col(c)))
        return df


    def nullify_zero_and_negative_cols(self, df: DataFrame, cols_to_apply: List[str]) -> DataFrame:
        self.logger.info(f"Nullifying zero and negative values for columns: {cols_to_apply} ...")
        for c in cols_to_apply:
            df = df.withColumn(c, when(col(c) <= 0, None).otherwise(col(c)))
        return df
    

    def nullify_negative_cols(self, df: DataFrame, cols_to_apply: List[str]) -> DataFrame:
        self.logger.info(f"Nullifying only negative values for columns: {cols_to_apply} ...")
        for c in cols_to_apply:
            df = df.withColumn(c, when(col(c) < 0, None).otherwise(col(c)))
        return df


    def handle_null(self, df: DataFrame, dropna_cols: List[str] = None, fill_map: Dict[str, any] = None) -> DataFrame:    
        if dropna_cols:
            self.logger.info(f"Dropping rows with nulls in columns: {dropna_cols} ...")
            df = df.dropna(subset=dropna_cols)
        
        if fill_map:
            for col_name, fill_cfg in fill_map.items():

                # Case 1: Fixed value
                if isinstance(fill_cfg, (int, float, str)):
                    self.logger.info(f"Filling column {col_name} with fixed value {fill_cfg}")
                    df = df.na.fill({col_name: fill_cfg})

                elif isinstance(fill_cfg, dict):
                    # Case 2: Dictionary Mapping
                    if "map" in fill_cfg and isinstance(fill_cfg["map"], dict):
                        col_to_lookup = fill_cfg["col_to_lookup"]
                        map_dict = fill_cfg["map"]
                        self.logger.info(f"Filling '{col_name}' from reference '{col_to_lookup}' with values mapped '{map_dict}'")
                        mapType_col = create_map(*[lit(i) for items in map_dict.items() for i in items])
                        df = df.withColumn(col_name, 
                                           when(col(col_name).isNull(), 
                                                mapType_col.getItem(col(col_to_lookup))
                                                ).otherwise(col(col_name)))
                    # Case 3: Template
                    elif "template" in fill_cfg:
                        col_to_lookup = fill_cfg["col_to_lookup"]
                        template = fill_cfg["template"]
                        self.logger.info(f"Filling '{col_name}' from reference '{col_to_lookup}' using template '{template}'")
                        df = df.withColumn(col_name,
                                           when(col(col_name).isNull(),
                                                format_string(template, col(col_to_lookup))
                                                ).otherwise(col(col_name)))

        return df


    def deduplicate(self, df: DataFrame, key_cols: List[str], condition_cols: List[Tuple[str, str]]) -> DataFrame:
        self.logger.info(f"Deduplicating DataFrame based on key columns: {key_cols} and condition column: {condition_cols} ...")

        order_exprs = []
        for col_name, direction in condition_cols:
            if col_name not in df.columns:
                self.logger.warning(f"Column '{col_name}' used as condition for deduplication not found, skipping.")
                continue
            if direction.lower() == "asc":
                order_exprs.append(asc(col_name))
            elif direction.lower() == "desc":
                order_exprs.append(desc(col_name))
            else:
                msg = "direction must be 'asc' or 'desc'"
                self.logger.error(msg)
                raise ValueError(msg)

        window = Window.partitionBy(*key_cols).orderBy(*order_exprs)
        df = df.withColumn("rank", row_number().over(window))
        return df.filter(col("rank") == 1).drop("rank")
    

    def _add_scd2_cols(self, df: DataFrame) -> DataFrame:
        self.logger.info("Adding SCD Type 2 columns: start_timestamp, end_timestamp, is_current ...")
        # it would be better to set start_timestamp = updated_at if available
        # but for simplicity, we use ingest_timestamp here
        start_col = "updated_at" if "updated_at" in df.columns else "ingest_timestamp"
        return (df.withColumn("start_timestamp", col(start_col).cast(TimestampType()))
                  .withColumn("end_timestamp", lit(None).cast(TimestampType()))
                  .withColumn("is_current", lit(1).cast(IntegerType())))


    def merge_scd2(self, df: DataFrame, silver_table: str, key_cols: List[str], tracked_cols: List[str]):
        df = self._add_scd2_cols(df)
        df.createOrReplaceTempView("source")

        key_condition = " AND ".join([f"source.{c} = target.{c}" for c in key_cols])
        update_condition = " OR ".join([f"NOT (source.{c} <=> target.{c})" for c in tracked_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"source.{c}" for c in df.columns])

        is_empty = self.spark.sql(f"SELECT 1 FROM {silver_table} LIMIT 1").count() == 0

        if is_empty:
            insert_stmt = f"""
                INSERT INTO {silver_table} ({insert_cols}) 
                SELECT {insert_vals} FROM source
            """
            self.logger.info(f"Inserting data into empty {silver_table} ...")
            self.spark.sql(insert_stmt)
        else:
            update_stmt = f"""
                MERGE INTO {silver_table} AS target
                USING source
                ON {key_condition} AND target.is_current = 1
                WHEN MATCHED AND ({update_condition}) THEN
                    UPDATE SET target.end_timestamp = source.start_timestamp, target.is_current = 0
            """
            insert_stmt = f"""
                MERGE INTO {silver_table} AS target
                USING source AS source
                ON {key_condition} AND target.is_current = 1
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            self.logger.info(f"Updating old records in {silver_table} ...")
            self.spark.sql(update_stmt)
            self.logger.info(f"Inserting new records into {silver_table} ...")
            self.spark.sql(insert_stmt)


    def merge_scd1(self, df: DataFrame, silver_table: str, key_cols: List[str], tracked_cols: List[str]):
        df.createOrReplaceTempView("source")

        key_condition = " AND ".join([f"source.{c} = target.{c}" for c in key_cols])
        update_condition = " OR ".join([f"NOT (source.{c} <=> target.{c})" for c in tracked_cols])
        update_clause = ", ".join([f"target.{c} = source.{c}" for c in df.columns])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"source.{c}" for c in df.columns])

        is_empty = self.spark.sql(f"SELECT 1 FROM {silver_table} LIMIT 1").count() == 0

        if is_empty:
            insert_stmt = f"""
                INSERT INTO {silver_table} ({insert_cols})
                SELECT {insert_vals} FROM source
            """
            self.logger.info(f"Inserting data into empty {silver_table} ...")
            self.spark.sql(insert_stmt)
        else:
            upsert_stmt = f"""
                MERGE INTO {silver_table} AS target
                USING source
                ON {key_condition}
                WHEN MATCHED AND {update_condition} THEN
                  UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                  INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            self.logger.info(f"Upserting data into {silver_table} ...")
            self.spark.sql(upsert_stmt)

    
    def stop(self):
        self.logger.info("Stopping SparkSession ...")
        self.spark.stop()
        self.logger.info("SparkSession stopped.")


    def read_stream_from_bronze(self, bronze_table: str) -> DataFrame:
        self.logger.info(f"Reading streaming data from {bronze_table} ...")
        return self.spark.readStream.format("iceberg").load(bronze_table)


    def write_stream_to_silver(self, df: DataFrame, silver_table: str, checkpoint_path: str, processing_time: str):
        try:
            self.logger.info(f"Starting stream to {silver_table} with processing time '{processing_time}' ...")
            query = (
                df.writeStream
                  .format("iceberg")
                  .outputMode("append")
                  .option("checkpointLocation", checkpoint_path)
                  .trigger(processingTime=processing_time)
                  .toTable(silver_table)
            )
            query.awaitTermination()
        except KeyboardInterrupt:
            self.logger.warning("Streaming interrupted by user.")
        except Exception as e:
            self.logger.error(f"Streaming failed for {silver_table}: {e}")
        finally:
            self.logger.info("Streaming query has ended.")