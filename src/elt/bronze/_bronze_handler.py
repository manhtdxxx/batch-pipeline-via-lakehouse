# bronze/_bronze_handler.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, year, month
from typing import List
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class BronzeHandler:
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
    

    def read_file(self, path, format_type, **options) -> DataFrame:
        self.logger.info(f"Reading file from path: {path} with format: {format_type} ...")
        if format_type == "csv":
            return self.spark.read.csv(path, **options)
        elif format_type == "json":
            return self.spark.read.json(path, **options)
        else:
            self.logger.error(f"Unsupported format type: {format_type}")
        

    def add_metadata_cols(self, df: DataFrame, ingest_year: bool = True, ingest_month: bool = True) -> DataFrame:
        self.logger.info("Adding metadata columns...")
        df = df.withColumn("ingest_timestamp", current_timestamp())
        if ingest_year:
            df = df.withColumn("ingest_year", year("ingest_timestamp"))
        if ingest_month:
            df = df.withColumn("ingest_month", month("ingest_timestamp"))
        return df
    

    def load_to_bronze(self, df: DataFrame, table_name: str, partition_cols: List[str] = ["ingest_year", "ingest_month"]) -> None:
        self.logger.info(f"Writing DataFrame to {table_name} ...")
        (df.write
         .format("iceberg")
         .mode("append")
         .partitionBy(*partition_cols)
         .saveAsTable(table_name))
        self.logger.info(f"Done loading to {table_name}!")


    def stop(self) -> None:
        self.logger.info("Stopping SparkSession ...")
        self.spark.stop()
        self.logger.info("SparkSession stopped.")

    
    def read_kafka(self, kafka_server: str, topic: str):
        self.logger.info(f"Starting to read stream from Kafka topic '{topic}' on server '{kafka_server}' ...")
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_server)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load())
    

    def stream_to_bronze(self, df: DataFrame, table_name: str, checkpoint_location: str, partition_cols: List[str] = ["ingest_year", "ingest_month"], processing_time: str = '5 seconds') -> None:
        self.logger.info(f"Starting to stream DataFrame to {table_name} ...")
        query = (
            df.writeStream
            .format("iceberg")
            .outputMode("append")
            .trigger(processingTime=processing_time)
            .option("checkpointLocation", checkpoint_location)
            .partitionBy(*partition_cols)
            .table(table_name)
        )

        try:
            self.logger.info("Stream is running, awaiting termination...")
            query.awaitTermination()
        except KeyboardInterrupt:
            self.logger.warning("Stream terminated by user (KeyboardInterrupt).")
        except Exception as e:
            self.logger.error(f"Error occurred while streaming to {table_name}: {e}")
        finally:
            self.logger.info("Stream has stopped.")