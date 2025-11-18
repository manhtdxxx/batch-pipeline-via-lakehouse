# silver/_silver_pipeline.py

from typing import Dict, List, Tuple
from _silver_handler import SilverHandler
import logging
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class SilverPipeline:
    def __init__(self,
                 app_name: str, bronze_table: str, silver_table: str,
                 schema: StructType, 
                 key_cols: List[str],
                 rename_map: Dict[str, str] = None,
                 trimmed_cols: List[str] = None,
                 positive_cols: List[str] = None,
                 zero_and_positive_cols: List[str] = None,
                 fill_map: Dict[str, any] = None,
                 dropna_cols: List[str] = None,
                 scd_tracked_cols: List[str] = None,
                 deduplicate_condition_cols: List[Tuple[str, str]] = None):
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.handler = SilverHandler(app_name=app_name)

        self.bronze_table = bronze_table
        self.silver_table = silver_table

        self.schema = schema
        self.key_cols = key_cols
        self.rename_map = rename_map or {}

        self.trimmed_cols = trimmed_cols or []
        self.positive_cols = positive_cols or []
        self.zero_and_positive_cols = zero_and_positive_cols or []
        self.fill_map = fill_map or {}
        self.dropna_cols = dropna_cols or []

        self.deduplicate_condition_cols = deduplicate_condition_cols or [("updated_at", "desc"), ("ingest_timestamp", "desc")]
        self.scd_tracked_cols = scd_tracked_cols or []


    def transform(self, df) -> 'DataFrame | None':
        df = self.handler.normalize_schema(df, self.schema)

        if self.rename_map:
            df = self.handler.rename_cols(df, self.rename_map)
        
        if self.trimmed_cols:
            df = self.handler.trim_cols(df, self.trimmed_cols)

        if self.positive_cols:
            df = self.handler.nullify_zero_and_negative_cols(df, self.positive_cols)

        if self.zero_and_positive_cols:
            df = self.handler.nullify_negative_cols(df, self.zero_and_positive_cols)
        
        self.logger.info(f"Before handle_null: {df.count()} rows")

        if self.dropna_cols or self.fill_map:
            df = self.handler.handle_null(df, dropna_cols=self.dropna_cols, fill_map=self.fill_map)
            if df.rdd.isEmpty():
                self.logger.warning("No data left after null handling. Exiting.")
                return None

        df = self.handler.deduplicate(df, key_cols=self.key_cols, condition_cols=self.deduplicate_condition_cols)
        if df.rdd.isEmpty():
            self.logger.warning("No data left after deduplication. Exiting.")
            return None

        return df


    def run(self, scd_type: str = None, overwrite: bool = False):
        self.logger.info(f"Starting transformation for {self.bronze_table} to {self.silver_table}...")

        # Read
        bronze_df = self.handler.read_incremental_from_bronze(self.bronze_table, self.silver_table)
        if bronze_df.rdd.isEmpty():
            self.logger.warning(f"No new data in {self.bronze_table}. Exiting.")
            self.handler.stop()
            return

        # Transform
        silver_df = self.transform(bronze_df)
        if silver_df is None:
            self.handler.stop()
            return
        
        # Load
        if overwrite:
            self.logger.info(f"Overwriting entire {self.silver_table} ...")
            silver_df.write.format("iceberg").mode("overwrite").saveAsTable(self.silver_table)
        else:
            if scd_type == "one":
                merge_fn = self.handler.merge_scd1 
            elif scd_type == "two":
                merge_fn = self.handler.merge_scd2
            else:
                self.logger.error('scd_type not in [one, two]')
                return
            
            merge_fn(silver_df, self.silver_table, self.key_cols, self.scd_tracked_cols)
            self.handler.stop()

        self.logger.info(f"Done all!")