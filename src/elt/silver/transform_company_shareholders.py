# silver/transform_company_shareholders.py

from _silver_pipeline import SilverPipeline
import logging
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, TimestampType, DoubleType
from pyspark.sql import functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class CompanyShareholderTransformer(SilverPipeline):
    SCHEMA = StructType([
        StructField("id", StringType(), True),
        StructField("share_holder", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("share_own_percent", DoubleType(), True),
        StructField("update_date", DateType(), True),
        StructField("ingest_timestamp", TimestampType(), False)
    ])
        
    RENAME_MAP = {
        "share_holder": "shareholder_name",
        "share_own_percent": "ownership_percentage",
        "update_date": "updated_at",
    }

    FILL_MAP = {
        "shareholder_name": {
            "col_to_lookup": "id",
            "template": "UNKNOWN_{}"
        }
    }


    def __init__(self,
                 app_name="TransformCompanyShareholdersToSilver",
                 bronze_table="iceberg.bronze.company_shareholders",
                 silver_table="iceberg.silver.company_shareholders"):
        
        super().__init__(
            app_name=app_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            schema=self.SCHEMA,
            rename_map=self.RENAME_MAP,
            key_cols=["id"],
            trimmed_cols=["shareholder_name", "symbol"],
            positive_cols=["quantity", "ownership_percentage"],
            dropna_cols=["id", "symbol", "quantity", "ownership_percentage"],
            fill_map=self.FILL_MAP,
            scd_tracked_cols=["quantity", "ownership_percentage", "is_active"],
        )


    def transform(self, df):
        current_df =  super().transform(df)
        if current_df is None:
            return
        
        if "is_active" not in current_df.columns:
            current_df = current_df.withColumn("is_active", F.lit(True))
        
        try:
            query = f"SELECT * FROM {self.silver_table} WHERE is_active = True"
            existing_df = self.handler.spark.sql(query)
        except Exception as e:
            self.logger.error(f"Error reading {self.silver_table}: {e}")
            return None

        if not existing_df.rdd.isEmpty():
            missing_df = existing_df.join(current_df, on=["id"], how="left_anti")
            missing_df = missing_df.withColumn("is_active", F.lit(False))

            current_df = current_df.unionByName(missing_df)
        
        return current_df


if __name__ == "__main__":
    shareholder_transformer = CompanyShareholderTransformer()
    shareholder_transformer.run(scd_type="one")
