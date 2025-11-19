# silver/transform_industry.py

from _silver_pipeline import SilverPipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class IndustryTransformer(SilverPipeline):
    SCHEMA = StructType([
        StructField("icb_code", StringType(), True),
        StructField("level", IntegerType(), True),
        StructField("icb_name", StringType(), True),
        StructField("en_icb_name", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), False)
    ])
        

    def __init__(self,
                 app_name="TransformIndustryToSilver",
                 bronze_table="iceberg.bronze.industry",
                 silver_table="iceberg.silver.industry"):
        
        super().__init__(
            app_name=app_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            schema=self.SCHEMA,
            key_cols=["icb_code"],
            trimmed_cols=["icb_code", "icb_name", "en_icb_name"],
            positive_cols=["level"],
            dropna_cols=["icb_code"],
            fill_map={},  # if icb_code=... then level=..., icb_name=..., en_icb_name=...
            scd_tracked_cols=["level", "icb_name", "en_icb_name"]
        )


if __name__ == "__main__":
    industry_transformer = IndustryTransformer()
    industry_transformer.run(scd_type="one")