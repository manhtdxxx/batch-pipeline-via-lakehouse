# silver/transform_company.py

from _silver_pipeline import SilverPipeline
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class CompanyTransformer(SilverPipeline):
    SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("organ_name", StringType(), True),
        StructField("icb_code1", StringType(), True),
        StructField("icb_code2", StringType(), True),
        StructField("icb_code3", StringType(), True),
        StructField("icb_code4", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), False)
    ])
        
    RENAME_MAP = {
        "organ_name": "company_name",
        "icb_code1": "icb_code_1",
        "icb_code2": "icb_code_2",
        "icb_code3": "icb_code_3",
        "icb_code4": "icb_code_4",
    }

    def __init__(self,
                 app_name="TransformCompanyToSilver",
                 bronze_table="iceberg.bronze.company",
                 silver_table="iceberg.silver.company"):
        
        super().__init__(
            app_name=app_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            schema=self.SCHEMA,
            rename_map=self.RENAME_MAP,
            key_cols=["symbol"],
            trimmed_cols=["company_name", "icb_code_1", "icb_code_2", "icb_code_3", "icb_code_4"],
            dropna_cols=["symbol"],
            fill_map={},  # if symbol=... then company_name=...., icb_code=...
            scd_tracked_cols=["company_name", "icb_code_1", "icb_code_2", "icb_code_3", "icb_code_4"],
        )


if __name__ == "__main__":
    company_transformer = CompanyTransformer()
    company_transformer.run(scd_type="one")