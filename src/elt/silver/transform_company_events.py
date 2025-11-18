# silver/transform_company_events.py

from _silver_pipeline import SilverPipeline
import logging
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, TimestampType, DoubleType
from pyspark.sql import functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class CompanyEventTransformer(SilverPipeline):
    SCHEMA = StructType([
        StructField("id", StringType(), True),
        StructField("event_list_code", StringType(), True),
        StructField("event_list_name", StringType(), True),
        StructField("en__event_list_name", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("issue_date", DateType(), True),
        StructField("ratio", DoubleType(), True),
        StructField("value", LongType(), True),
        StructField("ingest_timestamp", TimestampType(), False)
    ])
        
    RENAME_MAP = {
        "event_list_code": "event_code",
        "event_list_name": "event_name",
        "en__event_list_name": "en_event_name",
    }

    FILL_MAP = {
        "event_name": {
            "col_to_lookup": "event_code",
            "map": {
                "DIV": "Trả cổ tức bằng tiền mặt",
                "AIS": "Niêm yết thêm",
                "ISS": "Phát hành cổ phiếu"
            }
        },
        "en_event_name": {
            "col_to_lookup": "event_code",
            "map": {
                "DIV": "Cash Dividend",
                "AIS": "Additional Listing",
                "ISS": "Share Issue"
            }
        }
    }


    def __init__(
        self,
        app_name="TransformCompanyEventsToSilver",
        bronze_table="iceberg.bronze.company_events",
        silver_table="iceberg.silver.company_events"
    ):
        super().__init__(
            app_name=app_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            schema=self.SCHEMA,
            rename_map=self.RENAME_MAP,
            key_cols=["id"],
            trimmed_cols=["event_code", "event_name", "en_event_name", "symbol"],
            zero_and_positive_cols=["ratio", "value"],
            dropna_cols=["id", "event_code", "symbol", "issue_date"],
            fill_map=self.FILL_MAP,
        )


if __name__ == "__main__":
    event_transformer = CompanyEventTransformer()
    event_transformer.run(overwrite=True)