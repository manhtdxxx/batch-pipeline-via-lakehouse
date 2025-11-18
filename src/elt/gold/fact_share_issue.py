# gold/fact_share_issue.py

from _gold_handler import GoldHandler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, quarter, desc, row_number
from pyspark.sql.window import Window
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class FactShareIssue:
    def __init__(
        self,
        app_name="FactShareIssue",
        silver_table="iceberg.silver.company_events",
        silver_joined_table="iceberg.silver.quarterly_ratio",
        gold_table="iceberg.gold.fact_share_issue"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.silver_joined_table = silver_joined_table
        self.gold_table = gold_table
        self.cols_order = [
            "id", "symbol", "issue_date", "ratio", "value", "approx_share_before_issue", "ingest_timestamp"
        ]


    def run(self):
        handler = GoldHandler(self.app_name)

        df_event = handler.read_incremental_from_silver(silver_table=self.silver_table, gold_table=self.gold_table)
        if df_event.rdd.isEmpty():
            self.logger.info("No new share issue events. Skipping append.")
            handler.stop()
            return

        # Filter only share issue events
        df_event = df_event.filter(col("event_code") == "ISS")

        ...


        handler.append_to_gold(df, gold_table=self.gold_table)
        handler.stop()

        self.logger.info("Done all!")


if __name__ == "__main__":
    fact = FactShareIssue()
    fact.run()
