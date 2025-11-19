# gold/fact_cash_dividend.py

from _gold_handler import GoldHandler
from pyspark.sql.functions import col
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class FactCashDividend:
    def __init__(
        self,
        app_name="FactCashDividend",
        silver_table="iceberg.silver.company_events",
        gold_table="iceberg.gold.fact_cash_dividend"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.cols_order = [
            "id", "symbol", "issue_date", "ratio", "value", "ingest_timestamp"
        ]


    def run(self):
        handler = GoldHandler(self.app_name)

        df_event = handler.read_incremental_from_silver(
            silver_table=self.silver_table, gold_table=self.gold_table
        )
        if df_event.rdd.isEmpty():
            self.logger.info("No new dividend events. Skipping append.")
            handler.stop()
            return

        # Filter only dividend events
        df_div = df_event.filter(col("event_code") == "DIV")
        if df_div.rdd.isEmpty():
            self.logger.info("No Dividend events found. Skipping.")
            handler.stop()
            return

        df_div = df_div.select(*self.cols_order)
        handler.append_to_gold(df_div, gold_table=self.gold_table)
        handler.stop()

        self.logger.info("Done all!")


if __name__ == "__main__":
    fact = FactCashDividend()
    fact.run()
