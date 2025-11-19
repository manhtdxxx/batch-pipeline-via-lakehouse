# gold/fact_share_issue.py

from _gold_handler import GoldHandler
from pyspark.sql.functions import col, min, max, desc, row_number, coalesce, lit
from pyspark.sql.types import LongType
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
            "id", "e.symbol", "issue_date", "ratio", "approx_share_before_issue", "ingest_timestamp"
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
        if df_event.rdd.isEmpty():
            self.logger.info("No Issue Share events found. Skipping.")
            handler.stop()
            return

        # Read quarterly_ratio to get outstanding_share
        min_issue_date, max_issue_date = df_event.agg(
            min("issue_date"), max("issue_date")
        ).first()
        # if min_issue_date, month = 1, then we need to take previous year
        df_ratio = handler.spark.sql(f"""
            SELECT symbol, year, quarter, outstanding_share 
            FROM {self.silver_joined_table}
            WHERE year BETWEEN {min_issue_date.year - 1} AND {max_issue_date.year}
        """)
        df_ratio = handler.add_date_col(df_ratio)

        # Join to get nearest quarter before issue_date
        w = Window.partitionBy("e.id").orderBy(desc("r.date"))
        df_joined = (
            df_event.alias("e")
            .join(
                df_ratio.alias("r"), 
                on=[col("e.symbol") == col("r.symbol"), col("r.date") <= col("e.issue_date")],
                how="left")
            .withColumn("row_num", row_number().over(w))
            .filter(col("row_num") == 1)
        )

        df_joined = df_joined.withColumn(
            "approx_share_before_issue",
            coalesce(col("r.outstanding_share"), lit(0)).cast(LongType())
        )

        df_joined = df_joined.select(*self.cols_order)

        handler.append_to_gold(df_joined, gold_table=self.gold_table)
        handler.stop()

        self.logger.info("Done all!")


if __name__ == "__main__":
    fact = FactShareIssue()
    fact.run()
