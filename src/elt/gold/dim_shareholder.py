# gold/dim_shareholder.py

from _gold_handler import GoldHandler
from pyspark.sql.functions import col
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class DimShareholder:
    def __init__(
        self,
        app_name="DimShareholder",
        silver_table="iceberg.silver.company_shareholders",
        gold_table="iceberg.gold.dim_shareholder"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.gold_table = gold_table


    def run(self):
        handler = GoldHandler(self.app_name)

        df = handler.read_incremental_from_silver(silver_table=self.silver_table, gold_table=self.gold_table)
        if df.rdd.isEmpty():
            self.logger.info("No new shareholder data to append. Exiting.")
            handler.stop()
            return

        df = df.filter(col("is_active") == True).drop("is_active").drop("updated_at")

        handler.append_to_gold(df, self.gold_table)
        handler.stop()
        self.logger.info("Done all.")


if __name__ == "__main__":
    dim_shareholder = DimShareholder()
    dim_shareholder.run()
