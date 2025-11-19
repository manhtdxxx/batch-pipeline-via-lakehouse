# gold/dim_company.py

from _gold_handler import GoldHandler
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class DimCompany:
    def __init__(
        self,
        app_name="DimCompany",
        silver_table="iceberg.silver.company",
        silver_joined_table="iceberg.silver.industry",
        gold_table="iceberg.gold.dim_company"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.silver_joined_table = silver_joined_table
        self.gold_table = gold_table


    def run(self):
        handler = GoldHandler(self.app_name)

        df_company = handler.read_incremental_from_silver(silver_table=self.silver_table, gold_table=self.gold_table)
        if df_company.rdd.isEmpty():
            self.logger.info("No new company data to append. Exiting.")
            handler.stop()
            return

        df_industry = handler.spark.sql(f"SELECT * FROM {self.silver_joined_table}")

        df_joined = df_company.alias("c").join(df_industry.alias("i"),
                                               on=col("c.icb_code_1") == col("i.icb_code"),
                                               how="left")
        df_joined = df_joined.select(
            col("c.symbol"),
            col("c.company_name"),
            col("c.icb_code_1").alias("icb_code"),
            col("i.icb_name"),
            col("i.en_icb_name"),
            col("c.ingest_timestamp")
        )

        handler.append_to_gold(df_joined, self.gold_table)
        handler.stop()
        self.logger.info("Done all.")


if __name__ == "__main__":
    dim_company = DimCompany()
    dim_company.run()
