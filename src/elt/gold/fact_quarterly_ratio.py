# gold/fact_quarterly_ratio.py

from _gold_handler import GoldHandler
from pyspark.sql.functions import col, lit, when, to_date, concat
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.types import DoubleType
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class FactQuarterlyRatio:
    def __init__(
        self,
        app_name="FactQuarterlyRatio",
        silver_table="iceberg.silver.quarterly_ratio",
        gold_table="iceberg.gold.fact_quarterly_ratio"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.cols_order = [
            "id", "symbol", "date",
            "roa", "roe", "net_profit_margin",
            "financial_leverage", "debt_to_equity",
            "market_capital", "outstanding_share",
            "price_to_earnings", "price_to_book_value", "price_to_sales",
            "eps", "bvps", "revenue","asset_turnover",
            "ingest_timestamp"
        ]


    def _add_date_col(self, df: DataFrame) -> DataFrame:
        self.logger.info("Adding date column from year & quarter...")
        df = df.withColumn(
            "date", 
            to_date(
                concat(
                    col("year"),
                    lit("-"),
                    when(col("quarter") == 1, lit("03-31"))
                    .when(col("quarter") == 2, lit("06-30"))
                    .when(col("quarter") == 3, lit("09-30"))
                    .when(col("quarter") == 4, lit("12-31"))
                ),
                "yyyy-MM-dd"
            )
        )
        df = df.drop("year", "quarter")
        return df
    
    def _calc_price(self) -> Column:
        return col("market_capital") / col("outstanding_share")
    
    def _calc_eps(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating EPS...")
        return df.withColumn("eps", (self._calc_price() / col("price_to_earnings")).cast(DoubleType()))

    def _calc_bvps(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating BVPS...")
        return df.withColumn("bvps", (self._calc_price() / col("price_to_book_value")).cast(DoubleType()))

    def _calc_revenue(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Revenue...")
        return df.withColumn("revenue", (col("market_capital") / col("price_to_sales")).cast(DoubleType()))

    def _calc_asset_turnover(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Asset Turnover...")
        return df.withColumn("asset_turnover", (col("roe") / (col("net_profit_margin") * col("financial_leverage"))).cast(DoubleType()))
    

    def calc_metrics(self, df: DataFrame) -> DataFrame:
        self.logger.info("Starting metric calculation pipeline...")
        df = self._add_date_col(df)
        df = self._calc_eps(df)
        df = self._calc_bvps(df)
        df = self._calc_revenue(df)
        df = self._calc_asset_turnover(df)
        return df
    

    def run(self):
        handler = GoldHandler(self.app_name)

        df = handler.read_incremental_from_silver(silver_table=self.silver_table, gold_table=self.gold_table)
        if df.rdd.isEmpty():
            self.logger.info(f"No new data from {self.silver_table}. Skipping append to {self.gold_table}.")
            handler.stop()
            return

        df = self.calc_metrics(df)
        df = df.select(*self.cols_order)

        handler.append_to_gold(df, gold_table=self.gold_table)
        handler.stop()

        self.logger.info(f"Done all!")


if __name__ == "__main__":
    fact = FactQuarterlyRatio()
    fact.run()