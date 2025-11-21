# gold/fact_quarterly_ratio.py

from _gold_handler import GoldHandler
from pyspark.sql.functions import col, when, lit
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
        self.key_cols = ["symbol", "year", "quarter"]
        self.cols_order = [
            "id", "symbol", "date",
            "roa", "roe", "net_profit_margin",
            "market_capital", "outstanding_share", "price_to_earnings", "price_to_book_value", "price_to_sales",
            "eps", "bvps", "revenue", "net_profit", "asset", "equity", "debt",
            "debt_to_asset", "financial_leverage", "asset_turnover",
            "ingest_timestamp"
        ]

    
    def _calc_price(self) -> Column:
        return col("market_capital") / col("outstanding_share")
    
    def _calc_eps(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating EPS...")
        return df.withColumn("eps", ((self._calc_price()) / col("price_to_earnings")).cast(DoubleType()))

    def _calc_bvps(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating BVPS...")
        return df.withColumn("bvps", (self._calc_price() / col("price_to_book_value")).cast(DoubleType()))

    def _calc_revenue(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Revenue...")
        return df.withColumn("revenue", (col("market_capital") / col("price_to_sales")).cast(DoubleType()))
    
    def _calc_net_profit(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Net Profit...")
        return df.withColumn("net_profit", (col("revenue") * col("net_profit_margin")).cast(DoubleType()))

    def _calc_asset(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Asset...")
        return df.withColumn("asset", (col("net_profit") / col("roa")).cast(DoubleType()))

    def _calc_equity(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Equity...")
        return df.withColumn("equity", (col("net_profit") / col("roe")).cast(DoubleType()))

    def _calc_debt(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Debt...")
        return df.withColumn("debt", (col("asset") - col("equity")).cast(DoubleType()))

    def _calc_debt_to_asset(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating D/A...")
        return df.withColumn("debt_to_asset", (col("debt") / col("asset")).cast(DoubleType()))

    def _calc_financial_leverage(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Financial Leverage...")
        return df.withColumn("financial_leverage", (col("asset") / col("equity")).cast(DoubleType()))

    def _calc_asset_turnover(self, df: DataFrame) -> DataFrame:
        self.logger.info("Calculating Asset Turnover...")
        return df.withColumn("asset_turnover",(col("roa") / col("net_profit_margin")).cast(DoubleType()))
    
    def calc_additional_metrics(self, df: DataFrame) -> DataFrame:
        self.logger.info("Starting metric calculation pipeline...")
        df = self._calc_eps(df)
        df = self._calc_bvps(df)
        df = self._calc_revenue(df)
        df = self._calc_net_profit(df)
        df = self._calc_asset(df)
        df = self._calc_equity(df)
        df = self._calc_debt(df)
        df = self._calc_debt_to_asset(df)
        df = self._calc_financial_leverage(df)
        df = self._calc_asset_turnover(df)
        return df
        

    def _clamp_metric(self, df: DataFrame, col_name: str, min_val: float, max_val: float) -> DataFrame:
        """
        if val > 0, then clip in range of [min_val, max_val]
        if val < 0, then clip in range of [-max_val, -min_val]
        """
        self.logger.info(f"Clamping metric {col_name}...")
        return df.withColumn(
            col_name,
            when(
                col(col_name) > 0,
                when(col(col_name) < min_val, lit(min_val))
                .when(col(col_name) > max_val, lit(max_val))
                .otherwise(col(col_name))
            ).when(
                col(col_name) < 0,
                when(col(col_name) > -min_val, lit(-min_val))
                .when(col(col_name) < -max_val, lit(-max_val))
                .otherwise(col(col_name))
            ).otherwise(col(col_name))  # remains if = 0
        )
    
    def clamp_existing_metrics(self, df: DataFrame) -> DataFrame:
        self.logger.info("Starting metric clamping pipeline...")
        df = self._clamp_metric(df, "roa", 0.001, 0.2)
        df = self._clamp_metric(df, "roe", 0.005, 0.3)
        df = self._clamp_metric(df, "net_profit_margin", 0.01, 0.5)
        df = self._clamp_metric(df, "price_to_earnings", 5.0, 40.0)
        df = self._clamp_metric(df, "price_to_book_value", 0.5, 6.0)
        df = self._clamp_metric(df, "price_to_sales", 0.5, 7.0)
        return df

    
    def fix_sign_of_npm(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "net_profit_margin",
            when(
                (col("roa") < 0) & (col("net_profit_margin") > 0),
                -col("net_profit_margin")
            ).when(
                (col("roa") > 0) & (col("net_profit_margin") < 0),
                -col("net_profit_margin")
            ).otherwise(col("net_profit_margin"))
        )
    

    def run(self):
        handler = GoldHandler(self.app_name)

        df = handler.read_incremental_from_silver(silver_table=self.silver_table, gold_table=self.gold_table)
        if df.rdd.isEmpty():
            self.logger.info(f"No new data from {self.silver_table}. Skipping append to {self.gold_table}.")
            handler.stop()
            return

        df = handler.add_surrogate_key(df, key_cols=self.key_cols, sk_col_name="id", use_hash=False)
        df = handler.add_date_col(df)

        df = self.fix_sign_of_npm(df)
        df = self.clamp_existing_metrics(df)
        df = self.calc_additional_metrics(df)

        df = df.select(*self.cols_order)
        handler.append_to_gold(df, gold_table=self.gold_table)
        handler.stop()

        self.logger.info(f"Done all!")


if __name__ == "__main__":
    fact = FactQuarterlyRatio()
    fact.run()