# gold/dim_date.py

from _gold_handler import GoldHandler
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sequence, explode, min, max, trunc, when, concat, lit
from pyspark.sql.functions import year, quarter, month, dayofyear, dayofmonth, dayofweek, date_format
from pyspark.sql.types import DateType
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class DimDate:
    def __init__(
        self,
        app_name="DimDate",
        silver_table="iceberg.silver.daily_ohlcv",
        gold_table="iceberg.gold.dim_date"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.holidays = []
    
    
    def extract_date(self, df: DataFrame, date_col: str = "date") -> DataFrame:
        self.logger.info(f"Creating dim_date from column: {date_col}")
        df_mm = df.agg(
            min(col(date_col)).alias("min_date"),
            max(col(date_col)).alias("max_date")
        )
        # shift to make min_date always start from dd=01
        df_mm = df_mm.withColumn("min_date", trunc(col("min_date"), "month"))

        df_date = df_mm.select(
            explode(
                sequence(
                    col("min_date").cast(DateType()),
                    col("max_date").cast(DateType())
                )
            ).alias("date")
        )
        return df_date
    
    
    def add_date_attributes(self, df: DataFrame) -> DataFrame:
        self.logger.info("Adding date attributes...")

        df = df.withColumn("year", year(col("date"))) \
                .withColumn("quarter_num", quarter(col("date"))) \
                .withColumn("quarter", concat(lit("Q"), col("quarter_num"))) \
                .withColumn("month_num", month(col("date"))) \
                .withColumn("month", date_format(col("date"), "MMM")) \
                .withColumn("weekday_num", dayofweek(col("date"))) \
                .withColumn("weekday", date_format(col("date"), "EEE"))
        
        df = df.withColumn(
            "is_weekend", 
            when(col("weekday_num").isin([1, 7]), True).otherwise(False)
        )
        return df


    def run(self):
        handler = GoldHandler(self.app_name)

        df = handler.read_incremental_from_silver(self.silver_table, self.gold_table, timestamp_col=None, date_col="date")
        if df.rdd.isEmpty():
            self.logger.info("No new dates to append. Exiting.")
            handler.stop()
            return

        df_date = self.extract_date(df, date_col="date")
        df_date = self.add_date_attributes(df_date)

        handler.append_to_gold(df_date, self.gold_table)
        handler.stop()
        self.logger.info("Done all")


if __name__ == "__main__":
    dim_date = DimDate()
    dim_date.run()
