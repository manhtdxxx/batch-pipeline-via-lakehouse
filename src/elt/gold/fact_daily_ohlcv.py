# gold/fact_daily_ohlcv.py

import logging
from _gold_handler import GoldHandler
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pandas as pd
from typing import List


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class FactDailyOHLCV:
    def __init__(
        self,
        app_name="FactDailyOHLCV",
        silver_table="iceberg.silver.daily_ohlcv",
        gold_table="iceberg.gold.fact_daily_ohlcv"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.app_name = app_name
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.business_keys = ["symbol", "date"]
        self.cols_order = [
            "id", "symbol", "date",
            "open", "high", "low", "close", "volume",
            "ema_20", "ema_50", "ema_200", "rsi_14",
            "ingest_timestamp"
        ]


    def _read_days_ago_from_gold(self, gold_table: str, length: int = 60, spark: SparkSession = None) -> DataFrame:
        self.logger.info(f"Reading last 60 days per symbol from {gold_table}...")
        query = f"""
            SELECT * FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                FROM {gold_table}
            ) tmp
            WHERE rn <= {length}
        """
        df = spark.sql(query)
        self.logger.info(f"Read {df.count()} rows from gold table")
        return df
    

    def _calc_metrics(self) -> pd.DataFrame:
        schema = StructType([
            StructField("id", StringType()),
            StructField("symbol", StringType()),
            StructField("date", DateType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", LongType()),
            StructField("ingest_timestamp", TimestampType()),
            StructField("ema_20", DoubleType()),
            StructField("ema_50", DoubleType()),
            StructField("ema_200", DoubleType()),
            StructField("rsi_14", DoubleType())
        ])

        @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        def calc(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf = pdf.sort_values("date")
            # EMA
            pdf["ema_20"] = pdf["close"].ewm(span=20, adjust=False).mean()
            pdf["ema_50"] = pdf["close"].ewm(span=50, adjust=False).mean()
            pdf["ema_200"] = pdf["close"].ewm(span=200, adjust=False).mean()
            # RSI
            delta = pdf["close"].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.rolling(14, min_periods=14).mean()
            avg_loss = loss.rolling(14, min_periods=14).mean()
            rs = avg_gain / avg_loss
            pdf["rsi_14"] = 100 - (100 / (1 + rs))
            return pdf

        return calc        


    def run(self):
        self.logger.info("Starting FactDailyOHLCV pipeline...")
        handler = GoldHandler(self.app_name)

        df = handler.read_incremental_from_silver(silver_table=self.silver_table, gold_table=self.gold_table, date_col="date")
        if df.rdd.isEmpty():
            self.logger.info(f"No new data from {self.silver_table}. Skipping append to {self.gold_table}.")
            handler.stop()
            return
        
        df = handler.add_surrogate_key(df, key_cols=self.business_keys, sk_col_name="id", use_hash=False)

        df_ago = self._read_days_ago_from_gold(gold_table=self.gold_table, length=60, spark=handler.spark)
        if not df_ago.rdd.isEmpty():
            df_union = df.unionByName(df_ago)
        else:
            df_union = df
        
        df_union = df_union.groupby("symbol").apply(self._calc_metrics())

        df_union = df_union.select(*self.cols_order)
        handler.append_to_gold(df_union, gold_table=self.gold_table)
        handler.stop()

        self.logger.info("Done all!")


if __name__ == "__main__":
    fact = FactDailyOHLCV()
    fact.run()