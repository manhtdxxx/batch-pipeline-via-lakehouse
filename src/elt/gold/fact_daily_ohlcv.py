import logging
from _gold_handler import GoldHandler
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType,
    TimestampType, IntegerType
)
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
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
            "return",
            "range", "body", "upper_wick", "lower_wick", "body_ratio", "upper_ratio", "lower_ratio", "is_green",
            "ema_10", "ema_20", "ema_10_dist_pct", "ema_20_dist_pct",
            "rsi_14", "rsi_14_scaled",
            "vol_ema_10", "rvol_10",
            "pct_change_std", "label_1", "label_2", "label_3",
            "ingest_timestamp"
        ]

    def _read_days_ago_from_gold(self, gold_table: str, length: int = 30, spark: SparkSession = None) -> DataFrame:
        self.logger.info(f"Reading last {length} days per symbol from {gold_table}...")
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

    def _calc_metrics(self):
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
            StructField("return", DoubleType()),
            StructField("range", DoubleType()),
            StructField("body", DoubleType()),
            StructField("upper_wick", DoubleType()),
            StructField("lower_wick", DoubleType()),
            StructField("body_ratio", DoubleType()),
            StructField("upper_ratio", DoubleType()),
            StructField("lower_ratio", DoubleType()),
            StructField("is_green", IntegerType()),
            StructField("ema_10", DoubleType()),
            StructField("ema_20", DoubleType()),
            StructField("ema_10_dist_pct", DoubleType()),
            StructField("ema_20_dist_pct", DoubleType()),
            StructField("rsi_14", DoubleType()),
            StructField("rsi_14_scaled", DoubleType()),
            StructField("vol_ema_10", LongType()),
            StructField("rvol_10", DoubleType()),
            StructField("pct_change_std", DoubleType()),
            StructField("label_1", IntegerType()),
            StructField("label_2", IntegerType()),
            StructField("label_3", IntegerType())
        ])

        @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        def calc(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf = pdf.sort_values("date")

            # Return
            pdf["return"] = (pdf["close"] - pdf["close"].shift(1)) / pdf["close"].shift(1)

            # Candle metrics
            pdf["range"] = pdf["high"] - pdf["low"]
            pdf["body"] = pdf["close"] - pdf["open"]
            pdf["upper_wick"] = pdf["high"] - pdf[["open", "close"]].max(axis=1)
            pdf["lower_wick"] = pdf[["open", "close"]].min(axis=1) - pdf["low"]

            pdf["body_ratio"] = (pdf["body"].abs() / pdf["range"]).fillna(0)
            pdf["upper_ratio"] = (pdf["upper_wick"] / pdf["range"]).fillna(0)
            pdf["lower_ratio"] = (pdf["lower_wick"] / pdf["range"]).fillna(0)
            pdf["is_green"] = (pdf["body"] > 0).astype(int)

            # EMA
            pdf["ema_10"] = pdf["close"].ewm(span=10, adjust=False).mean()
            pdf["ema_20"] = pdf["close"].ewm(span=20, adjust=False).mean()
            pdf["ema_10_dist_pct"] = (pdf["close"] - pdf["ema_10"]) / pdf["ema_10"]
            pdf["ema_20_dist_pct"] = (pdf["close"] - pdf["ema_20"]) / pdf["ema_20"]

            # RSI
            delta = pdf["close"].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.rolling(14, min_periods=14).mean()
            avg_loss = loss.rolling(14, min_periods=14).mean()
            rs = avg_gain / avg_loss
            pdf["rsi_14"] = 100 - (100 / (1 + rs))
            pdf["rsi_14_scaled"] = pdf["rsi_14"] / 100.0

            # Volume metrics
            pdf["vol_ema_10"] = pdf["volume"].ewm(span=10, adjust=False).mean()
            pdf["rvol_10"] = pdf["volume"] / pdf["vol_ema_10"]

            # Label calculation
            std_window = 20
            pdf['pct_change_std'] = pdf['close'].pct_change().rolling(std_window).std()
            
            future_window = 3
            future_return = (pdf['close'].shift(-future_window) - pdf['close']) / pdf['close']

            factors = [0.4, 0.5, 0.6]
            for idx, k in enumerate(factors, start=1):
                threshold = k * (np.sqrt(future_window) * pdf['pct_change_std'])
                pdf[f"label_{idx}"] = np.where(future_return.isna(), np.nan, 
                                               np.where(future_return > threshold, 2,
                                                        np.where(future_return < -threshold, 0, 
                                                                 1)))

            return pdf
        return calc


    def run(self):
        self.logger.info("Starting FactDailyOHLCV pipeline...")
        handler = GoldHandler(self.app_name)

        df = handler.read_incremental_from_silver(
            silver_table=self.silver_table,
            gold_table=self.gold_table,
            date_col="date"
        )

        if df.rdd.isEmpty():
            self.logger.info(f"No new data from {self.silver_table}. Skipping append to {self.gold_table}.")
            handler.stop()
            return

        df = handler.add_surrogate_key(df, key_cols=self.business_keys, sk_col_name="id", use_hash=False)

        df_ago = self._read_days_ago_from_gold(gold_table=self.gold_table, length=30, spark=handler.spark)
        df_union = df.unionByName(df_ago) if not df_ago.rdd.isEmpty() else df

        df_union = df_union.groupby("symbol").apply(self._calc_metrics())
        df_union = df_union.select(*self.cols_order)

        handler.append_to_gold(df_union, gold_table=self.gold_table)
        handler.stop()
        self.logger.info("Done all!")


if __name__ == "__main__":
    fact = FactDailyOHLCV()
    fact.run()
