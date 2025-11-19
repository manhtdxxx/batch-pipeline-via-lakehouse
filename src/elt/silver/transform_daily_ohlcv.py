# silver/transform_daily_ohlcv.py

from _silver_pipeline import SilverPipeline
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, DateType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import last, col, coalesce, lit


class DailyOHLCVTransformer(SilverPipeline):
    SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("time", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("ingest_timestamp", TimestampType(), False),
    ])

    RENAME_MAP = {
        "time": "date",
    }


    def __init__(self,
                 app_name="TransformDailyOHLCVToSilver",
                 bronze_table="iceberg.bronze.daily_ohlcv",
                 silver_table="iceberg.silver.daily_ohlcv"):

        super().__init__(
            app_name=app_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            schema=self.SCHEMA,
            rename_map=self.RENAME_MAP,
            key_cols=["symbol", "date"],
            trimmed_cols=["symbol"],
            positive_cols=["open", "high", "low", "close", "volume"],
            dropna_cols=["symbol", "date"],
            fill_map={}
        )


    def _read_latest_ohlcv(self) -> DataFrame:
        query = f"""
            SELECT symbol, date, open, high, low, close, volume
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY date DESC) as rn
                FROM {self.silver_table}    
            )
            WHERE rn = 1
        """
        return self.handler.spark.sql(query)


    def fill_null(self, df: DataFrame) -> DataFrame:
        df_prev = self._read_latest_ohlcv()

        if df_prev.rdd.isEmpty():
            df_all = df
        else:
            df_all = df_prev.unionByName(df)

        # forward fill for price
        w = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        for col_name in ["open", "high", "low", "close"]:
            df_all = df_all.withColumn(col_name, last(col_name, ignorenulls=True).over(w))

        # fill null with 0 for volume
        df_all = df_all.withColumn("volume", coalesce(col("volume"), lit(0)))

        # exclude the row read from silver
        df_filled = df_all.join(
            df.select("symbol", "date"),
            on=["symbol", "date"],
            how="inner"
        )
        return df_filled


    def transform(self, df):
        df = super().transform(df)
        if df is None:
            return None
        
        df = self.fill_null(df)
        return df


if __name__ == "__main__":
    ohlcv_transformer = DailyOHLCVTransformer()
    ohlcv_transformer.run(overwrite=True)
