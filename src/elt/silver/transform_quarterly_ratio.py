# silver/transform_quarterly_ratio.py

from _silver_pipeline import SilverPipeline
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class QuarterlyRatioTransformer(SilverPipeline):
    SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("roa", DoubleType(), True),
        StructField("roe", DoubleType(), True),
        StructField("net_profit_margin", DoubleType(), True),
        StructField("financial_leverage", DoubleType(), True),
        StructField("debt_to_equity", DoubleType(), True),
        StructField("market_capital", DoubleType(), True),
        StructField("outstanding_share", LongType(), True),
        StructField("price_to_earnings", DoubleType(), True),
        StructField("price_to_book_value", DoubleType(), True),
        StructField("price_to_sales", DoubleType(), True),
        StructField("ingest_timestamp", TimestampType(), False),
    ])

    RENAME_MAP = {
        "ticker": "symbol",
        "yearReport": "year",
        "lengthReport": "quarter",
        "ROA (%)": "roa",
        "ROE (%)": "roe",
        "Net Profit Margin (%)": "net_profit_margin",
        "Financial Leverage": "financial_leverage",
        "Debt/Equity": "debt_to_equity",
        "Market Capital (Bn. VND)": "market_capital",
        "Outstanding Share (Mil. Shares)": "outstanding_share",
        "P/E": "price_to_earnings",
        "P/B": "price_to_book_value",
        "P/S": "price_to_sales"
    }


    def __init__(self,
                 app_name="TransformQuarterlyRatioToSilver",
                 bronze_table="iceberg.bronze.quarterly_ratio",
                 silver_table="iceberg.silver.quarterly_ratio"):

        super().__init__(
            app_name=app_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            schema=self.SCHEMA,
            rename_map=self.RENAME_MAP,
            key_cols=["symbol", "year", "quarter"],
            trimmed_cols=["symbol"],
            dropna_cols=["symbol", "year", "quarter"],
            fill_map={},  # if null, then fill 0,
        )


    def transform(self, df):
        # rename first
        if self.rename_map:
            df = self.handler.rename_cols(df, self.rename_map)

        print(df.columns)

        # turn off to avoid rename again
        self.rename_map = {}

        df = super().transform(df)
        if df is None:
            return None

        df = self.handler.add_suggorate_key(df, key_cols=self.key_cols, sk_col_name="id", use_hash=False)

        print(df.columns)

        cols = ['id'] + [col for col in df.columns if col != "id"]
        return df.select(*cols)


if __name__ == "__main__":
    quarterly_transformer = QuarterlyRatioTransformer()
    quarterly_transformer.run(overwrite=True)
