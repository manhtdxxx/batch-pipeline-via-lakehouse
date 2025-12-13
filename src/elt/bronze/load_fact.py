# bronze/load_fact.py

from _bronze_handler import BronzeHandler
import argparse
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

class FactLoader:
    def __init__(self, app_name: str, bronze_table: str, fact_type: str, file_path: str = None, use_vnstock: bool = False, options: dict = None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.app_name = app_name
        self.bronze_table = bronze_table
        self.fact_type = fact_type
        self.file_path = file_path
        self.use_vnstock = use_vnstock
        self.options = options or {}

        self._validate()


    def _validate(self):
        self.logger.info("Validating arguments...")

        if not self.fact_type:
            raise ValueError("fact_type is required")

        if self.use_vnstock:
            if self.file_path:
                raise ValueError("Do not pass file_path when use_vnstock=True")
        else:
            if not self.file_path:
                raise ValueError("file_path is required when not using vnstock")

        self.logger.info("Validation passed")


    def _load_from_vnstock(self):
        from _vnstock_handler import VnstockHandler
        vnstock = VnstockHandler()

        self.logger.info("Start loading from vnstock | fact_type=%s", self.fact_type)

        if self.fact_type == "quarterly_ratio":
            return vnstock.fetch_ratio_df(
                start_year=self.options.get("start_year"),
                end_year=self.options.get("end_year"),
                period="quarter",
            )

        if self.fact_type == "daily_ohlcv":
            return vnstock.fetch_ohlcv_df(
                start_date=self.options.get("start_date"),
                end_date=self.options.get("end_date"),
                interval="1D",
            )

        if self.fact_type == "company_events":
            return vnstock.fetch_company_events_df(
                start_date=self.options.get("start_date"),
                end_date=self.options.get("end_date"),
            )

        if self.fact_type == "company_news":
            return vnstock.fetch_company_news_df(
                start_date=self.options.get("start_date"),
                end_date=self.options.get("end_date"),
            )

        raise ValueError(f"Unsupported fact_type: {self.fact_type}")


    def _apply_file_filters(self, df):
        self.logger.info("Applying filters | fact_type=%s", self.fact_type)

        if self.fact_type == "quarterly_ratio":
            if self.options.get("start_year"):
                df = df.filter(df["yearReport"] >= self.options["start_year"])
            if self.options.get("end_year"):
                df = df.filter(df["yearReport"] <= self.options["end_year"])

        elif self.fact_type == "daily_ohlcv":
            if self.options.get("start_date"):
                df = df.filter(df["time"] >= self.options["start_date"])
            if self.options.get("end_date"):
                df = df.filter(df["time"] <= self.options["end_date"])

        elif self.fact_type == "company_events":
            if self.options.get("start_date"):
                df = df.filter(df["issue_date"] >= self.options["start_date"])
            if self.options.get("end_date"):
                df = df.filter(df["issue_date"] <= self.options["end_date"])

        elif self.fact_type == "company_news":
            if self.options.get("start_date"):
                df = df.filter(df["public_date"] >= self.options["start_date"])
            if self.options.get("end_date"):
                df = df.filter(df["public_date"] <= self.options["end_date"])

        else:
            raise ValueError(f"Unsupported fact_type: {self.fact_type}")

        return df


    def run(self):
        handler = BronzeHandler(app_name=self.app_name)

        if self.use_vnstock:
            df = self._load_from_vnstock()
        else:
            self.logger.info("Start loading from file %s | fact_type=%s", self.file_path, self.fact_type)
            df = handler.read_file(self.file_path, format_type="csv", header=True, inferSchema=False)
            df = self._apply_file_filters(df)

        df = handler.add_metadata_cols(df, ingest_year=True, ingest_month=True)

        handler.load_to_bronze(
            df=df,
            table_name=self.bronze_table,
            partition_cols=["ingest_year", "ingest_month"],
        )

        handler.stop()
        self.logger.info("Finished successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Load Fact to Bronze")

    parser.add_argument("--app_name", required=True)
    parser.add_argument("--bronze_table", required=True)
    parser.add_argument("--fact_type", required=True)
    parser.add_argument("--file_path")
    parser.add_argument("--use_vnstock", action="store_true")
    parser.add_argument("--options", type=str)  # optional: json string

    args = parser.parse_args()

    options = json.loads(args.options) if args.options else None

    loader = FactLoader(
        app_name=args.app_name,
        bronze_table=args.bronze_table,
        fact_type=args.fact_type,
        file_path=args.file_path,
        use_vnstock=args.use_vnstock,
        options=options,
    )

    loader.run()
