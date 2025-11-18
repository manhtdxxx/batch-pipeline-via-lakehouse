from _bronze_handler import BronzeHandler
import os


def load_fact(
        app_name: str,
        bronze_table: str,
        fact_type: str,
        file_path: str = None,
        use_vnstock: bool = False,
        options: dict = None
    ):
  
    handler = BronzeHandler(app_name=app_name)
    options = options or {}

    if use_vnstock:
        from _vnstock_handler import VnstockHandler
        vnstock_handler = VnstockHandler()

        if fact_type == "quarterly_ratio":
            df = vnstock_handler.fetch_ratio_df(
                start_year=options.get("start_year"),
                end_year=options.get("end_year"),
                period="quarter"
            )

        elif fact_type == "daily_ohlcv":
            df = vnstock_handler.fetch_ohlcv_df(
                start_date=options.get("start_date"),
                end_date=options.get("end_date"),
                interval="1D"
            )

        elif fact_type == "company_events":
            df = vnstock_handler.fetch_company_events_df(
                start_date=options.get("start_date"),
                end_date=options.get("end_date")
            )

        elif fact_type == "company_news":
            df = vnstock_handler.fetch_company_news_df(
                start_date=options.get("start_date"),
                end_date=options.get("end_date")
            )

        else:
            raise ValueError(f"Invalid fact_type '{fact_type}'.")
    
    elif file_path and not use_vnstock:
        df = handler.read_file(file_path, format_type="csv", header=True, inferSchema=False)

        if fact_type == "quarterly_ratio":
            if "start_year" in options:
                df = df.filter(df["yearReport"] >= options["start_year"])
            if "end_year" in options and options["end_year"]:
                df = df.filter(df["yearReport"] <= options["end_year"])

        elif fact_type == "daily_ohlcv":
            if "start_date" in options:
                df = df.filter(df["time"] >= options["start_date"])
            if "end_date" in options and options["end_date"]:
                df = df.filter(df["time"] <= options["end_date"])

        elif fact_type == "company_events":
            if "start_date" in options:
                df = df.filter(df["issue_date"] >= options["start_date"])
            if "end_date" in options and options["end_date"]:
                df = df.filter(df["issue_date"] <= options["end_date"])

        elif fact_type == "company_news":
            if "start_date" in options:
                df = df.filter(df["public_date"] >= options["start_date"])
            if "end_date" in options and options["end_date"]:
                df = df.filter(df["public_date"] <= options["end_date"])

    else:
        raise ValueError("Either file_path must be provided or use_vnstock=True")

    df = handler.add_metadata_cols(df, ingest_year=True, ingest_month=True)
    handler.load_to_bronze(df, table_name=bronze_table, partition_cols=["ingest_year", "ingest_month"])
    handler.stop()


if __name__ == "__main__":
    DATA_DIR = "/opt/spark/data"
    START_DATE = "2022-01-01"
    END_DATE = None
    START_YEAR = START_DATE.split("-")[0]

    load_fact(
        app_name="LoadCompanyEventsToBronze",
        bronze_table="iceberg.bronze.company_events",
        fact_type="company_events",
        file_path=os.path.join(DATA_DIR, "company_events.csv"),
        use_vnstock=False,
        options={"start_date": START_DATE, "end_date": END_DATE}
    )

    load_fact(
        app_name="LoadQuarterlyRatioToBronze",
        bronze_table="iceberg.bronze.quarterly_ratio",
        fact_type="quarterly_ratio",
        file_path=os.path.join(DATA_DIR, "quarterly_ratio.csv"),
        use_vnstock=False,
        options={"start_year": START_YEAR, "end_year": None})

    load_fact(
        app_name="LoadDailyOhlcvToBronze",
        bronze_table="iceberg.bronze.daily_ohlcv",
        fact_type="daily_ohlcv",
        file_path=os.path.join(DATA_DIR, "daily_ohlcv.csv"),
        use_vnstock=False,
        options={"start_date": START_DATE, "end_date": END_DATE})

