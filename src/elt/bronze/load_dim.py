from _bronze_handler import BronzeHandler
import os


def load_dim(app_name: str, bronze_table: str, file_path: str, dim_type: str, use_vnstock=False):
    handler = BronzeHandler(app_name=app_name)

    if use_vnstock:
        from _vnstock_handler import VnstockHandler
        vnstock_handler = VnstockHandler()

        if dim_type == "industry":
            df = vnstock_handler.fetch_industry_df()
        elif dim_type == "company":
            df = vnstock_handler.fetch_company_df()
        elif dim_type == "company_shareholders":
            df = vnstock_handler.fetch_company_shareholders_df()
        elif dim_type == "company_officers":
            df = vnstock_handler.fetch_company_officers_df()
        else:
            raise ValueError(f"Invalid dim_type: {dim_type}.")
        
    elif file_path:
        df = handler.read_file(file_path, format_type="csv", header=True, inferSchema=False)
    else:
        raise ValueError("Either file_path must be provided or use_vnstock=True")

    df = handler.add_metadata_cols(df, ingest_year=True, ingest_month=True)
    handler.load_to_bronze(df, table_name=bronze_table, partition_cols=["ingest_year", "ingest_month"])

    handler.stop()


if __name__ == "__main__":
    DATA_DIR = "/opt/spark/data"

    load_dim(
        app_name="LoadIndustryToBronze",
        bronze_table="iceberg.bronze.industry",
        file_path=os.path.join(DATA_DIR, "industry.csv"),
        dim_type="industry",
        use_vnstock=False
    )

    load_dim(
        app_name="LoadCompanyToBronze",
        bronze_table="iceberg.bronze.company",
        file_path=os.path.join(DATA_DIR, "company.csv"),
        dim_type="company",
        use_vnstock=False
    )

    load_dim(
        app_name="LoadCompanyShareholdersToBronze",
        bronze_table="iceberg.bronze.company_shareholders",
        file_path=os.path.join(DATA_DIR, "company_shareholders.csv"),
        dim_type="company_shareholders",
        use_vnstock=False
    )
