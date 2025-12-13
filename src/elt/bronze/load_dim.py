# bronze/load_dim.py

from _bronze_handler import BronzeHandler
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

class DimLoader():
    def __init__(self, app_name: str, bronze_table: str, file_path: str = None, use_vnstock: bool = False, dim_type: str = None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.app_name = app_name
        self.bronze_table = bronze_table
        self.file_path = file_path
        self.use_vnstock = use_vnstock
        self.dim_type = dim_type

        self._validate()


    def _validate(self):
        self.logger.info("Validating arguments...")

        if self.use_vnstock:
            if not self.dim_type:
                raise ValueError("use_vnstock=True requires dim_type")
            if self.file_path:
                raise ValueError("Do not pass file_path when use_vnstock=True")
        else:
            if not self.file_path:
                raise ValueError("file_path is required when not using vnstock")
        
        self.logger.info("Validation passed...")


    def _load_from_vnstock(self):
        from _vnstock_handler import VnstockHandler
        vnstock = VnstockHandler()

        self.logger.info("Starting loading from vnstock | dim_type=%s...", self.dim_type)

        if self.dim_type == "industry":
            return vnstock.fetch_industry_df()
        if self.dim_type == "company":
            return vnstock.fetch_company_df()
        if self.dim_type == "company_shareholders":
            return vnstock.fetch_company_shareholders_df()
        if self.dim_type == "company_officers":
            return vnstock.fetch_company_officers_df()

        raise ValueError(f"Unsupported dim_type: {self.dim_type}")


    def run(self):
        handler = BronzeHandler(app_name=self.app_name)

        if self.use_vnstock:
            df = self._load_from_vnstock()
        else:
            self.logger.info("Starting loading from file | file_path=%s...", self.file_path)
            df = handler.read_file(self.file_path, format_type="csv", header=True, inferSchema=False)

        df = handler.add_metadata_cols(df, ingest_year=True, ingest_month=True)
        handler.load_to_bronze(
            df=df,
            table_name=self.bronze_table,
            partition_cols=["ingest_year", "ingest_month"],
        )
        handler.stop()

        self.logger.info("Finished loading | app_name=%s", self.app_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Load Dim to Bronze")

    parser.add_argument("--app_name", required=True)
    parser.add_argument("--bronze_table", required=True)
    parser.add_argument("--file_path")
    parser.add_argument("--use_vnstock", action="store_true")
    parser.add_argument("--dim_type")

    args = parser.parse_args()

    loader = DimLoader(
        bronze_table=args.bronze_table,
        app_name=args.app_name,
        file_path=args.file_path,
        use_vnstock=args.use_vnstock,
        dim_type=args.dim_type,
    )

    loader.run()
