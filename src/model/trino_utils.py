import trino
import pandas as pd
import logging
from typing import Optional, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class TrinoUtils:
    def __init__(self, host: str = "trino", port: int = 8080, user: str = "TrinoPython", catalog: str = "iceberg", schema: str = "gold"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )

    def read_table(self, table: str, columns=None, date_col=None, start_date=None, end_date=None):
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        filters = []
        if date_col:
            if start_date:
                filters.append(f"{date_col} >= DATE '{start_date}'")
            if end_date:
                filters.append(f"{date_col} <= DATE '{end_date}'")
        if filters:
            query += " WHERE " + " AND ".join(filters)

        self.logger.info("Reading table %s using Trino", table)
        self.logger.debug("Query: %s", query)

        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=[c[0] for c in cur.description])
        cur.close()

        self.logger.info("Loaded dataframe | shape=%s", df.shape)
        return df
