import trino
import pandas as pd
import logging
from typing import Optional, List, Union

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class TrinoUtils:
    def __init__(
        self, 
        host: str = "trino", 
        port: int = 8080, 
        user: str = "TrinoPython", 
        catalog: str = "iceberg", 
        schema: str = "gold"
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )

    def read_table(self, table: str, columns: Optional[List[str]] = None, 
                   filters: Optional[List[str]] = None, 
                   order_by: Optional[str] = None,
                   limit: Optional[int] = None,
                   offset: Optional[int] = None) -> pd.DataFrame:

        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        if filters:
            query += " WHERE " + " AND ".join(filters)

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit is not None:
            query += f" LIMIT {limit}"

        if offset is not None:
            query += f" OFFSET {offset}"

        self.logger.info("Executing query on table %s", table)
        self.logger.debug("Query: %s", query)

        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=[c[0] for c in cur.description])
        cur.close()

        self.logger.info("Loaded dataframe | shape=%s", df.shape)
        return df
