from typing import List
import polars as pl
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.loaders.base import DatabaseConnection, logger

class SilverLoader(DatabaseConnection):
    """
    Load data into the Silver table using upsert.

    This class provides a method to upsert data into the Silver table.
    It uses a PK to detect duplicates and update existing records.
    """

    def upsert(self, df: pl.DataFrame, table_name: str, pk: str) -> int:
        """
        Upsert data into the Silver table using a PK.

        This method takes a Polar DataFrame, a table name, and a PK column name as arguments.
        It converts complex columns to string, inserts or updates records in the table based on the PK,
        and returns the number of inserted rows.

        Args:
            df (pl.DataFrame): The data to be inserted or updated.
            table_name (str): The name of the table to insert or update data into.
            pk (str): The primary key column name.

        Returns:
            int: The number of inserted rows.

        Raises:
            SQLAlchemyError: If there is an error during the upsert operation.
        """

        if df.is_empty():
            return 0

        # Convert complex columns to string
        complex_types = [pl.List, pl.Struct, pl.Object]
        cols_to_cast = [c for c in df.columns if any(isinstance(df[c].dtype, t) for t in complex_types)]
        if cols_to_cast:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cols_to_cast])

        insert_cols = ", ".join(df.columns)
        insert_params = ", ".join([f":{c}" for c in df.columns])
        update_cols = [c for c in df.columns if c != pk]
        conflict_action = (
            "DO NOTHING" if not update_cols else "DO UPDATE SET " + ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        )

        query = text(f"""
            INSERT INTO {table_name} ({insert_cols})
            VALUES ({insert_params})
            ON CONFLICT ({pk}) {conflict_action}
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, df.to_dicts())
            logger.info("Silver load completed", table=table_name, rows=len(df))
            return len(df)
        except SQLAlchemyError as e:
            logger.error("Error during Silver load", table=table_name, error=str(e))
            raise
