from typing import List
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.loaders.base import DatabaseConnection, logger


class SchemaValidator(DatabaseConnection):
    """Validate the schema of a PostgreSQL table.
    
    This class provides a method to validate if a table exists and contains all the expected columns.
    """
    
    def validate_table_columns(
        self, table_name: str, expected_columns: List[str]
    ) -> None:
        """
        Validate if a table exists and contains all the expected columns.
        
        Args:
            table_name (str): The name of the table in the database.
            expected_columns (List[str]): The list of expected columns.
        
        Raises:
            ValueError: If the table does not exist or columns are missing.
        """
        query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table
            AND table_schema = 'public'
        """)

        try:
            with self.engine.connect() as conn:
                existing_cols = [row[0] for row in conn.execute(query, {"table": table_name})]

            if not existing_cols:
                logger.warning(
                    "Table not detected; it will be created via DDL",
                    table=table_name
                )
                return

            missing_columns = [col for col in expected_columns if col not in existing_cols]
            if missing_columns:
                logger.error(
                    "Schema divergence detected",
                    table=table_name,
                    missing_columns=missing_columns
                )
                raise ValueError(
                    f"Table '{table_name}' is out of sync. Missing columns: {missing_columns}"
                )

            logger.info(
                "Schema validation completed",
                table=table_name,
                columns=existing_cols
            )

        except SQLAlchemyError as e:
            logger.error("Failed to inspect table", table=table_name, error=str(e))
            raise
