from typing import List
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.config.settings import settings
from src.loaders.base import DatabaseConnection, logger

class SchemaValidator(DatabaseConnection):
    def __init__(self):
        super().__init__(database_url=settings.DATABASE_URL)
    """
    Valida se a tabela existe e contém todas as colunas esperadas.
    """

    def validate_table_columns(self, table_name: str, expected_columns: List[str]) -> None:
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
                logger.warning("Table not detected; it will be created via DDL", table=table_name)
                return

            missing_columns = [col for col in expected_columns if col not in existing_cols]
            if missing_columns:
                logger.error("Schema divergence detected", table=table_name, missing_columns=missing_columns)
                raise ValueError(f"Table '{table_name}' is out of sync. Missing columns: {missing_columns}")

            logger.info("Schema validation completed", table=table_name, columns=existing_cols)

        except SQLAlchemyError as e:
            logger.error("Failed to inspect table", table=table_name, error=str(e))
            raise