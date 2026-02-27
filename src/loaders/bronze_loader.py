from datetime import datetime, timezone
import json
from typing import Any, Dict, List
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.loaders.base import DatabaseConnection, logger

class BronzeLoader(DatabaseConnection):
    def load(self, data: List[Dict[str, Any]], table_name: str, source: str) -> int:
        """
        Persist raw data in JSONB, with a UTC timestamp.

        Args:
            data (List[Dict[str, Any]]): The raw data to be persisted.
            table_name (str): The name of the table where the data will be persisted.
            source (str): The source of the data.

        Returns:
            int: The number of rows persisted.

        Raises:
            SQLAlchemyError: If there is an error while persisting the data.
        """
        if not data:
            return 0

        ingested_at = datetime.now(timezone.utc)
        rows = [
            # Create a dictionary with the source, raw data, and ingestion timestamp
            {"source": source, "raw_data": json.dumps(row, default=str), "ingested_at": ingested_at}
            for row in data
        ]

        query = text(f"""
            INSERT INTO {table_name} (source, raw_data, ingested_at)
            VALUES (:source, :raw_data, :ingested_at)
        """)
        
        try:
            with self.engine.begin() as conn:
                # Execute the query with the rows
                conn.execute(query, rows)
            return len(rows)
        except SQLAlchemyError as e:
            logger.error(f"Error while persisting data to {table_name}", error=str(e))
            raise
