from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.loaders.base import DatabaseConnection, logger

class WatermarkManager(DatabaseConnection):
    """
    Class responsible for managing and retrieving the last ingestion date (UTC-aware) for incremental loads.
    """

    def get_last_ingested(self, table_name: str, column: str = "date_utc") -> datetime:
        """
        Get the last ingestion date (UTC-aware) for incremental loads.

        :param table_name: str, name of the table where the watermark is stored
        :param column: str, name of the column where the watermark is stored
        :return datetime, last ingestion date (UTC-aware)
        """
        query = text(f"SELECT MAX({column}) FROM {table_name}")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
                if result is None:
                    # if no watermark is found, return a datetime object representing 2000-01-01 UTC
                    return datetime(2000, 1, 1, tzinfo=timezone.utc)
                # make sure the datetime object is UTC-aware
                return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result
        except SQLAlchemyError as e:
            logger.warning(
                "Failed to retrieve watermark, using default 2000-01-01 UTC",
                table=table_name,
                error=str(e),
            )
            # if an error occurs, return a datetime object representing 2000-01-01 UTC
            return datetime(2000, 1, 1, tzinfo=timezone.utc)
