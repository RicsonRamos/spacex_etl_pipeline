from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.loaders.base import DatabaseConnection, logger
from src.config.settings import settings


class WatermarkManager(DatabaseConnection):
    """
    Gerencia a última data de ingestão (UTC) para cargas incrementais.
    """

    # 🔒 Colunas permitidas para watermark
    ALLOWED_COLUMNS = {"date_utc", "ingested_at"}

    def __init__(self, table_name: str):
        super().__init__(database_url=settings.DATABASE_URL)
        self.table_name = table_name

    def get_last_ingested(self, column: str = "date_utc") -> datetime:
        # 🔒 Proteção contra coluna inválida
        if column not in self.ALLOWED_COLUMNS:
            raise ValueError(
                f"Invalid watermark column '{column}'. "
                f"Allowed: {self.ALLOWED_COLUMNS}"
            )

        query = text(f"SELECT MAX({column}) FROM {self.table_name}")

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()

                if result is None:
                    return datetime(2000, 1, 1, tzinfo=timezone.utc)

                # Garantir timezone UTC
                if result.tzinfo is None:
                    return result.replace(tzinfo=timezone.utc)

                return result

        except SQLAlchemyError as e:
            logger.warning(
                "Failed to retrieve watermark, using default 2000-01-01 UTC",
                table=self.table_name,
                error=str(e),
            )
            return datetime(2000, 1, 1, tzinfo=timezone.utc)