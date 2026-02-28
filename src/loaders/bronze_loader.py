# src/loaders/bronze_loader.py

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import settings
from src.loaders.base import DatabaseConnection, logger
from src.application.entity_config import EntityConfig


class BronzeLoader(DatabaseConnection):
    """
    Loader responsável pela camada Bronze (dados brutos em JSONB).

    Funcionalidades:
    - Determina tabela Bronze automaticamente via entity
    - Persiste registros brutos com timestamp UTC
    - Logging detalhado de operações
    """

    def __init__(self, table_name: Optional[str] = None, entity: Optional[str] = None):
        """
        Args:
            table_name: Nome físico da tabela (sobrescreve entity)
            entity: Nome lógico da entidade para pegar tabela via EntityConfig
        """
        super().__init__(database_url=settings.DATABASE_URL)

        if table_name:
            self.table_name = table_name
        elif entity:
            self.table_name = EntityConfig(name=entity).bronze_table
        else:
            raise ValueError("Either 'table_name' or 'entity' must be provided for BronzeLoader")

    def load(
        self,
        raw_data: List[Dict[str, Any]],
        source: str,
    ) -> int:
        """
        Persiste dados brutos na camada Bronze com timestamp UTC.

        Args:
            raw_data: Lista de registros brutos
            source: Fonte do dado

        Returns:
            int: Número de linhas inseridas
        """
        if not raw_data:
            logger.info("No data to load", table=self.table_name)
            return 0

        ingested_at = datetime.now(timezone.utc)

        rows = [
            {
                "source": source,
                "raw_data": json.dumps(record, default=str),
                "ingested_at": ingested_at,
            }
            for record in raw_data
        ]

        query = text(f"""
            INSERT INTO {self.table_name} (source, raw_data, ingested_at)
            VALUES (:source, :raw_data, :ingested_at)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, rows)

            logger.info(
                "Bronze load completed",
                table=self.table_name,
                rows=len(rows),
            )

            return len(rows)

        except SQLAlchemyError as e:
            logger.error(
                "Error while persisting data to Bronze",
                table=self.table_name,
                error=str(e),
            )
            raise