# src/loaders/silver_loader.py

from typing import List, Optional
import polars as pl
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import settings
from src.loaders.base import DatabaseConnection, logger
from src.application.entity_schema import SCHEMAS
from src.application.entity_config import EntityConfig

# Chave primária padrão por entidade
PKS = {
    "rockets": "rocket_id",
    "launches": "launch_id",
}


class SilverLoader(DatabaseConnection):
    """
    Loader da camada Silver que aplica schema dinamicamente
    com base na entidade.

    Funcionalidades:
    - Valida schema automaticamente com SCHEMAS
    - Converte tipos complexos (List, Struct, Object) para string
    - Upsert com chave primária configurável
    """

    def __init__(
        self,
        entity: Optional[str] = None,
        table_name: Optional[str] = None,
        primary_key: Optional[str] = None,
    ):
        super().__init__(database_url=settings.DATABASE_URL)

        self.entity = entity
        self.table_name = table_name or (EntityConfig(name=entity).silver_table if entity else None)

        if entity:
            if entity not in SCHEMAS:
                raise ValueError(f"Entity '{entity}' has no registered schema")
            self.expected_schema: List[str] = SCHEMAS[entity]
        else:
            self.expected_schema: List[str] = []

        self.primary_key = primary_key or PKS.get(entity)
        if not self.primary_key:
            raise ValueError("Primary key must be defined either via constructor or PKS mapping")

    def upsert(self, df: pl.DataFrame) -> int:
        """
        Realiza upsert na camada Silver, garantindo compatibilidade com SCHEMAS.

        - Adiciona colunas ausentes com valor default
        - Converte tipos complexos para string
        """
        if not self.table_name:
            raise ValueError("Silver table_name must be provided in constructor")

        if df.is_empty():
            logger.warning("Empty DataFrame, nothing to upsert", table=self.table_name)
            return 0

        # Adiciona colunas ausentes com valor default
        for col in self.expected_schema:
            if col not in df.columns:
                dtype = pl.Utf8
                default_value = None

                # Define default para alguns tipos conhecidos
                if col in ["success_rate_pct", "cost_per_launch", "stages", "boosters", "flight_number"]:
                    dtype = pl.Float64
                    default_value = 0
                elif col in ["active", "upcoming", "auto_update", "tbd"]:
                    dtype = pl.Boolean
                    default_value = False

                df = df.with_columns(pl.lit(default_value).cast(dtype).alias(col))

        # Converter tipos complexos para string
        complex_types = [pl.List, pl.Struct, pl.Object]
        cols_to_cast = [
            c for c in df.columns
            if any(isinstance(df[c].dtype, t) for t in complex_types)
        ]
        if cols_to_cast:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cols_to_cast])

        # Preparar INSERT com ON CONFLICT
        insert_cols = ", ".join(df.columns)
        insert_params = ", ".join([f":{c}" for c in df.columns])
        update_cols = [c for c in df.columns if c != self.primary_key]

        conflict_action = (
            "DO NOTHING"
            if not update_cols
            else "DO UPDATE SET " + ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        )

        query = text(f"""
            INSERT INTO {self.table_name} ({insert_cols})
            VALUES ({insert_params})
            ON CONFLICT ({self.primary_key}) {conflict_action}
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, df.to_dicts())

            logger.info("Silver load completed", table=self.table_name, rows=len(df))
            return len(df)

        except SQLAlchemyError as e:
            logger.error("Error during Silver load", table=self.table_name, error=str(e))
            raise