import structlog
import polars as pl
import json
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from src.config.settings import settings
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()

class PostgresLoader:
    def __init__(self):
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            future=True,
        )

    def load_bronze(self, data: List[Dict[str, Any]], entity: str, source: str) -> int:
        """Carga bruta em formato JSON."""
        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade não registrada: {entity}")

        rows = [
            {"source": source, "raw_data": json.dumps(row, default=str)}
            for row in data
        ]

        query = text(f"INSERT INTO {schema.bronze_table} (source, raw_data) VALUES (:source, :raw_data)")

        try:
            with self.engine.begin() as conn:
                conn.execute(query, rows)
            return len(rows)
        except SQLAlchemyError as e:
            logger.error("Erro no load Bronze", entity=entity, error=str(e))
            raise

    def upsert_silver(self, df: pl.DataFrame, entity: str) -> int:
        """Upsert canônico na camada Silver."""
        if df.is_empty():
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade não registrada: {entity}")

        # Validação de integridade de colunas obrigatórias
        missing_req = [c for c in schema.required if c not in df.columns]
        if missing_req:
            raise ValueError(f"Colunas obrigatórias ausentes para {entity}: {missing_req}")

        # Tratamento de tipos complexos (Listas/Dicts) para String JSON (Postgres compativel)
        complex_cols = [c for c in df.columns if df[c].dtype in (pl.List, pl.Struct, pl.Object)]
        if complex_cols:
            df = df.with_columns([
                pl.col(c).map_elements(lambda v: json.dumps(v) if v is not None else None, return_dtype=pl.Utf8)
                for c in complex_cols
            ])

        records = df.to_dicts()
        cols = df.columns
        
        # Geração de SQL Dinâmico para UPSERT
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f":{c}" for c in cols])
        update_cols = [c for c in cols if c != schema.pk]
        
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

        # Se não houver colunas para atualizar (apenas PK), fazemos NOTHING
        conflict_action = f"DO UPDATE SET {set_clause}" if update_cols else "DO NOTHING"

        query = text(f"""
            INSERT INTO {schema.silver_table} ({insert_cols})
            VALUES ({insert_vals})
            ON CONFLICT ({schema.pk})
            {conflict_action}
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)
            logger.info("Silver upsert OK", entity=entity, table=schema.silver_table, rows=len(records))
            return len(records)
        except SQLAlchemyError as e:
            logger.error("Erro no upsert Silver", entity=entity, error=str(e))
            raise

    def refresh_gold_view(self, entity: str) -> None:
        """Atualização de visualizações na camada Gold."""
        schema = SCHEMA_REGISTRY.get(entity)
        if not schema or not schema.gold_view:
            return

        try:
            with self.engine.begin() as conn:
                # Uso de OR REPLACE para evitar quebra de dependências (evita o DROP CASCADE)
                conn.execute(text(f"CREATE OR REPLACE VIEW {schema.gold_view} AS {schema.gold_definition}"))
            logger.info("Gold view refreshed", view=schema.gold_view)
        except SQLAlchemyError as e:
            logger.error("Erro ao atualizar Gold", entity=entity, error=str(e))
            raise
