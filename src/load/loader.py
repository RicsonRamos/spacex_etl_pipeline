import json
import structlog
import polars as pl
from datetime import datetime, timezone
from typing import Optional, Any, List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()

class PostgresLoader:
    def __init__(self, connection_string: Optional[str] = None, engine: Optional[Engine] = None):
        if engine:
            self.engine = engine
        elif connection_string:
            self.engine = create_engine(connection_string, pool_pre_ping=True)
        else:
            raise ValueError("Provide 'connection_string' or 'engine'.")

    def _get_sql_type(self, dtype: pl.DataType) -> str:
        if dtype == pl.Int64: return "BIGINT"
        if dtype == pl.Float64: return "DOUBLE PRECISION"
        if dtype == pl.Boolean: return "BOOLEAN"
        if isinstance(dtype, pl.Datetime): return "TIMESTAMPTZ"
        return "TEXT"

    def _migrate_schema(self, df: pl.DataFrame, table_name: str):
        """Garante que a tabela exista e tenha as colunas necessárias."""
        with self.engine.begin() as conn:
            # Criação inicial mínima
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} (ingested_at TIMESTAMPTZ DEFAULT NOW())"))
            
            res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name.lower()}'"))
            existing_cols = {row[0] for row in res}

            for col in df.columns:
                if col.lower() not in existing_cols:
                    sql_type = self._get_sql_type(df.schema[col])
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} {sql_type}"))

    def load_bronze(self, df: pl.DataFrame, table: str, source: str = "spacex_api") -> int:
        """Carrega dados adicionando metadados de auditoria para evitar erros de NOT NULL."""
        if df.is_empty():
            return 0

        # Adiciona colunas de auditoria que seu banco antigo exige
        df = df.with_columns([
            pl.lit(source).alias("source"),
            pl.lit(datetime.now(timezone.utc)).alias("ingested_at")
        ])

        self._migrate_schema(df, table)
        
        # Converter colunas complexas (List/Struct) para JSON string para o Postgres aceitar em colunas TEXT
        complex_cols = [col for col, dtype in df.schema.items() if isinstance(dtype, (pl.List, pl.Struct, pl.Object))]
        if complex_cols:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in complex_cols])

        records = df.to_dicts()
        cols = df.columns
        query = text(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join([f':{c}' for c in cols])})")

        with self.engine.begin() as conn:
            conn.execute(query, records)
        return len(records)

    def load_silver(self, df: pl.DataFrame, entity: str) -> int:
        """Upsert rigoroso na camada Silver."""
        schema_cfg = SCHEMA_REGISTRY.get(entity)
        if not schema_cfg: raise ValueError(f"Entity {entity} missing in registry.")
        
        table = schema_cfg.silver_table
        df_silver = df.select([c for c in schema_cfg.columns if c in df.columns])
        
        self._migrate_schema(df_silver, table)
        records = df_silver.to_dicts()
        pk = schema_cfg.pk
        cols = df_silver.columns
        update_cols = [c for c in cols if c != pk]
        
        query = text(f"""
            INSERT INTO {table} ({", ".join(cols)}) VALUES ({", ".join([f":{c}" for c in cols])})
            ON CONFLICT ({pk}) DO UPDATE SET {", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])}
        """)

        with self.engine.begin() as conn:
            conn.execute(query, records)
        return len(records)