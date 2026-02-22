import json
import structlog
import polars as pl
from datetime import datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import settings
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()


class PostgresLoader:
    """Persistência nas camadas Bronze e Silver, UTC-aware e compatível com TIMESTAMPTZ."""

    def __init__(self):
        """Inicializa engine SQLAlchemy com pool otimizado."""
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            future=True,
        )

    
    # Validação de schema
    
    def validate_and_align_schema(self, entity: str):
        """Valida se colunas do banco estão de acordo com o SCHEMA_REGISTRY."""
        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade '{entity}' não encontrada no Registry.")

        query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table
            AND table_schema = 'public'
        """)
        try:
            with self.engine.connect() as conn:
                existing_cols = [row[0] for row in conn.execute(query, {"table": schema.silver_table})]

            if not existing_cols:
                logger.warning(
                    "Tabela não detectada; assumindo criação via DDL inicial",
                    table=schema.silver_table
                )
                return

            missing_in_db = [c for c in schema.columns if c not in existing_cols]
            if missing_in_db:
                logger.error(
                    "Divergência de schema detectada",
                    entity=entity,
                    missing_columns=missing_in_db
                )
                raise ValueError(f"Banco desatualizado. Colunas ausentes: {missing_in_db}")

        except SQLAlchemyError as e:
            logger.error("Falha ao inspecionar banco", error=str(e))
            raise

    
    # Marca d'água (incremental)
    
    def get_last_ingested(self, table_name: str, column: str = "date_utc") -> datetime:
        """Retorna último timestamp ingerido (UTC-aware) para cargas incrementais."""
        query = text(f"SELECT MAX({column}) FROM {table_name}")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
                if result is None:
                    return datetime(2000, 1, 1, tzinfo=timezone.utc)

                # Garante que é UTC-aware
                if result.tzinfo is None:
                    return result.replace(tzinfo=timezone.utc)
                return result
        except Exception as e:
            logger.warning(
                "Falha ao buscar marca d'água, usando padrão 2000-01-01 UTC",
                table=table_name,
                error=str(e)
            )
            return datetime(2000, 1, 1, tzinfo=timezone.utc)

    
    # Bronze Loader
    
    def load_bronze(self, data: List[Dict[str, Any]], entity: str, source: str) -> int:
        """Persiste dados brutos em JSONB, timestamp UTC."""
        if not data:
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        ingested_at = datetime.now(timezone.utc)

        rows = [
            {
                "source": source,
                "raw_data": json.dumps(row, default=str),
                "ingested_at": ingested_at
            }
            for row in data
        ]

        query = text(f"""
            INSERT INTO {schema.bronze_table} (source, raw_data, ingested_at)
            VALUES (:source, :raw_data, :ingested_at)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, rows)
            return len(rows)
        except SQLAlchemyError as e:
            logger.error("Falha na carga Bronze", entity=entity, error=str(e))
            raise

    
    # Silver Loader (Upsert)
    
    def upsert_silver(self, df: pl.DataFrame, entity: str) -> int:
        """Upsert para tabela Silver usando PK do schema."""
        if df.is_empty():
            return 0

        schema = SCHEMA_REGISTRY.get(entity)

        # Converte colunas complexas para string (List, Struct, Object)
        complex_types = [pl.List, pl.Struct, pl.Object]
        cols_to_cast = [c for c in df.columns if any(isinstance(df[c].dtype, t) for t in complex_types)]
        if cols_to_cast:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cols_to_cast])

        # Alinhamento com Registry: apenas colunas definidas
        target_cols = [c for c in schema.columns if c in df.columns]
        records = df.select(target_cols).to_dicts()

        insert_cols = ", ".join(target_cols)
        insert_params = ", ".join([f":{c}" for c in target_cols])

        # Define ação de conflito
        update_cols = [c for c in target_cols if c != schema.pk]
        conflict_action = "DO NOTHING" if not update_cols else \
            "DO UPDATE SET " + ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        query = text(f"""
            INSERT INTO {schema.silver_table} ({insert_cols})
            VALUES ({insert_params})
            ON CONFLICT ({schema.pk}) {conflict_action}
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)
            logger.info("Carga Silver concluída", entity=entity, rows=len(records))
            return len(records)
        except SQLAlchemyError as e:
            logger.error("Falha no Upsert Silver", entity=entity, error=str(e))
            raise