import json
import structlog
import polars as pl
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import settings
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()

class PostgresLoader:
    """Responsável pela persistência nas camadas Bronze e Silver com validação de contrato."""

    def __init__(self):
        """Inicializa a engine com configurações de pool otimizadas para o Docker."""
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,  # Verifica se a conexão está viva antes de usar
            pool_size=10,
            max_overflow=20,
            future=True,
        )

    def validate_and_align_schema(self, entity: str):
        """
        Audita se o banco de dados possui as colunas definidas no SCHEMA_REGISTRY.
        Aborta o pipeline em caso de divergência (Data Drift).
        """
        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade '{entity}' não encontrada no Registry.")

        inspect_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table
            AND table_schema = 'public'
        """)

        try:
            with self.engine.connect() as conn:
                existing_cols = [
                    row[0] for row in conn.execute(inspect_query, {"table": schema.silver_table})
                ]

            if not existing_cols:
                logger.warning("Tabela não detectada; o loader prosseguirá assumindo criação via DDL inicial.", 
                               table=schema.silver_table)
                return

            missing_in_db = [c for c in schema.columns if c not in existing_cols]

            if missing_in_db:
                logger.error("DIVERGÊNCIA DE SCHEMA DETECTADA", 
                             entity=entity, missing_columns=missing_in_db)
                raise ValueError(f"O banco de dados está desatualizado. Colunas ausentes: {missing_in_db}")

        except SQLAlchemyError as e:
            logger.error("Falha técnica ao inspecionar o banco", error=str(e))
            raise

    def get_last_ingested(self, table_name: str, column: str = "date_utc") -> datetime:
        """Busca a marca d'água para carga incremental."""
        query = text(f"SELECT MAX({column}) FROM {table_name}")

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
                if result:
                    # Garante que o retorno seja um datetime UTC ciente (aware)
                    if not hasattr(result, "tzinfo") or result.tzinfo is None:
                        return result.replace(tzinfo=timezone.utc)
                    return result
                return datetime(2000, 1, 1, tzinfo=timezone.utc)
        except Exception as e:
            logger.warning("Falha ao buscar marca d'água, utilizando data padrão (2000-01-01)", 
                           table=table_name, error=str(e))
            return datetime(2000, 1, 1, tzinfo=timezone.utc)

    def load_bronze(self, data: List[Dict[str, Any]], entity: str, source: str) -> int:
        """Persistência do dado bruto em JSONB para linhagem e auditoria."""
        if not data:
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        
        # Preparação do lote
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
            logger.error("Falha catastrófica na carga Bronze", entity=entity, error=str(e))
            raise

    def upsert_silver(self, df: pl.DataFrame, entity: str) -> int:
        """Executa a sincronização (Upsert) baseada na PK definida no contrato."""
        if df.is_empty():
            return 0

        schema = SCHEMA_REGISTRY.get(entity)

        # Otimização Polars: Converte colunas complexas (Listas/Objetos) para String 
        # nativamente antes de converter para dicionário Python.
        complex_dtypes = [pl.List, pl.Struct, pl.Object]
        cols_to_cast = [c for c in df.columns if any(isinstance(df[c].dtype, t) for t in complex_dtypes)]
        
        if cols_to_cast:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cols_to_cast])

        # Alinhamento de colunas: garante que o insert só use o que está no Registry
        target_cols = [c for c in schema.columns if c in df.columns]
        records = df.select(target_cols).to_dicts()

        insert_cols = ", ".join(target_cols)
        insert_params = ", ".join([f":{c}" for c in target_cols])
        
        # Colunas que serão atualizadas em caso de conflito (todas menos a PK)
        update_cols = [c for c in target_cols if c != schema.pk]
        
        if not update_cols:
            conflict_action = "DO NOTHING"
        else:
            update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            conflict_action = f"DO UPDATE SET {update_stmt}"

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
