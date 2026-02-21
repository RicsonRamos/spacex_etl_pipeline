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
    def __init__(self):
        """Inicializa a engine do SQLAlchemy 2.0."""
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            future=True,
        )

    def validate_and_align_schema(self, entity: str):
        """
        Verifica a integridade entre o SCHEMA_REGISTRY e o Banco de Dados.
        Deve ser chamado ANTES do processamento da task.
        """
        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            logger.error("Entidade não mapeada para validação", entity=entity)
            return

        # Busca colunas reais do banco de dados (Information Schema)
        # Filtramos por table_name para evitar colisões entre schemas
        inspect_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table
        """)

        try:
            with self.engine.connect() as conn:
                existing_cols = [
                    row[0] for row in conn.execute(inspect_query, {"table": schema.silver_table})
                ]

            if not existing_cols:
                logger.warning("Tabela não encontrada no banco. O Loader tentará criá-la via DDL ou falhará no insert.", table=schema.silver_table)
                return

            # Detecta colunas que o código espera mas o banco não tem
            missing_in_db = [c for c in schema.columns if c not in existing_cols]
            
            if missing_in_db:
                logger.error(
                    "DIVERGÊNCIA DE SCHEMA DETECTADA", 
                    entity=entity, 
                    table=schema.silver_table,
                    missing_columns=missing_in_db
                )
                # Lançar erro impede que o pipeline tente um insert que vai falhar
                raise ValueError(f"O Banco de Dados está desatualizado para a entidade {entity}. Colunas faltando: {missing_in_db}")
                
        except SQLAlchemyError as e:
            logger.error("Falha técnica ao validar schema no banco", error=str(e))
            raise

    def get_last_ingested(self, table_name: str, column: str = "date_utc") -> datetime:
        """Busca o valor máximo de uma coluna para carga incremental."""
        query = text(f"SELECT MAX({column}) FROM {table_name}")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
                if result:
                    return result if hasattr(result, "tzinfo") and result.tzinfo else result.replace(tzinfo=timezone.utc)
                return datetime(2000, 1, 1, tzinfo=timezone.utc)
        except Exception as e:
            logger.warning("Falha ao buscar last_ingested, iniciando do padrão", table=table_name, error=str(e))
            return datetime(2000, 1, 1, tzinfo=timezone.utc)

    def load_bronze(self, data: List[Dict[str, Any]], entity: str, source: str) -> int:
        """Carga direta de JSON bruto na camada Bronze."""
        if not data:
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade '{entity}' não registrada no SCHEMA_REGISTRY")

        rows = [
            {
                "source": source,
                "raw_data": json.dumps(row, default=str),
                "ingested_at": datetime.now(timezone.utc)
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

    def upsert_silver(self, df: pl.DataFrame, entity: str) -> int:
        """Executa Upsert dinâmico baseado no contrato do SCHEMA_REGISTRY."""
        if df.is_empty():
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        
        # Tratamento de tipos complexos para JSON
        complex_cols = [c for c in df.columns if df[c].dtype in (pl.List, pl.Struct, pl.Object)]
        if complex_cols:
            df = df.with_columns([
                pl.col(c).map_elements(lambda x: json.dumps(x, default=str) if x is not None else None, return_dtype=pl.Utf8)
                for c in complex_cols
            ])

        target_cols = [c for c in schema.columns if c in df.columns]
        records = df.select(target_cols).to_dicts()

        insert_cols = ", ".join(target_cols)
        insert_params = ", ".join([f":{c}" for c in target_cols])
        update_cols = [c for c in target_cols if c != schema.pk]
        
        conflict_action = f"DO UPDATE SET {', '.join([f'{c}=EXCLUDED.{c}' for c in update_cols])}" if update_cols else "DO NOTHING"

        query = text(f"INSERT INTO {schema.silver_table} ({insert_cols}) VALUES ({insert_params}) ON CONFLICT ({schema.pk}) {conflict_action}")

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)
            return len(records)
        except SQLAlchemyError as e:
            logger.error("Erro no Upsert Silver", entity=entity, error=str(e))
            raise
