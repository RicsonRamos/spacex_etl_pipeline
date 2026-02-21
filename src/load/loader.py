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
        """
        Inicializa a engine do SQLAlchemy. 
        O uso de future=True garante compatibilidade com SQLAlchemy 2.0.
        """
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            future=True,
        )

    def get_last_ingested(self, table_name: str, column: str = "date_utc") -> datetime:
        """
        Busca o valor máximo de uma coluna de timestamp para controle incremental.
        PROTEÇÃO: Usa f-string apenas para o nome da tabela (que é controlado internamente),
        mas o valor de retorno é tratado pelo driver.
        """
        query = text(f"SELECT MAX({column}) FROM {table_name}")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
                if result:
                    # Garante que o retorno seja timezone-aware se for string/datetime ingênuo
                    return result if hasattr(result, "tzinfo") and result.tzinfo else result.replace(tzinfo=timezone.utc)
                return datetime(2000, 1, 1, tzinfo=timezone.utc)
        except Exception as e:
            logger.warning("Falha ao buscar last_ingested, iniciando do padrão", table=table_name, error=str(e))
            return datetime(2000, 1, 1, tzinfo=timezone.utc)

    def load_bronze(self, data: List[Dict[str, Any]], entity: str, source: str) -> int:
        """
        Carga na camada Bronze (Raw Data). 
        Armazena o JSON bruto para auditoria e re-processamento.
        """
        if not data:
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade '{entity}' não registrada no SCHEMA_REGISTRY")

        # Preparação dos dados para inserção em lote
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
            logger.info("Carga Bronze concluída", entity=entity, count=len(rows))
            return len(rows)
        except SQLAlchemyError as e:
            logger.error("Falha na carga Bronze", entity=entity, error=str(e))
            raise

    def upsert_silver(self, df: pl.DataFrame, entity: str) -> int:
        """
        Realiza o Upsert (Insert or Update) na camada Silver.
        Resolve o problema de colunas complexas (List/Struct) serializando para JSON.
        """
        if df.is_empty():
            logger.warning("DataFrame vazio, pulando Silver", entity=entity)
            return 0

        schema = SCHEMA_REGISTRY.get(entity)
        if not schema:
            raise ValueError(f"Entidade '{entity}' não registrada.")

        # 1. TRATAMENTO DE TIPOS COMPLEXOS (Serialização para JSON strings)
        # O Postgres não aceita listas nativas do Polars via driver comum sem cast.
        complex_cols = [
            c for c in df.columns 
            if df[c].dtype in (pl.List, pl.Struct, pl.Object)
        ]

        if complex_cols:
            df = df.with_columns([
                pl.col(c).map_elements(
                    lambda x: json.dumps(x, default=str) if x is not None else None, 
                    return_dtype=pl.Utf8
                )
                for c in complex_cols
            ])

        # 2. SELEÇÃO DE COLUNAS E VALIDAÇÃO DE PK
        # Garante que não enviaremos colunas extras que não existem na tabela Silver
        target_cols = [c for c in schema.columns if c in df.columns]
        if schema.pk not in target_cols:
            raise ValueError(f"Chave Primária '{schema.pk}' ausente no DataFrame após transformação.")

        df_to_load = df.select(target_cols)
        records = df_to_load.to_dicts()

        # 3. CONSTRUÇÃO DO SQL DINÂMICO (UPSERT)
        insert_cols = ", ".join(target_cols)
        insert_params = ", ".join([f":{c}" for c in target_cols])
        
        # Colunas que serão atualizadas se houver conflito (todas menos a PK)
        update_cols = [c for c in target_cols if c != schema.pk]
        
        if update_cols:
            set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            conflict_action = f"DO UPDATE SET {set_clause}"
        else:
            conflict_action = "DO NOTHING"

        query = text(f"""
            INSERT INTO {schema.silver_table} ({insert_cols})
            VALUES ({insert_params})
            ON CONFLICT ({schema.pk})
            {conflict_action}
        """)

        # 4. EXECUÇÃO
        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)
            
            logger.info("Upsert Silver concluído", entity=entity, rows=len(records), table=schema.silver_table)
            return len(records)
        except SQLAlchemyError as e:
            logger.error("Erro no Upsert Silver", entity=entity, error=str(e))
            raise

    def refresh_gold_view(self, entity: str):
        """
        Atualiza a camada Gold. 
        Usa CREATE OR REPLACE para evitar queda de performance e lock de dependências.
        """
        schema = SCHEMA_REGISTRY.get(entity)
        if not schema or not schema.gold_view or not schema.gold_definition:
            return

        query = text(f"CREATE OR REPLACE VIEW {schema.gold_view} AS {schema.gold_definition}")

        try:
            with self.engine.begin() as conn:
                conn.execute(query)
            logger.info("Gold View atualizada", view=schema.gold_view)
        except SQLAlchemyError as e:
            logger.error("Falha ao atualizar Gold View", view=schema.gold_view, error=str(e))
            raise
