import polars as pl
import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()

class SpaceXTransformer:
    def transform(
        self,
        endpoint: str,
        data: List[Dict[str, Any]],
        last_ingested: Optional[datetime] = None
    ) -> pl.DataFrame:
        if not data:
            return pl.DataFrame()

        schema = SCHEMA_REGISTRY.get(endpoint)
        if not schema:
            raise ValueError(f"Endpoint '{endpoint}' não mapeado.")

        try:
            # 1. Criação otimizada do DataFrame
            df = pl.from_dicts(data)

            # 2. FILTRO INCREMENTAL (Vetorizado no Polars)
            # Aplicamos o filtro ANTES das transformações pesadas
            if last_ingested and "date_utc" in df.columns:
                df = df.with_columns(pl.col("date_utc").str.to_datetime())
                df = df.filter(pl.col("date_utc") > last_ingested)
                if df.is_empty():
                    logger.info("Nenhum registro novo após filtro incremental", endpoint=endpoint)
                    return df

            # 3. RENAME
            if schema.rename:
                rename_map = {k: v for k, v in schema.rename.items() if k in df.columns}
                df = df.rename(rename_map)

            # 4. CASTS & SELEÇÃO
            for col, dtype in schema.casts.items():
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(dtype, strict=False))

            # Seleciona apenas as colunas do contrato que de fato existem
            df = df.select([c for c in schema.columns if c in df.columns])

            # 5. DEDUPLICAÇÃO & QUALIDADE
            df = df.unique(subset=[schema.pk])
            
            # Remove linhas onde a PK é nula (Garante integridade do Upsert)
            df = df.filter(pl.col(schema.pk).is_not_null())

            logger.info("Transformação concluída", endpoint=endpoint, rows=df.height)
            return df

        except Exception as e:
            logger.exception("Falha na transformação Polars", endpoint=endpoint)
            raise
