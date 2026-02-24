import polars as pl
import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()


class SpaceXTransformer:
    """Transforma dados brutos da API SpaceX em formato Silver pronto para persistência."""
    def __init__(self):
        pass

    def process_in_batches(self, df: pl.DataFrame, batch_size: int):
        """Divide o DataFrame em batches e processa cada um"""
        total_batches = math.ceil(len(df) / batch_size)
        for i in range(total_batches):
            yield df[i * batch_size : (i + 1) * batch_size]

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
            raise ValueError(f"Endpoint '{endpoint}' não mapeado no Registry.")

        try:
            df = pl.from_dicts(data)

            
            # Renomeação imediata
            
            if schema.rename:
                rename_map = {k: v for k, v in schema.rename.items() if k in df.columns}
                df = df.rename(rename_map)

            
            # Conversão de datas UTC
            
            date_col = "date_utc" if "date_utc" in df.columns else "ingested_at"
            if date_col in df.columns and df.schema[date_col] == pl.String:
                # Parse ISO 8601 com timezone explícito
                df = df.with_columns(
                    pl.col(date_col)
                    .str.to_datetime(
                        format="%Y-%m-%dT%H:%M:%S%.fZ",  # ISO 8601
                        time_zone="UTC",                  # Garantia UTC
                        strict=False
                    )
                    .cast(pl.Datetime("us", time_zone="UTC"))
                    .alias(date_col)
                )

            
            # Filtro incremental
            
            if last_ingested and date_col in df.columns:
                # Garante que last_ingested seja UTC-aware
                if last_ingested.tzinfo is None:
                    last_ingested = last_ingested.replace(tzinfo=timezone.utc)
                df = df.filter(pl.col(date_col) > last_ingested)

            
            # Casts & validação de colunas
            
            for col, dtype in schema.casts.items():
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(dtype, strict=False))

            missing = [c for c in schema.columns if c not in df.columns]
            if missing:
                raise ValueError(
                    f"Divergência de Schema no endpoint '{endpoint}'. Colunas ausentes: {missing}"
                )

            df = df.select(schema.columns)

            
            # Deduplicação
            
            df = df.unique(subset=[schema.pk]).filter(pl.col(schema.pk).is_not_null())

            logger.info(
                "Transformação concluída",
                endpoint=endpoint,
                rows=df.height
            )
            return df

        except Exception as e:
            logger.exception("Falha na transformação Polars", endpoint=endpoint)
            raise
