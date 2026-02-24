import polars as pl
import structlog
import math
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()


class SpaceXTransformer:
    """Transforma dados brutos da API SpaceX em formato Silver pronto para persistência."""
    def __init__(self):
        pass

    def transform_launches(self, raw_data):
        return self.transform("launches", raw_data)

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

            # -------------------------
            # Rename
            # -------------------------
            if getattr(schema, "rename", None):
                rename_map = {
                    k: v for k, v in schema.rename.items()
                    if k in df.columns
                }
                if rename_map:
                    df = df.rename(rename_map)

            # -------------------------
            # Date parsing (ISO 8601 UTC)
            # -------------------------
            date_col = None
            if "date_utc" in df.columns:
                date_col = "date_utc"
            elif "updated_at" in df.columns:
                date_col = "updated_at"

            if date_col:
                dtype = df.schema.get(date_col)

                if dtype == pl.Utf8:
                    df = df.with_columns(
                        pl.col(date_col)
                        .str.to_datetime(
                            format="%Y-%m-%dT%H:%M:%S%.fZ",
                            time_zone="UTC",
                            strict=False,
                        )
                        .cast(pl.Datetime("us", time_zone="UTC"))
                    )

            # -------------------------
            # Incremental filter
            # -------------------------
            if last_ingested and date_col and date_col in df.columns:
                if last_ingested.tzinfo is None:
                    last_ingested = last_ingested.replace(
                        tzinfo=timezone.utc
                    )

                df = df.filter(pl.col(date_col) > last_ingested)

            # -------------------------
            # Casts
            # -------------------------
            if getattr(schema, "casts", None):
                for col, dtype in schema.casts.items():
                    if col in df.columns:
                        df = df.with_columns(
                            pl.col(col).cast(dtype, strict=False)
                        )

            # -------------------------
            # Schema validation
            # -------------------------
            missing = [
                c for c in schema.columns
                if c not in df.columns
            ]
            if missing:
                raise ValueError(
                    f"Divergência de Schema no endpoint "
                    f"'{endpoint}'. Colunas ausentes: {missing}"
                )

            df = df.select(schema.columns)

            # -------------------------
            # Deduplicação segura
            # -------------------------
            if schema.pk in df.columns:
                df = (
                    df.unique(subset=[schema.pk])
                    .filter(pl.col(schema.pk).is_not_null())
                )

            logger.info(
                "Transformação concluída",
                endpoint=endpoint,
                rows=df.height,
            )

            return df

        except Exception:
            logger.exception(
                "Falha na transformação Polars",
                endpoint=endpoint,
            )
            raise
