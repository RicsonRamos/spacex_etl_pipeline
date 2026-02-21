import polars as pl
import structlog
from typing import List, Dict, Any

from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()


class SpaceXTransformer:

    def transform(
        self,
        endpoint: str,
        data: List[Dict[str, Any]]
    ) -> pl.DataFrame:

        if not data:
            logger.warning("Dataset vazio", endpoint=endpoint)
            return pl.DataFrame()

        if endpoint not in SCHEMA_REGISTRY:
            raise ValueError(f"Endpoint sem schema: {endpoint}")

        schema = SCHEMA_REGISTRY[endpoint]

        try:
            df = pl.from_dicts(data)
        except Exception as e:
            logger.exception("Falha ao criar DataFrame", error=str(e))
            raise

        # =====================================================
        # RENAME
        # =====================================================
        if schema.rename:
            df = df.rename(schema.rename)

        # =====================================================
        # CAST
        # =====================================================
        for col, dtype in schema.casts.items():

            if col not in df.columns:
                raise ValueError(f"Coluna ausente: {col}")

            df = df.with_columns(
                pl.col(col).cast(dtype, strict=False)
            )

        # =====================================================
        # DERIVED
        # =====================================================
        for col, expr_fn in schema.derived.items():

            df = df.with_columns(
                expr_fn(df).alias(col)
            )

        # =====================================================
        # PK VALIDATION
        # =====================================================
        if schema.pk not in df.columns:
            raise ValueError(f"PK ausente: {schema.pk}")

        df = df.unique(subset=[schema.pk])

        return df