# src/transformers/rocket.py
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
import structlog

from src.transformers.base import BaseTransformer
from src.application.entity_schema import SCHEMAS

logger = structlog.get_logger()


class RocketTransformer(BaseTransformer):
    """
    Transformer para a entidade 'rockets' usando schema centralizado.
    Todas as colunas do SCHEMAS['rockets'] serão usadas automaticamente.
    """

    ENTITY_NAME = "rockets"

    schema = {
        "name": ENTITY_NAME,
        "columns": SCHEMAS[ENTITY_NAME],  # todas as colunas do schema central
        "pk": "rocket_id",
        "casts": {
            "rocket_id": pl.Utf8,
            "name": pl.Utf8,
            "type": pl.Utf8,
            "active": pl.Boolean,
            "stages": pl.Int32,
            "boosters": pl.Int32,
            "cost_per_launch": pl.Int64,
            "success_rate_pct": pl.Int32,
            "first_flight": pl.Datetime,
            "country": pl.Utf8,
            "company": pl.Utf8,
            "description": pl.Utf8,
            "wikipedia": pl.Utf8,
        },
        "rename": {},  # caso precise renomear campos da API
    }

    def transform(
        self,
        data: List[Dict[str, Any]],
        last_ingested: Optional[datetime] = None,
    ) -> pl.DataFrame:
        if not data:
            logger.warning("No data received to transform", entity=self.ENTITY_NAME)
            return pl.DataFrame()

        df = self._build_df(data)
        df = self._rename_columns(df)
        df = self._normalize_dates(df)
        df = self._apply_incremental_filter(df, last_ingested)
        df = self._apply_casts(df)
        self._validate_schema(df)
        df = self._select_columns(df)
        df = self._deduplicate(df)

        logger.info(
            "Transformed data successfully",
            endpoint=self.ENTITY_NAME,
            rows=df.height
        )
        return df