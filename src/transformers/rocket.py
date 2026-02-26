from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
import structlog

from src.transformers.base import BaseTransformer

logger = structlog.get_logger()


class RocketTransformer(BaseTransformer):
    schema = {
        "name": "rockets",
        "columns": ["rocket_id", "name", "active", "cost_per_launch", "success_rate_pct"],
        "pk": "rocket_id",
        "casts": {"rocket_id": pl.Utf8},
    }

    def transform(
        self,
        data: List[Dict[str, Any]],
        last_ingested: Optional[datetime] = None,
    ) -> pl.DataFrame:
        if not data:
            return pl.DataFrame()

        df = self._build_df(data)
        df = self._rename_columns(df)
        df = self._apply_incremental_filter(df, last_ingested)
        df = self._apply_casts(df)
        self._validate_schema(df)
        df = self._select_columns(df)
        df = self._deduplicate(df)

        logger.info("Transformed data successfully", endpoint=self.schema["name"], rows=df.height)
        return df
