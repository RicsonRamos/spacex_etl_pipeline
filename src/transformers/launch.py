from typing import List, Dict, Optional, Any
from datetime import datetime
import polars as pl
from src.transformers.base import BaseTransformer
import structlog

logger = structlog.get_logger()

class LaunchTransformer(BaseTransformer):
    schema = {
        "name": "launches",
        "columns": ["launch_id", "name", "date_utc", "rocket_id"],
        "pk": "launch_id",
        "casts": {"rocket_id": pl.Utf8},
        "rename": {"date": "date_utc"},
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
        df = self._normalize_dates(df)
        df = self._apply_incremental_filter(df, last_ingested)
        df = self._apply_casts(df)
        self._validate_schema(df)
        df = self._select_columns(df)
        df = self._deduplicate(df)

        logger.info("Transformed data successfully", endpoint=self.schema["name"], rows=df.height)
        return df