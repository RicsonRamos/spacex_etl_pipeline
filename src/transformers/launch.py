from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
import structlog

from src.transformers.base import BaseTransformer
from src.application.entity_schema import SCHEMAS

logger = structlog.get_logger()


class LaunchTransformer(BaseTransformer):
    ENTITY_NAME = "launches"

    schema = {
        "name": ENTITY_NAME,
        "columns": SCHEMAS[ENTITY_NAME],
        "pk": "launch_id",
        "casts": {"rocket_id": pl.Utf8},
        "rename": {},  # API já usa date_utc
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
        df = self._apply_casts(df)
        df = self._apply_incremental_filter(df, last_ingested)
        df = self._deduplicate(df)
        self._validate_schema(df)
        df = self._select_columns(df)

        logger.info(
            "Transformed data successfully",
            entity=self.ENTITY_NAME,
            rows=df.height
        )
        return df

    def _build_df(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Extrai apenas os campos que entram no Silver.
        """
        records = []
        for raw in data:
            records.append({
                "launch_id": raw.get("id"),
                "rocket_id": raw.get("rocket"),
                "name": raw.get("name"),
                "date_utc": raw.get("date_utc"),
                "success": raw.get("success"),
                "details": raw.get("details"),
            })
        return pl.DataFrame(records)

    def _apply_casts(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Casts definidos no schema
        """
        casts = self.schema.get("casts", {})
        for col, dtype in casts.items():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(dtype))
        # Cast date_utc para datetime
        if "date_utc" in df.columns:
            df = df.with_columns(pl.col("date_utc").str.strptime(pl.Datetime, fmt="%Y-%m-%dT%H:%M:%S.%fZ"))
        return df

    def _apply_incremental_filter(self, df: pl.DataFrame, last_ingested: Optional[datetime]) -> pl.DataFrame:
        if last_ingested and "date_utc" in df.columns:
            df = df.filter(pl.col("date_utc") > last_ingested)
        return df

    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unique(subset=[self.schema["pk"]], keep="last")

    def _select_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self.schema["columns"])