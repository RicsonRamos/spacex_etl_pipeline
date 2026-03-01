from abc import ABC
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import polars as pl
import structlog

logger = structlog.get_logger()

class BaseTransformer(ABC):
    """
    Template-based transformer. 
    Define o fluxo fixo: Build -> Rename -> Normalize -> Filter -> Cast -> Validate -> Deduplicate.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def transform(self, data: List[Dict[str, Any]], last_ingested: Optional[datetime] = None) -> pl.DataFrame:
        if not data:
            logger.warning("No data provided for transformation", entity=self.config.get("name"))
            return pl.DataFrame()

        # O pipeline é imutável para garantir consistência entre entidades
        return (
            pl.from_dicts(data)
            .pipe(self._rename_columns)
            .pipe(self._normalize_dates)
            .pipe(self._apply_incremental_filter, last_ingested)
            .pipe(self._apply_casts)
            .pipe(self._validate_and_select)
            .pipe(self._deduplicate)
        )

    def _rename_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        mapping = self.config.get("rename", {})
        # Filtra apenas colunas que realmente existem no DF para evitar erro do Polars
        valid_mapping = {k: v for k, v in mapping.items() if k in df.columns}
        return df.rename(valid_mapping)

    def _normalize_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        date_col = self.config.get("date_column")
        if date_col and date_col in df.columns:
            # Uso de strict=False para evitar quebra do pipeline por erro de milissegundos na API
            return df.with_columns(
                pl.col(date_col)
                .str.to_datetime(strict=False)
                .dt.replace_time_zone("UTC")
            )
        return df

    def _apply_incremental_filter(self, df: pl.DataFrame, last_ingested: Optional[datetime]) -> pl.DataFrame:
        date_col = self.config.get("date_column")
        if last_ingested and date_col and date_col in df.columns:
            ts = last_ingested.replace(tzinfo=timezone.utc) if not last_ingested.tzinfo else last_ingested
            return df.filter(pl.col(date_col) > ts)
        return df

    def _apply_casts(self, df: pl.DataFrame) -> pl.DataFrame:
        casts = self.config.get("casts", {})
        # Aplica casts apenas em colunas existentes
        return df.with_columns([
            pl.col(c).cast(t, strict=False) for c, t in casts.items() if c in df.columns
        ])

    def _validate_and_select(self, df: pl.DataFrame) -> pl.DataFrame:
        required = self.config.get("columns", [])
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.error("Schema validation failed", missing_columns=missing)
            raise ValueError(f"Silver Layer Violation: Missing columns {missing}")
        return df.select(required)

    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        pk = self.config.get("pk")
        return df.unique(subset=[pk], keep="last") if pk else df
