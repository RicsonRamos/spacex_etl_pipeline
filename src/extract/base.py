from abc import ABC
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import polars as pl

class BaseTransformer(ABC):
    """
    Template-based transformer ensuring consistent Silver layer processing.
    Follows: Build -> Rename -> Cast -> Filter -> Validate -> Deduplicate.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def transform(self, data: List[Dict[str, Any]], last_ingested: Optional[datetime] = None) -> pl.DataFrame:
        if not data:
            return pl.DataFrame()

        # Fluxo imutável e previsível
        df = pl.from_dicts(data)
        df = self._rename_columns(df)
        df = self._normalize_dates(df)
        df = self._apply_incremental_filter(df, last_ingested)
        df = self._apply_casts(df)
        df = self._validate_and_select(df)
        df = self._deduplicate(df)
        
        return df

    def _rename_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        mapping = self.config.get("rename", {})
        return df.rename({k: v for k, v in mapping.items() if k in df.columns})

    def _normalize_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        date_col = self.config.get("date_column", "date_utc")
        if date_col in df.columns:
            return df.with_columns(
                pl.col(date_col).str.to_datetime(strict=False).dt.replace_time_zone("UTC")
            )
        return df

    def _apply_incremental_filter(self, df: pl.DataFrame, last_ingested: Optional[datetime]) -> pl.DataFrame:
        date_col = self.config.get("date_column", "date_utc")
        if last_ingested and date_col in df.columns:
            # Garante que last_ingested seja UTC
            ts = last_ingested.replace(tzinfo=timezone.utc) if not last_ingested.tzinfo else last_ingested
            return df.filter(pl.col(date_col) > ts)
        return df

    def _apply_casts(self, df: pl.DataFrame) -> pl.DataFrame:
        casts = self.config.get("casts", {})
        return df.with_columns([pl.col(c).cast(t) for c, t in casts.items() if c in df.columns])

    def _validate_and_select(self, df: pl.DataFrame) -> pl.DataFrame:
        required = self.config.get("columns", [])
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Silver Layer Violation: Missing columns {missing}")
        return df.select(required)

    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        pk = self.config.get("pk")
        return df.unique(subset=[pk], keep="last") if pk else df
