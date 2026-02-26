from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import polars as pl


# -------------------- Base Transformer -----------------------
class BaseTransformer(ABC):
    """
    Abstract base class for data transformers.

    Each transformer must define its schema and implement the transform method.
    """

    schema: Any  # cada transformer define seu schema

    @abstractmethod
    def transform(
        self,
        data: List[Dict[str, Any]],
        last_ingested: Optional[datetime] = None,
    ) -> pl.DataFrame:
        """
        Transform data from the input format to a Polar DataFrame.

        Args:
            data (List[Dict[str, Any]]): The input data to be transformed.
            last_ingested (Optional[datetime], optional): The last ingestion date. Defaults to None.

        Returns:
            pl.DataFrame: The transformed data.
        """

    def _build_df(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Build a Polar DataFrame from a list of dictionaries.

        Args:
            data (List[Dict[str, Any]]): The input data to be transformed.

        Returns:
            pl.DataFrame: The transformed data.
        """
        return pl.from_dicts(data)

    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Deduplicate the transformed data by removing duplicate records based on the primary key.

        Args:
            df (pl.DataFrame): The transformed data.

        Returns:
            pl.DataFrame: The deduplicated data.
        """
        # Deduplicate the data by removing duplicate records based on the primary key
        # The filter is used to remove any records where the primary key is null
        return df.unique(subset=[self.schema["pk"]]).filter(pl.col(self.schema["pk"]).is_not_null())

    def _validate_schema(self, df: pl.DataFrame) -> None:
        """
        Validate if the transformed data has all the required columns.

        Args:
            df (pl.DataFrame): The transformed data.

        Raises:
            ValueError: If any required columns are missing.
        """
        # Get the list of missing columns
        missing = [c for c in self.schema["columns"] if c not in df.columns]

        # Raise an error if any columns are missing
        if missing:
            raise ValueError(f"Missing columns in transformed data: {missing}")

    def _rename_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Rename columns in the transformed data based on the schema.

        Args:
            df (pl.DataFrame): The transformed data.

        Returns:
            pl.DataFrame: The transformed data with renamed columns.
        """
        if self.schema.get("rename"):
            # Get the rename map from the schema
            rename_map = {k: v for k, v in self.schema["rename"].items() if k in df.columns}
            # Rename the columns
            df = df.rename(rename_map)
        return df

    def _normalize_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Normalize the date columns in the transformed data by
        converting them to UTC and datetime type.

        Args:
            df (pl.DataFrame): The transformed data.

        Returns:
            pl.DataFrame: The transformed data with normalized date columns.
        """
        date_col = "date_utc" if "date_utc" in df.columns else "ingested_at"
        if date_col in df.columns and df.schema[date_col] == pl.String:
            # Convert the date column to UTC and datetime type
            df = df.with_columns(
                pl.col(date_col)
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%S.%fZ", time_zone="UTC", strict=False)
                .cast(pl.Datetime("us", time_zone="UTC"))
                .alias(date_col)
            )
        return df

    def _apply_incremental_filter(
        self, df: pl.DataFrame, last_ingested: Optional[datetime]
    ) -> pl.DataFrame:
        """
        Apply an incremental filter to the transformed data based on the last ingestion date.

        Args:
            df (pl.DataFrame): The transformed data.
            last_ingested (Optional[datetime]): The last ingestion date.

        Returns:
            pl.DataFrame: The transformed data with the incremental filter applied.
        """
        date_col = "date_utc" if "date_utc" in df.columns else "ingested_at"
        if last_ingested and date_col in df.columns:
            # If the last ingestion date is naive, make it UTC-aware
            if last_ingested.tzinfo is None:
                last_ingested = last_ingested.replace(tzinfo=timezone.utc)
            # Filter the data to only include records after the last ingestion date
            df = df.filter(pl.col(date_col) > last_ingested)
        return df

    def _apply_casts(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply casts to the transformed data based on the schema.

        Args:
            df (pl.DataFrame): The transformed data.

        Returns:
            pl.DataFrame: The transformed data with the casts applied.
        """
        # Apply casts to the transformed data based on the schema
        for col, dtype in self.schema.get("casts", {}).items():
            if col in df.columns:
                # Cast the column to the specified type
                df = df.with_columns(pl.col(col).cast(dtype, strict=False))
        return df

    def _select_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Select only the columns specified in the schema.

        Args:
            df (pl.DataFrame): The transformed data.

        Returns:
            pl.DataFrame: The transformed data with only the specified columns.
        """
        # Select only the columns specified in the schema
        return df.select(self.schema["columns"])
