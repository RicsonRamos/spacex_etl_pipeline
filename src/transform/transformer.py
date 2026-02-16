import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    """
    Class responsible for transforming raw SpaceX API data into polished Polars DataFrames.
    Ensures type safety, handles nulls, and standardizes datetime formats.
    """

    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """
        Converts raw list of dictionaries to a Polars DataFrame with basic empty check.
        """
        if not raw_data:
            logger.warning("Empty data received", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms rocket data.
        Maps 'id' to 'rocket_id' and enforces numeric types for costs and rates.
        """
        df = self._to_df(data, "rockets")
        if df.is_empty(): return df
        
        return df.select([
            pl.col("id").alias("rocket_id"),
            "name", "type",
            pl.col("active").cast(pl.Boolean),
            pl.col("stages").cast(pl.Int32),
            pl.col("cost_per_launch").cast(pl.Float64),
            pl.col("success_rate_pct").cast(pl.Float64)
        ])

    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms launchpad data.
        Maps 'id' to 'launchpad_id' for relational consistency.
        """
        df = self._to_df(data, "launchpads")
        if df.is_empty(): return df
        
        return df.select([
            pl.col("id").alias("launchpad_id"),
            "name", "full_name", "locality", "region", "status"
        ])

    def transform_payloads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms payload data.
        Handles null masses by filling with 0.0 before casting.
        """
        df = self._to_df(data, "payloads")
        if df.is_empty(): return df
        
        return df.select([
            pl.col("id").alias("payload_id"),
            "name", "type",
            pl.col("reused").cast(pl.Boolean),
            pl.col("mass_kg").fill_null(0).cast(pl.Float64),
            "orbit"
        ])

    def transform_launches(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms launch data.
        Critical: Handles ISO8601 strings with 'Z' suffix using time_zone="UTC".
        Extracts launch_year for analytical partitioning.
        """
        df = self._to_df(data, "launches")
        if df.is_empty(): return df
        
        return (
            df.with_columns([
                # Explicitly handle timezone-aware strings to avoid ComputeError
                pl.col("date_utc").str.to_datetime(time_zone="UTC"),
                pl.col("success").cast(pl.Boolean)
            ])
            .with_columns([
                pl.col("date_utc").dt.year().alias("launch_year")
            ])
            .select([
                pl.col("id").alias("launch_id"),
                "name", "date_utc", "success", "flight_number",
                pl.col("rocket").alias("rocket_id"),
                pl.col("launchpad").alias("launchpad_id"),
                "launch_year"
            ])
        )

transformer = SpaceXTransformer()