import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        if not raw_data:
            logger.warning("Empty data received", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "rockets")
        if df.is_empty(): return df # Proteção essencial
        
        return df.select([
            pl.col("id").alias("rocket_id"),
            "name", "type",
            pl.col("active").cast(pl.Boolean),
            pl.col("stages").cast(pl.Int32),
            pl.col("cost_per_launch").cast(pl.Float64),
            pl.col("success_rate_pct").cast(pl.Float64)
        ])

    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "launchpads")
        if df.is_empty(): return df # Proteção essencial
        
        return df.select([
            pl.col("id").alias("launchpad_id"),
            "name", "full_name", "locality", "region", "status"
        ])

    def transform_payloads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "payloads")
        if df.is_empty(): return df # Proteção essencial
        
        return df.select([
            pl.col("id").alias("payload_id"),
            "name", "type",
            pl.col("reused").cast(pl.Boolean),
            pl.col("mass_kg").fill_null(0).cast(pl.Float64),
            "orbit"
        ])

    def transform_launches(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "launches")
        if df.is_empty(): return df # Proteção essencial
        
        return (
            df.with_columns([
                # A forma mais robusta de lidar com o ISO da SpaceX no Polars
                pl.col("date_utc").str.to_datetime(time_unit="ms").dt.replace_time_zone("UTC"),
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