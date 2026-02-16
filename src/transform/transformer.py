import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    """
    Class responsible for transforming data from the SpaceX API into a DataFrame format
    """

    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """
        Converts a list of dictionaries into a DataFrame.

        Args:
            raw_data (List[Dict[str, Any]]): List of dictionaries containing data from the SpaceX API
            endpoint (str): Name of the endpoint being processed

        Returns:
            pl.DataFrame: DataFrame containing the transformed data
        """
        if not raw_data:
            logger.warning("Empty data received", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms data from the "rockets" endpoint into a DataFrame.

        Args:
            data (List[Dict[str, Any]]): List of dictionaries containing data from the "rockets" endpoint

        Returns:
            pl.DataFrame: DataFrame containing the transformed data
        """
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
        """
        Transforms data from the "launchpads" endpoint into a DataFrame.

        Args:
            data (List[Dict[str, Any]]): List of dictionaries containing data from the "launchpads" endpoint

        Returns:
            pl.DataFrame: DataFrame containing the transformed data
        """
        df = self._to_df(data, "launchpads")
        if df.is_empty(): return df # Proteção essencial
        
        return df.select([
            pl.col("id").alias("launchpad_id"),
            "name", "full_name", "locality", "region", "status"
        ])

    def transform_payloads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms data from the "payloads" endpoint into a DataFrame.

        Args:
            data (List[Dict[str, Any]]): List of dictionaries containing data from the "payloads" endpoint

        Returns:
            pl.DataFrame: DataFrame containing the transformed data
        """
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
        """
        Transforms data from the "launches" endpoint into a DataFrame.

        Args:
            data (List[Dict[str, Any]]): List of dictionaries containing data from the "launches" endpoint

        Returns:
            pl.DataFrame: DataFrame containing the transformed data
        """
        df = self._to_df(data, "launches")
        if df.is_empty(): return df # Proteção essencial
        
        return (
            df.with_columns([
                # A forma mais robusta de lidar com o ISO da SpaceX no Polars
                pl.col("date_utc").str.to_datetime(utc=True, time_unit="ms"),
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