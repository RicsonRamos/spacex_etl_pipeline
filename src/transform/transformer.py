import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    """
    Class responsible for transforming the data extracted from the SpaceX API
    into a suitable format for loading into a PostgreSQL database.
    """
  
    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """
        Internal method to convert a list of dictionaries into a Polars DataFrame.

        Args:
            raw_data (List[Dict[str, Any]]): The data to be converted.
            endpoint (str): The name of the endpoint the data was extracted from.

        Returns:
            pl.DataFrame: The DataFrame containing the converted data.
        """
        if not raw_data:
            logger.warn("Empty data received", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Method to transform the data extracted from the "rockets" endpoint.

        Args:
            data (List[Dict[str, Any]]): The data to be transformed.

        Returns:
            pl.DataFrame: The DataFrame containing the transformed data.
        """
        df = self._to_df(data, "rockets")
        return (
            df.select([
                pl.col("id").alias("rocket_id"),
                pl.col("name"),
                pl.col("type"),
                pl.col("active").cast(pl.Int8),
                pl.col("stages"),
                pl.col("cost_per_launch").cast(pl.Float64),
                pl.col("success_rate_pct").cast(pl.Float64)
            ])
        )

    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Method to transform the data extracted from the "launchpads" endpoint.

        Args:
            data (List[Dict[str, Any]]): The data to be transformed.

        Returns:
            pl.DataFrame: The DataFrame containing the transformed data.
        """
        df = self._to_df(data, "launchpads")
        return (
            df.select([
                pl.col("id").alias("launchpad_id"),
                pl.col("name"),
                pl.col("full_name"),
                pl.col("locality"),
                pl.col("region"),
                pl.col("status")
            ])
        )

    def transform_payloads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Method to transform the data extracted from the "payloads" endpoint.

        Args:
            data (List[Dict[str, Any]]): The data to be transformed.

        Returns:
            pl.DataFrame: The DataFrame containing the transformed data.
        """
        df = self._to_df(data, "payloads")
        return (
            df.with_columns(
                pl.col("reused").cast(pl.Int8)
            ).select([
                pl.col("id").alias("payload_id"),
                pl.col("name"),
                pl.col("type"),
                pl.col("reused"),
                pl.col("mass_kg").fill_null(0).cast(pl.Float64),
                pl.col("orbit")
            ])
        )

    def transform_launches(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Method to transform the data extracted from the "launches" endpoint.

        Args:
            data (List[Dict[str, Any]]): The data to be transformed.

        Returns:
            pl.DataFrame: The DataFrame containing the transformed data.
        """
        df = self._to_df(data, "launches")
        
        return (
            df.with_columns([
                
                pl.col("date_utc").cast(pl.Datetime),
                pl.col("success").cast(pl.Int8).fill_null(0)
            ]).with_columns([
                pl.col("date_utc").dt.year().alias("launch_year")
            ]).select([
                pl.col("id").alias("launch_id"),
                pl.col("name"),
                pl.col("date_utc"),
                pl.col("success"),
                pl.col("flight_number"),
                pl.col("rocket").alias("rocket_id"),
               
                pl.col("launchpad").alias("launchpad_id") if "launchpad" in df.columns else pl.lit(None).alias("launchpad_id"),
                pl.col("launch_year")
            ])
        )

transformer = SpaceXTransformer()