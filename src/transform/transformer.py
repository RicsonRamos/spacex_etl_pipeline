import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()


class SpaceXTransformer:
    """Responsible for cleaning and strictly typing SpaceX data using Polars for efficient in-memory processing.
    
    The transformers are organized by endpoint, and each transformer method
    takes a list of dictionaries as input and returns a Polars DataFrame.
    """
    
    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """Converts a list of dictionaries into a Polars DataFrame.
        
        Args:
            raw_data (List[Dict[str, Any]]): The list of dictionaries to convert.
            endpoint (str): The name of the endpoint being processed.
        
        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        if not raw_data:
            logger.warning("Empty data received", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)
    
    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms the 'rockets' endpoint data into a Polars DataFrame.
        
        Args:
            data (List[Dict[str, Any]]): The list of dictionaries to transform.
        
        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        df = self._to_df(data, "rockets")
        return (
            df.select([
                pl.col("id").alias("rocket_id"),
                pl.col("name"),
                pl.col("type"),
                pl.col("active").cast(pl.Boolean),
                pl.col("stages").cast(pl.Int32),
                pl.col("cost_per_launch").cast(pl.Float64),
                pl.col("success_rate_pct").cast(pl.Float64)
            ])
        )
    
    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms the 'launchpads' endpoint data into a Polars DataFrame.
        
        Args:
            data (List[Dict[str, Any]]): The list of dictionaries to transform.
        
        Returns:
            pl.DataFrame: The resulting DataFrame.
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
        """Transforms the 'payloads' endpoint data into a Polars DataFrame.
        
        Args:
            data (List[Dict[str, Any]]): The list of dictionaries to transform.
        
        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        df = self._to_df(data, "payloads")
        return (
            df.select([
                pl.col("id").alias("payload_id"),
                pl.col("name"),
                pl.col("type"),
                pl.col("reused").cast(pl.Boolean),
                pl.col("mass_kg").fill_null(0).cast(pl.Float64),
                pl.col("orbit")
            ])
        )
    
    def transform_launches(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms the 'launches' endpoint data into a Polars DataFrame.
        
        Args:
            data (List[Dict[str, Any]]): The list of dictionaries to transform.
        
        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        df = self._to_df(data, "launches")
        
        return (
            df.with_columns([
                pl.col("date_utc").str.to_datetime(),
                pl.col("success").cast(pl.Boolean)
            ])
            .with_columns([
                pl.col("date_utc").dt.year().alias("launch_year")
            ])
            .select([
                pl.col("id").alias("launch_id"),
                pl.col("name"),
                pl.col("date_utc"),
                pl.col("success"),
                pl.col("flight_number").cast(pl.Int32),
                pl.col("rocket").alias("rocket_id"),
                pl.col("launchpad").alias("launchpad_id"),
                pl.col("launch_year")
            ])
        )


transformer = SpaceXTransformer()
