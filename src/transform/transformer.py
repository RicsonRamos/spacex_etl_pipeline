import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    """
    Responsible for transforming raw data from the API into Polars DataFrames.
    Totally resilient to changes in the API: creates columns with null values or default values.
    """

    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """
        Converts raw data into a Polars DataFrame.

        Args:
            raw_data: List of dictionaries containing raw data.
            endpoint: Name of the endpoint being processed.

        Returns:
            pl.DataFrame: DataFrame containing the transformed data.
        """
        if not raw_data:
            logger.warning("No data received", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms raw data from the "rockets" endpoint into a Polars DataFrame.

        Args:
            data: List of dictionaries containing raw data.

        Returns:
            pl.DataFrame: DataFrame containing the transformed data.
        """
        df = self._to_df(data, "rockets")
        if df.is_empty():
            return df

        # Rename id to rocket_id if it exists
        if "id" in df.columns:
            df = df.rename({"id": "rocket_id"})

        # Create columns with default values if they don't exist
        rocket_columns = {
            "rocket_id": None,
            "name": None,
            "type": None,
            "active": pl.Boolean,
            "stages": pl.Int32,
            "cost_per_launch": pl.Float64,
            "success_rate_pct": pl.Float64
        }

        select_exprs = []
        for col_name, col_type in rocket_columns.items():
            if col_name in df.columns:
                expr = pl.col(col_name)
                if col_type:
                    expr = expr.cast(col_type)
                select_exprs.append(expr)
            else:
                # Create column with null values if it doesn't exist
                if col_type == pl.Boolean:
                    select_exprs.append(pl.lit(False).alias(col_name))
                elif col_type:
                    select_exprs.append(pl.lit(None).cast(col_type).alias(col_name))
                else:
                    select_exprs.append(pl.lit(None).alias(col_name))

        return df.select(select_exprs)

    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms raw data from the "launchpads" endpoint into a Polars DataFrame.

        Args:
            data: List of dictionaries containing raw data.

        Returns:
            pl.DataFrame: DataFrame containing the transformed data.
        """
        df = self._to_df(data, "launchpads")
        if df.is_empty():
            return df

        if "id" in df.columns:
            df = df.rename({"id": "launchpad_id"})

        launchpad_columns = {
            "launchpad_id": None,
            "name": None,
            "full_name": None,
            "locality": None,
            "region": None,
            "status": None
        }

        select_exprs = []
        for col_name, col_type in launchpad_columns.items():
            if col_name in df.columns:
                expr = pl.col(col_name)
                if col_type:
                    expr = expr.cast(col_type)
                select_exprs.append(expr)
            else:
                select_exprs.append(pl.lit(None).alias(col_name))

        return df.select(select_exprs)

    def transform_launch(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transforms raw data from the "launches" endpoint into a Polars DataFrame.

        Args:
            data: List of dictionaries containing raw data.

        Returns:
            pl.DataFrame: DataFrame containing the transformed data.
        """
        df = self._to_df(data, "launches")
        if df.is_empty():
            return df

        # Convert date_utc to datetime if it exists
        if "date_utc" in df.columns and df["date_utc"].dtype == pl.Utf8:
            df = df.with_columns([pl.col("date_utc").str.to_datetime(time_zone="UTC")])

        # Rename id/rocket/launchpad columns
        rename_map = {"id": "launch_id", "rocket": "rocket_id", "launchpad": "launchpad_id"}
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        if actual_rename:
            df = df.rename(actual_rename)

        launch_columns = {
            "launch_id": None,
            "name": None,
            "date_utc": None,
            "success": pl.Boolean,
            "flight_number": pl.Int32,
            "rocket_id": None,
            "launchpad_id": None
        }

        select_exprs = []
        for col_name, col_type in launch_columns.items():
            if col_name in df.columns:
                expr = pl.col(col_name)
                if col_type:
                    expr = expr.cast(col_type)
                select_exprs.append(expr)
            else:
                if col_type == pl.Boolean:
                    select_exprs.append(pl.lit(False).alias(col_name))
                elif col_type:
                    select_exprs.append(pl.lit(None).cast(col_type).alias(col_name))
                else:
                    select_exprs.append(pl.lit(None).alias(col_name))

        # Add year of launch

