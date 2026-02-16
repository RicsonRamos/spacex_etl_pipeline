import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    """
    Transforms raw SpaceX API data into clean Polars DataFrames.
    
    Fully resilient to API changes: creates missing columns with nulls or default values.
    Ensures consistent types, column names, and adds derived fields like launch year.
    """

    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """
        Convert a list of dictionaries into a Polars DataFrame.

        Args:
            raw_data (List[Dict[str, Any]]): Raw API data.
            endpoint (str): API endpoint name for logging.

        Returns:
            pl.DataFrame: Polars DataFrame with the raw data.
        """
        if not raw_data:
            logger.warning("Received empty data", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transform raw rocket data into a standardized DataFrame.

        Missing columns are filled with nulls or default values.

        Args:
            data (List[Dict[str, Any]]): Raw rocket data.

        Returns:
            pl.DataFrame: Cleaned rocket DataFrame.
        """
        df = self._to_df(data, "rockets")
        if df.is_empty():
            return df

        # Rename 'id' to 'rocket_id' if exists
        if "id" in df.columns:
            df = df.rename({"id": "rocket_id"})

        # Define columns we want to keep with types
        rocket_columns = {
            "rocket_id": None,
            "name": None,
            "type": None,
            "active": pl.Boolean,
            "stages": pl.Int32,
            "cost_per_launch": pl.Float64,
            "success_rate_pct": pl.Float64
        }

        # Build select expressions, handling missing columns
        select_exprs = []
        for col_name, col_type in rocket_columns.items():
            if col_name in df.columns:
                expr = pl.col(col_name)
                if col_type:
                    expr = expr.cast(col_type)
                select_exprs.append(expr)
            else:
                # Create nulls or default values
                if col_type == pl.Boolean:
                    select_exprs.append(pl.lit(False).alias(col_name))
                elif col_type:
                    select_exprs.append(pl.lit(None).cast(col_type).alias(col_name))
                else:
                    select_exprs.append(pl.lit(None).alias(col_name))

        return df.select(select_exprs)

    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transform raw launchpad data into a standardized DataFrame.

        Args:
            data (List[Dict[str, Any]]): Raw launchpad data.

        Returns:
            pl.DataFrame: Cleaned launchpad DataFrame.
        """
        df = self._to_df(data, "launchpads")
        if df.is_empty():
            return df

        # Rename 'id' to 'launchpad_id' if exists
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

    def transform_launches(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transform raw launch data into a standardized DataFrame.

        Converts UTC dates, renames columns, and adds 'launch_year'.

        Args:
            data (List[Dict[str, Any]]): Raw launch data.

        Returns:
            pl.DataFrame: Cleaned launch DataFrame.
        """
        df = self._to_df(data, "launches")
        if df.is_empty():
            return df

        # Convert date strings to datetime
        if "date_utc" in df.columns and df["date_utc"].dtype == pl.Utf8:
            df = df.with_columns([pl.col("date_utc").str.to_datetime(time_zone="UTC")])

        # Rename columns for consistency
        rename_map = {"id": "launch_id", "rocket": "rocket_id", "launchpad": "launchpad_id"}
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        if actual_rename:
            df = df.rename(actual_rename)

        # Define desired columns
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

        # Add derived column: launch_year
        if "date_utc" in df.columns:
            df = df.with_columns([pl.col("date_utc").dt.year().alias("launch_year")])
            select_exprs.append(pl.col("launch_year"))
        else:
            select_exprs.append(pl.lit(None).alias("launch_year"))

        return df.select(select_exprs)

    def transform_payloads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Transform raw payload data into a standardized DataFrame.

        Fills missing mass with 0.0, converts dates, and adds 'year_created'.

        Args:
            data (List[Dict[str, Any]]): Raw payload data.

        Returns:
            pl.DataFrame: Cleaned payload DataFrame.
        """
        df = self._to_df(data, "payloads")
        if df.is_empty():
            return df

        # Convert creation date to datetime
        if "date_created" in df.columns and df["date_created"].dtype == pl.Utf8:
            df = df.with_columns([pl.col("date_created").str.to_datetime(time_zone="UTC")])

        # Rename 'id' to 'payload_id'
        if "id" in df.columns:
            df = df.rename({"id": "payload_id"})

        # Define desired columns
        payload_columns = {
            "payload_id": None,
            "name": None,
            "type": None,
            "reused": pl.Boolean,
            "mass_kg": pl.Float64,
            "orbit": None,
            "date_created": None
        }

        select_exprs = []
        for col_name, col_type in payload_columns.items():
            if col_name in df.columns:
                expr = pl.col(col_name)
                if col_name == "mass_kg":
                    # Fill null mass with 0.0
                    expr = expr.cast(pl.Float64).fill_null(0.0)
                elif col_type:
                    expr = expr.cast(col_type)
                select_exprs.append(expr)
            else:
                # Missing columns
                if col_name == "mass_kg":
                    select_exprs.append(pl.lit(0.0).alias(col_name))
                elif col_type == pl.Boolean:
                    select_exprs.append(pl.lit(False).alias(col_name))
                elif col_type:
                    select_exprs.append(pl.lit(None).cast(col_type).alias(col_name))
                else:
                    select_exprs.append(pl.lit(None).alias(col_name))

        # Add derived column: year_created
        if "date_created" in df.columns:
            df = df.with_columns([pl.col("date_created").dt.year().alias("year_created")])
            select_exprs.append(pl.col("year_created"))
        else:
            select_exprs.append(pl.lit(None).alias("year_created"))

        return df.select(select_exprs)
