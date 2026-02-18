import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    """
    Transforms raw SpaceX API data into clean Polars DataFrames.
    
    Fully resilient to API changes: creates missing columns with nulls or default values.
    Ensures consistent types, column names, and adds derived fields.
    """

    def _to_df(self, raw_data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
        """Helper to convert list of dicts to Polars DataFrame."""
        if not raw_data:
            logger.warning("Received empty data", endpoint=endpoint)
            return pl.DataFrame()
        return pl.from_dicts(raw_data)

    def transform_rockets(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "rockets")
        if df.is_empty():
            return df

        if "id" in df.columns:
            df = df.rename({"id": "rocket_id"})

        rocket_columns = {
            "rocket_id": pl.Utf8,
            "name": pl.Utf8,
            "type": pl.Utf8,
            "active": pl.Boolean,
            "stages": pl.Int32,
            "cost_per_launch": pl.Float64,
            "success_rate_pct": pl.Float64
        }

        select_exprs = []
        for col_name, col_type in rocket_columns.items():
            if col_name in df.columns:
                select_exprs.append(pl.col(col_name).cast(col_type))
            else:
                val = False if col_type == pl.Boolean else None
                select_exprs.append(pl.lit(val).cast(col_type).alias(col_name))

        return df.select(select_exprs)

    def transform_launchpads(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "launchpads")
        if df.is_empty():
            return df

        if "id" in df.columns:
            df = df.rename({"id": "launchpad_id"})

        launchpad_columns = {
            "launchpad_id": pl.Utf8,
            "name": pl.Utf8,
            "full_name": pl.Utf8,
            "locality": pl.Utf8,
            "region": pl.Utf8,
            "status": pl.Utf8
        }

        select_exprs = []
        for col_name, col_type in launchpad_columns.items():
            if col_name in df.columns:
                select_exprs.append(pl.col(col_name).cast(col_type))
            else:
                select_exprs.append(pl.lit(None).cast(col_type).alias(col_name))

        return df.select(select_exprs)

    def transform_launches(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        df = self._to_df(data, "launches")
        if df.is_empty():
            return df

        # RIGOR: Só aplica conversão se a coluna for do tipo String/Utf8
        if "date_utc" in df.columns:
            if df["date_utc"].dtype == pl.Utf8:
                df = df.with_columns([
                    pl.col("date_utc").str.to_datetime(time_zone="UTC", strict=False)
                ])
            else:
                # Se já for datetime, apenas garante que está em UTC
                df = df.with_columns([
                    pl.col("date_utc").dt.replace_time_zone("UTC")
                ])

        rename_map = {"id": "launch_id", "rocket": "rocket_id", "launchpad": "launchpad_id"}
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        if actual_rename:
            df = df.rename(actual_rename)

        launch_columns = {
            "launch_id": pl.Utf8,
            "name": pl.Utf8,
            "date_utc": pl.Datetime,
            "success": pl.Boolean,
            "flight_number": pl.Int32,
            "rocket_id": pl.Utf8,
            "launchpad_id": pl.Utf8
        }

        select_exprs = []
        for col_name, col_type in launch_columns.items():
            if col_name in df.columns:
                select_exprs.append(pl.col(col_name).cast(col_type))
            else:
                val = False if col_type == pl.Boolean else None
                select_exprs.append(pl.lit(val).cast(col_type).alias(col_name))

        # Adicionar launch_year derivado
        if "date_utc" in df.columns:
            df = df.with_columns([pl.col("date_utc").dt.year().alias("launch_year")])
            select_exprs.append(pl.col("launch_year"))
        else:
            select_exprs.append(pl.lit(None).cast(pl.Int32).alias("launch_year"))

        return df.select(select_exprs)

    def transform_payloads(self, data: List[Dict[str, Any]], valid_launch_ids: List[str] = None) -> pl.DataFrame:
        """
        RIGOR: Prevenção de ForeignKeyViolation via filtragem de IDs órfãos.
        """
        df = self._to_df(data, "payloads")
        if df.is_empty():
            return df

        rename_map = {"id": "payload_id", "launch": "launch_id"}
        actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        if actual_rename:
            df = df.rename(actual_rename)

        # Datas
        if "date_created" in df.columns:
            df = df.with_columns([
                pl.col("date_created")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
                .alias("date_created")
            ])
            df = df.with_columns([pl.col("date_created").dt.year().alias("year_created")])
        else:
            df = df.with_columns([
                pl.lit(None).cast(pl.Datetime).alias("date_created"),
                pl.lit(None).cast(pl.Int32).alias("year_created")
            ])

        expected_schema = {
            "payload_id": pl.Utf8,
            "launch_id": pl.Utf8,
            "name": pl.Utf8,
            "type": pl.Utf8,
            "reused": pl.Boolean,
            "mass_kg": pl.Float64,
            "orbit": pl.Utf8,
            "date_created": pl.Datetime,
            "year_created": pl.Int32
        }

        select_exprs = []
        for col, dtype in expected_schema.items():
            if col in df.columns:
                if col == "mass_kg":
                    select_exprs.append(pl.col(col).cast(pl.Float64).fill_null(0.0))
                elif col == "reused":
                    select_exprs.append(pl.col(col).cast(pl.Boolean).fill_null(False))
                else:
                    select_exprs.append(pl.col(col).cast(dtype))
            else:
                select_exprs.append(pl.lit(None).cast(dtype).alias(col))

        df = df.select(select_exprs)

        # Filtragem de Integridade Referencial
        if valid_launch_ids is not None:
            initial_count = len(df)
            df = df.filter(
                pl.col("launch_id").is_in(valid_launch_ids) | pl.col("launch_id").is_null()
            )
            dropped = initial_count - len(df)
            if dropped > 0:
                logger.info("Payloads órfãos removidos", count=dropped)

        return df