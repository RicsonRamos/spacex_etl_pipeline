import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()


class SpaceXTransformer:

    def transform(
        self,
        endpoint: str,
        data: List[Dict[str, Any]]
    ) -> pl.DataFrame:

        if not data:
            logger.warning(
                "Nenhum dado recebido",
                endpoint=endpoint
            )
            return pl.DataFrame()

        try:
            df = pl.from_dicts(data)

        except Exception as e:
            logger.exception(
                "Erro ao criar DataFrame",
                endpoint=endpoint,
                error=str(e)
            )
            return pl.DataFrame()

        # -----------------------------
        # PADRONIZAÇÃO DE PK
        # -----------------------------
        endpoint_pk_map = {
            "launches": "launch_id",
            "rockets": "rocket_id",
            "launchpads": "launchpad_id"
        }

        if "id" in df.columns:

            pk_name = endpoint_pk_map.get(
                endpoint,
                f"{endpoint.rstrip('s')}_id"
            )

            df = df.rename({"id": pk_name})

        # -----------------------------
        # DISPATCHER
        # -----------------------------
        dispatch_map = {
            "launches": self._transform_launches,
            "rockets": self._transform_rockets,
            "launchpads": self._transform_launchpads
        }

        transform_fn = dispatch_map.get(endpoint)

        if not transform_fn:
            logger.warning(
                "Transformação padrão aplicada",
                endpoint=endpoint
            )
            return df

        return transform_fn(df)

    # =====================================================
    # LAUNCHES
    # =====================================================
    def _transform_launches(
        self,
        df: pl.DataFrame
    ) -> pl.DataFrame:

        if "date_utc" not in df.columns:
            logger.error("Coluna date_utc ausente")
            return df

        df = df.with_columns(
            pl.col("date_utc")
            .str.strptime(
                pl.Datetime,
                format="%Y-%m-%dT%H:%M:%S%.fZ",
                strict=False
            )
            .dt.replace_time_zone("UTC")
            .alias("launch_datetime")
        )

        df = df.with_columns(
            pl.col("launch_datetime")
            .dt.year()
            .alias("launch_year")
        )

        return df

    # =====================================================
    # ROCKETS
    # =====================================================
    def _transform_rockets(
        self,
        df: pl.DataFrame
    ) -> pl.DataFrame:

        required = [
            "rocket_id",
            "name",
            "active",
            "cost_per_launch",
            "success_rate_pct"
        ]

        self._check_required_columns(df, required, "rockets")

        return df.select([
            pl.col("rocket_id"),
            pl.col("name"),
            pl.col("active").cast(pl.Boolean),
            pl.col("cost_per_launch").cast(pl.Int64),
            pl.col("success_rate_pct").cast(pl.Float64)
        ])

    # =====================================================
    # LAUNCHPADS
    # =====================================================
    def _transform_launchpads(
        self,
        df: pl.DataFrame
    ) -> pl.DataFrame:

        required = [
            "launchpad_id",
            "full_name",
            "status",
            "rockets"
        ]

        self._check_required_columns(df, required, "launchpads")

        return df.select([
            pl.col("launchpad_id"),
            pl.col("full_name"),
            pl.col("status"),
            pl.col("rockets")
        ])

    # =====================================================
    # VALIDATION
    # =====================================================
    def _check_required_columns(
        self,
        df: pl.DataFrame,
        required: list,
        endpoint: str
    ):

        missing = [
            col for col in required
            if col not in df.columns
        ]

        if missing:
            logger.error(
                "Colunas obrigatórias ausentes",
                endpoint=endpoint,
                missing=missing
            )
            raise ValueError(
                f"Colunas ausentes em {endpoint}: {missing}"
            )