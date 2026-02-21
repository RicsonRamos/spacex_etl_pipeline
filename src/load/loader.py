import structlog
import polars as pl
import json
from prefect import task
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import settings

logger = structlog.get_logger()


class PostgresLoader:

    def __init__(self):
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            future=True,
        )

    # =====================================================
    # BRONZE — RAW ONLY (JSON)
    # =====================================================

    def load_bronze(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        source: str,
    ) -> int:

        if not data:
            logger.warning("Bronze skipped (empty)", table=table_name)
            return 0

        rows = [
            {
                "source": source,
                "raw_data": json.dumps(row, default=str),
            }
            for row in data
        ]

        query = text(f"""
            INSERT INTO {table_name} (source, raw_data)
            VALUES (:source, :raw_data)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, rows)

            logger.info(
                "Bronze load OK",
                table=table_name,
                rows=len(rows),
            )

            return len(rows)

        except SQLAlchemyError as e:
            logger.error("Bronze failed", table=table_name, error=str(e))
            raise


    # =====================================================
    # VALIDATION (SILVER INPUT)
    # =====================================================

    @staticmethod
    @task
    def validate_launches(data: list[dict]):

        for row in data:

            if not row.get("id"):
                raise ValueError(f"Missing id: {row}")

            if not row.get("date_utc"):
                raise ValueError(f"Missing date_utc: {row}")

        return data


    # =====================================================
    # SILVER — UPSERT ESTRUTURADO
    # =====================================================

    def upsert_silver(
        self,
        df: pl.DataFrame,
        table_name: str,
        pk: str,
    ) -> int:

        if df.is_empty():
            logger.warning("Silver skipped (empty)", table=table_name)
            return 0


        # -----------------------------
        # Serialização segura
        # -----------------------------

        def safe_json(v):

            if v is None:
                return None

            if isinstance(v, (list, dict)):
                return json.dumps(v, default=str)

            if isinstance(v, pl.Series):
                return json.dumps(v.to_list(), default=str)

            return v


        complex_cols = [
            c for c in df.columns
            if df[c].dtype in (pl.List, pl.Struct, pl.Object)
        ]

        if complex_cols:

            df = df.with_columns([
                pl.col(c)
                .map_elements(safe_json, return_dtype=pl.Utf8)
                .alias(c)
                for c in complex_cols
            ])


        records = df.to_dicts()

        if not records:
            return 0


        # -----------------------------
        # Validação PK
        # -----------------------------

        if pk not in df.columns:
            raise ValueError(f"PK '{pk}' not found in {table_name}")


        # -----------------------------
        # SQL UPSERT
        # -----------------------------

        cols = df.columns

        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f":{c}" for c in cols])

        update_cols = [c for c in cols if c != pk]

        set_clause = ", ".join(
            f"{c}=EXCLUDED.{c}"
            for c in update_cols
        )

        query = text(f"""
            INSERT INTO {table_name} ({insert_cols})
            VALUES ({insert_vals})
            ON CONFLICT ({pk})
            DO UPDATE SET {set_clause}
        """)


        # -----------------------------
        # Execução
        # -----------------------------

        try:

            with self.engine.begin() as conn:
                conn.execute(query, records)

            logger.info(
                "Silver upsert OK",
                table=table_name,
                rows=len(records),
            )

            return len(records)

        except SQLAlchemyError as e:

            logger.error(
                "Silver failed",
                table=table_name,
                error=str(e),
            )

            raise


    # =====================================================
    # GOLD — VIEWS
    # =====================================================

    def refresh_gold_view(
        self,
        view_name: str,
        definition: str,
    ):

        try:

            with self.engine.begin() as conn:

                conn.execute(
                    text(f"DROP VIEW IF EXISTS {view_name} CASCADE")
                )

                conn.execute(
                    text(f"CREATE VIEW {view_name} AS {definition}")
                )


            logger.info("Gold refreshed", view=view_name)

        except SQLAlchemyError as e:

            logger.error(
                "Gold failed",
                view=view_name,
                error=str(e),
            )

            raise