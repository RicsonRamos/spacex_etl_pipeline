import structlog
import polars as pl
import json

from typing import List, Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from prefect import task

from src.config.settings import settings
from src.config.schema_registry import TABLE_REGISTRY


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
    # BRONZE — RAW JSON
    # =====================================================

    def load_bronze(
        self,
        data: List[Dict[str, Any]],
        entity: str,
        source: str,
    ) -> int:

        if not data:
            logger.warning("Bronze skipped (empty)", entity=entity)
            return 0


        meta = TABLE_REGISTRY.get(entity)

        if not meta:
            raise ValueError(f"Entity not registered: {entity}")


        table = meta["bronze"]


        rows = [
            {
                "source": source,
                "raw_data": json.dumps(row, default=str),
            }
            for row in data
        ]


        query = text(f"""
            INSERT INTO {table} (source, raw_data)
            VALUES (:source, :raw_data)
        """)


        try:

            with self.engine.begin() as conn:
                conn.execute(query, rows)


            logger.info(
                "Bronze load OK",
                entity=entity,
                table=table,
                rows=len(rows),
            )

            return len(rows)


        except SQLAlchemyError as e:

            logger.error(
                "Bronze failed",
                entity=entity,
                table=table,
                error=str(e),
            )

            raise


    # =====================================================
    # GENERIC VALIDATION
    # =====================================================

    @staticmethod
    @task
    def validate(entity: str, data: list[dict]) -> list[dict]:

        meta = TABLE_REGISTRY.get(entity)

        if not meta:
            raise ValueError(f"Entity not registered: {entity}")


        required = meta.get("required", [])


        for row in data:

            for col in required:

                if not row.get(col):
                    raise ValueError(
                        f"Missing '{col}' in {entity}: {row}"
                    )

        return data


    # =====================================================
    # SILVER — CANONICAL UPSERT
    # =====================================================

    def upsert_silver(
        self,
        df: pl.DataFrame,
        entity: str,
    ) -> int:

        if df.is_empty():
            logger.warning("Silver skipped (empty)", entity=entity)
            return 0


        meta = TABLE_REGISTRY.get(entity)

        if not meta:
            raise ValueError(f"Entity not registered: {entity}")


        table = meta["silver"]
        pk = meta["pk"]

        columns = list(meta["columns"].keys())
        required = meta.get("required", [])


        # -----------------------------
        # VALIDATION
        # -----------------------------

        missing_required = set(required) - set(df.columns)

        if missing_required:
            raise ValueError(
                f"Missing required columns for {entity}: {missing_required}"
            )


        extra_cols = set(df.columns) - set(columns)

        if extra_cols:

            logger.warning(
                "Extra columns ignored",
                entity=entity,
                cols=list(extra_cols),
            )


        df = df.select([c for c in columns if c in df.columns])


        if pk not in df.columns:
            raise ValueError(f"Primary key '{pk}' not found in {entity}")


        # -----------------------------
        # SAFE JSON SERIALIZATION
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
                .map_elements(
                    safe_json,
                    return_dtype=pl.Utf8
                )
                .alias(c)
                for c in complex_cols
            ])


        records = df.to_dicts()

        if not records:
            return 0


        # -----------------------------
        # SQL UPSERT
        # -----------------------------

        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join(f":{c}" for c in df.columns)

        update_cols = [c for c in df.columns if c != pk]

        if not update_cols:
            raise ValueError(
                f"No updatable columns for {entity}"
            )


        set_clause = ", ".join(
            f"{c}=EXCLUDED.{c}"
            for c in update_cols
        )


        query = text(f"""
            INSERT INTO {table} ({insert_cols})
            VALUES ({insert_vals})
            ON CONFLICT ({pk})
            DO UPDATE SET {set_clause}
        """)


        # -----------------------------
        # EXECUTION
        # -----------------------------

        try:

            with self.engine.begin() as conn:
                conn.execute(query, records)


            logger.info(
                "Silver upsert OK",
                entity=entity,
                table=table,
                rows=len(records),
            )

            return len(records)


        except SQLAlchemyError as e:

            logger.error(
                "Silver failed",
                entity=entity,
                table=table,
                error=str(e),
            )

            raise


    # =====================================================
    # GOLD — VIEW REFRESH
    # =====================================================

    def refresh_gold_view(
        self,
        entity: str,
    ) -> None:

        meta = TABLE_REGISTRY.get(entity)

        if not meta:
            raise ValueError(f"Entity not registered: {entity}")


        view = meta.get("gold_view")
        definition = meta.get("gold_definition")


        if not view or not definition:
            raise ValueError(
                f"Gold config missing for {entity}"
            )


        try:

            with self.engine.begin() as conn:

                conn.execute(
                    text(f"DROP VIEW IF EXISTS {view} CASCADE")
                )

                conn.execute(
                    text(f"""
                        CREATE VIEW {view} AS
                        {definition}
                    """)
                )


            logger.info(
                "Gold refreshed",
                entity=entity,
                view=view,
            )


        except SQLAlchemyError as e:

            logger.error(
                "Gold failed",
                entity=entity,
                view=view,
                error=str(e),
            )

            raise