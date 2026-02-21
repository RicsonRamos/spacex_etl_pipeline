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

   
    # BRONZE
   

    def load_bronze(self, data: List[Dict[str, Any]], table_name: str) -> int:
        if not data:
            logger.warning("Bronze load skipped (empty dataset)", table=table_name)
            return 0

        formatted_data = [
            {"raw_data": json.dumps(item, default=str)}
            for item in data
        ]

        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"INSERT INTO {table_name} (raw_data) VALUES (:raw_data)"),
                    formatted_data,
                )

            logger.info("Bronze load successful", table=table_name, rows=len(data))
            return len(data)

        except SQLAlchemyError as e:
            logger.error("Bronze load failed", table=table_name, error=str(e))
            raise

    @task
    def validate_launches(data: list[dict]):
        for row in data:
            if row.get("launch_id") is None:
                raise ValueError(f"Missing launch_id in row: {row}")
            if row.get("date_utc") is None:
                raise ValueError(f"Missing date_utc in row: {row}")
        return data
    
    # SILVER
   

    def upsert_silver(self, df: pl.DataFrame, table_name: str, pk: str) -> int:
        if df.is_empty():
            logger.warning("Silver upsert skipped (empty DataFrame)", table=table_name)
            return 0

       
        # Serialização segura
       

        def safe_json(value):
            if value is None:
                return None

            # Series -> list
            if isinstance(value, pl.Series):
                return json.dumps(value.to_list(), default=str)

            # Struct vira dict
            if isinstance(value, dict):
                return json.dumps(value, default=str)

            # Lista
            if isinstance(value, list):
                return json.dumps(value, default=str)

            # Fallback seguro
            return json.dumps(value, default=str)

        cols_to_serialize = [
            col
            for col in df.columns
            if df[col].dtype in (pl.List, pl.Struct, pl.Object)
        ]

        if cols_to_serialize:
            logger.info(
                "Serializing complex columns",
                table=table_name,
                columns=cols_to_serialize,
            )

            df = df.with_columns(
                [
                    pl.col(col)
                    .map_elements(safe_json, return_dtype=pl.Utf8)
                    .alias(col)
                    for col in cols_to_serialize
                ]
            )

       
        # Garantir tipos Python puros
       

        records = df.to_dicts()

        if not records:
            return 0

       
        # Construção do UPSERT
       

        cols = df.columns

        if pk not in cols:
            raise ValueError(f"Primary key '{pk}' not found in DataFrame")

        update_cols = [c for c in cols if c != pk]

        set_clause = ", ".join(
            [f"{c} = EXCLUDED.{c}" for c in update_cols]
        )

        insert_columns = ", ".join(cols)
        insert_values = ", ".join([f":{c}" for c in cols])

        query = text(f"""
            INSERT INTO {table_name} ({insert_columns})
            VALUES ({insert_values})
            ON CONFLICT ({pk})
            DO UPDATE SET {set_clause}
        """)

       
        # Execução
       

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)

            logger.info(
                "Silver upsert successful",
                table=table_name,
                rows=len(records),
            )

            return len(records)

        except SQLAlchemyError as e:
            logger.error(
                "Silver upsert failed",
                table=table_name,
                error=str(e),
            )
            raise

   
    # GOLD
   

    def refresh_gold_view(self, view_name: str, definition: str):

        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
                conn.execute(text(f"CREATE VIEW {view_name} AS {definition}"))

            logger.info("Gold layer refreshed", view=view_name)

        except SQLAlchemyError as e:
            logger.error("Gold refresh failed", view=view_name, error=str(e))
            raise