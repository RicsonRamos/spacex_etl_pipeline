import structlog
import polars as pl
from sqlalchemy import create_engine, text
from src.config.settings import settings

logger = structlog.get_logger()

class PostgresLoader:
    def __init__(self):
        # Apenas prepara o motor, não abre conexão ainda
        self.engine = create_engine(settings.DATABASE_URL)

    def ensure_table(self):
        """DML isolado para garantir a infraestrutura."""
        query = text("""
            CREATE TABLE IF NOT EXISTS launches (
                id SERIAL PRIMARY KEY,
                launch_id TEXT UNIQUE,
                name TEXT,
                date_utc TIMESTAMP,
                success INTEGER,
                flight_number INTEGER,
                rocket_id TEXT,
                launchpad_id TEXT,
                launch_year INTEGER
            );
        """)
        with self.engine.begin() as conn:
            conn.execute(query)

    def upsert_dataframe(self, df: pl.DataFrame, table_name: str, pk_col: str):
        if df.is_empty():
            logger.warn("Carga ignorada: DataFrame vazio.")
            return

        records = df.to_dicts()
        cols = df.columns
        update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != pk_col])

        query = text(f"""
            INSERT INTO {table_name} ({", ".join(cols)})
            VALUES ({", ".join([f":{c}" for c in cols])})
            ON CONFLICT ({pk_col}) DO UPDATE SET {update_stmt};
        """)

        with self.engine.begin() as conn:
            conn.execute(query, records)
            logger.info("Upsert concluído", table=table_name, rows=df.height)