import structlog
import polars as pl
from sqlalchemy import create_engine, text
from config.settings import settings

logger = structlog.get_logger()

class PostgresLoader:
    def __init__(self):
        self.engine = create_engine(settings.DATABASE_URL)
        # Garante a tabela assim que a classe é instanciada
        self._init_db()

    def _init_db(self):
        """Cria a estrutura inicial se não existir."""
        create_table_query = """
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
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(create_table_query))
        except Exception as e:
            logger.error("Erro ao inicializar tabela", error=str(e))
            raise

    def upsert_dataframe(self, df: pl.DataFrame, table_name: str, pk_col: str):
        if df.is_empty():
            logger.warn("DataFrame vazio, pulando carga", table=table_name)
            return

        log = logger.bind(table=table_name, rows=df.height)
        
        # O Polars usa nomes de colunas que devem bater com o banco
        records = df.to_dicts()
        cols = df.columns
        
        # Construção da Query
        col_names = ", ".join(cols)
        # SQLAlchemy 2.0 usa :column para bind parameters
        placeholder_names = ", ".join([f":{col}" for col in cols])
        update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col != pk_col])

        query = text(f"""
            INSERT INTO {table_name} ({col_names})
            VALUES ({placeholder_names})
            ON CONFLICT ({pk_col})
            DO UPDATE SET {update_stmt};
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)
                log.info("UPSERT concluído com sucesso")
        except Exception as e:
            log.error("Falha crítica no carregamento PostgreSQL", error=str(e))
            raise

loader = PostgresLoader()