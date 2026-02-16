import structlog
import polars as pl
from sqlalchemy import create_engine, text
from src.config.settings import settings

logger = structlog.get_logger()

class PostgresLoader:
    """
    A loader for loading data into a PostgreSQL database.
    
    The loader provides methods for creating tables if they do not exist,
    and for upserting data into those tables.
    """
    def __init__(self):
        """
        Initialize the loader.
        
        The loader creates an engine from the database URL in the settings.
        """
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True, 
            pool_size=5         
        )
        
        self._schemas = {
            "rockets": """
                rocket_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT,
                active BOOLEAN
            """,
            "launchpads": """
                launchpad_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                locality TEXT,
                region TEXT
            """,
            "payloads": """
                payload_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT,
                mass_kg FLOAT
            """,
            "launches": """
                launch_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                date_utc TIMESTAMPTZ NOT NULL,
                success BOOLEAN,
                flight_number INTEGER,
                rocket_id TEXT,
                launchpad_id TEXT,
                launch_year INTEGER
            """
        }

    def ensure_tables(self):
        """
        Ensure that all tables exist in the database.
        
        If a table does not exist, it is created with the schema defined
        in the `_schemas` dictionary.
        """
        with self.engine.begin() as conn:
            for table_name, schema in self._schemas.items():
                query = text(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
                conn.execute(query)
                logger.debug("Infraestrutura validada", table=table_name)

    def upsert_dataframe(self, df: pl.DataFrame, table_name: str, pk_col: str):
        """
        Upsert a DataFrame into a table in the database.
        
        If the DataFrame is empty, a warning is logged and the method returns.
        
        The loader first ensures that the table exists, and then inserts
        the data into the table. If a record already exists with the same
        primary key, the record is updated with the new values.
        """
        if df.is_empty():
            logger.warning("Carga ignorada: DataFrame vazio.", table=table_name)
            return

        self.ensure_tables()

        records = df.to_dicts()
        cols = df.columns
        
        
        update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != pk_col])

        query = text(f"""
            INSERT INTO {table_name} ({", ".join(cols)})
            VALUES ({", ".join([f":{c}" for c in cols])})
            ON CONFLICT ({pk_col}) DO UPDATE SET {update_stmt};
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)
                logger.info("Upsert concluído com sucesso", 
                           table=table_name, 
                           rows=df.height)
        except Exception as e:
            logger.error("Falha crítica no carregamento", 
                        table=table_name, 
                        error=str(e))
            raise


loader = PostgresLoader()