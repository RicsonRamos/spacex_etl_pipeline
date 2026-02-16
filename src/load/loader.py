import re
import structlog
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from src.config.settings import settings
from src.database.models import Base

logger = structlog.get_logger()


class PostgresLoader:
    def __init__(self) -> None:
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            future=True
        )

    def ensure_tables(self) -> None:
        """
        Garante que as tabelas definidas nos models existam no banco.
        Observação: create_all NÃO altera tabelas existentes.
        """
        Base.metadata.create_all(self.engine)

    def _validate_identifier(self, name: str) -> None:
        """
        Proteção básica contra SQL injection em table_name / pk_col.
        Aceita apenas letras, números e underscore.
        """
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
            raise ValueError(f"Identificador inválido: {name}")

    def upsert_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        pk_col: str
    ) -> None:
        """
        Executa UPSERT (INSERT ... ON CONFLICT DO UPDATE)
        a partir de um DataFrame Polars.
        """

        if df.is_empty():
            logger.warning("Carga ignorada: DataFrame vazio.", table=table_name)
            return

        self._validate_identifier(table_name)
        self._validate_identifier(pk_col)

        self.ensure_tables()

        records = df.to_dicts()
        columns = df.columns

        update_clause = ", ".join(
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col != pk_col
        )

        insert_stmt = text(f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({", ".join(f":{col}" for col in columns)})
            ON CONFLICT ({pk_col})
            DO UPDATE SET {update_clause};
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(insert_stmt, records)

            logger.info(
                "Upsert concluído",
                table=table_name,
                rows=len(records)
            )

        except SQLAlchemyError as exc:
            logger.error(
                "Falha crítica no carregamento",
                table=table_name,
                error=str(exc)
            )
            raise
            

loader = PostgresLoader()
