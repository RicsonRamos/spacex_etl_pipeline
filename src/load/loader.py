import polars as pl
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.config.schema_registry import SCHEMA_REGISTRY


class PostgresLoader:
    """
    Loader desacoplado e testável.

    Pode receber:
    - connection_string
    - engine (para testes)
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        engine: Optional[Engine] = None,
    ):
        if engine is not None:
            self.engine = engine
        elif connection_string:
            self.engine = create_engine(connection_string)
        else:
            raise ValueError(
                "You must provide either 'connection_string' or 'engine'."
            )

    
    # BRONZE / GOLD (simples append/replace)
    

    def load_bronze(self, df: pl.DataFrame, table: str) -> int:
        if df.is_empty():
            return 0

        df.to_pandas().to_sql(
            table,
            self.engine,
            if_exists="append",
            index=False,
        )
        return len(df)

    def load_gold(self, df: pl.DataFrame, table: str) -> int:
        if df.is_empty():
            return 0

        df.to_pandas().to_sql(
            table,
            self.engine,
            if_exists="replace",
            index=False,
        )
        return len(df)

    
    # SILVER (ENTITY-DRIVEN ou TABLE-DRIVEN)
    

    def load_silver(
        self,
        df: pl.DataFrame,
        entity: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> int:
        """
        Pode ser chamado de duas formas:

        load_silver(df, entity="launches")
        load_silver(df, table_name="silver_launches")
        """

        if df.is_empty():
            return 0

        # 🔹 Novo padrão (entity)
        if entity:
            schema = self._validate_schema(df, entity)
            table = schema.silver_table
            df = df.select(schema.columns)

        # 🔹 Padrão antigo (table_name direto)
        elif table_name:
            table = table_name

        else:
            raise ValueError(
                "Either 'entity' or 'table_name' must be provided."
            )

        return self._upsert(df, table)

    
    # BACKWARD COMPATIBILITY
    

    def upsert_silver(self, df: pl.DataFrame, entity: str) -> int:
        """Compatível com testes antigos"""
        if df.is_empty():
            return 0

        schema = self._validate_schema(df, entity)
        df = df.select(schema.columns)

        return self._upsert(df, schema.silver_table)

    def validate_and_align_schema(self, entity: str):
        """
        Método mantido para compatibilidade com flows antigos.
        Apenas valida existência da entidade.
        """
        if entity not in SCHEMA_REGISTRY:
            raise ValueError(
                f"Entity '{entity}' not found in SCHEMA_REGISTRY"
            )

        return SCHEMA_REGISTRY[entity]

    
    # VALIDAÇÃO DE SCHEMA
    

    def _validate_schema(self, df: pl.DataFrame, entity: str):
        if entity not in SCHEMA_REGISTRY:
            raise ValueError(
                f"Entity '{entity}' not found in SCHEMA_REGISTRY"
            )

        schema = SCHEMA_REGISTRY[entity]

        expected = set(schema.columns)
        received = set(df.columns)

        if not expected.issubset(received):
            missing = expected - received
            raise ValueError(
                f"Schema mismatch. Missing columns: {missing}"
            )

        return schema

    
    # UPSERT SIMPLES (INSERT)
    
    def _create_table_if_not_exists(
        self,
        df: pl.DataFrame,
        table_name: str,
    ):

        columns_sql = []

        for col, dtype in zip(df.columns, df.dtypes):

            if dtype == pl.Int64:
                sql_type = "BIGINT"
            elif dtype == pl.Float64:
                sql_type = "DOUBLE PRECISION"
            elif dtype == pl.Boolean:
                sql_type = "BOOLEAN"
            elif isinstance(dtype, pl.Datetime):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT"

            columns_sql.append(f"{col} {sql_type}")

        create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_sql)}
            )
        """

        with self.engine.begin() as conn:
            conn.execute(text(create_stmt))
    def _upsert(self, df: pl.DataFrame, table_name: str) -> int:
        """
        Insert simples.
        Não usa ON CONFLICT para manter compatibilidade com testes.
        """

        records = df.to_dicts()

        if not records:
            return 0

        columns = list(records[0].keys())
        insert_cols = ", ".join(columns)
        insert_params = ", ".join([f":{c}" for c in columns])
        
        self._create_table_if_not_exists(df, table_name)
        query = text(f"""
            INSERT INTO {table_name} ({insert_cols})
            VALUES ({insert_params})
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, records)

            return len(records)

        except SQLAlchemyError as e:
            raise e