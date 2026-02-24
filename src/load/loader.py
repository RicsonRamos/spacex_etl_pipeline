import polars as pl
from typing import Optional, Any
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
            # Se um engine for passado, use-o diretamente
            self.engine = engine
        elif connection_string:
            # Se a string de conexão for passada, cria o engine
            self.engine = create_engine(connection_string)
        else:
            # Se nenhum dos dois for passado, lança um erro
            raise ValueError(
                "You must provide either 'connection_string' or 'engine'."
            )

        # Assegure-se de que 'self.engine' é do tipo Engine
        if not isinstance(self.engine, Engine):
            raise ValueError("The provided engine is not an instance of SQLAlchemy Engine.")

    def _create_table_if_not_exists(
        self,
        df: pl.DataFrame,
        table_name: str,
    ):
        """
        Cria a tabela no banco de dados caso não exista.
        """
        columns_sql = []

        # Mapeamento de tipos do Polars para SQL
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

    def load_bronze(self, df: pl.DataFrame, table: str) -> int:
        """
        Carrega os dados no banco no estágio bronze.
        """
        if df.is_empty():
            return 0

        # Criar a tabela se ela não existir
        self._create_table_if_not_exists(df, table)

        # Converter o DataFrame Polars para uma lista de dicionários
        records = df.to_dicts()

        # Obter a conexão do engine
        with self.engine.connect() as conn:
            insert_cols = ", ".join(df.columns)
            insert_params = ", ".join([f":{col}" for col in df.columns])

            query = text(f"""
                INSERT INTO {table} ({insert_cols})
                VALUES ({insert_params})
            """)

            try:
                # Inserir os dados no banco
                conn.execute(query, records)
                return len(records)

            except SQLAlchemyError as e:
                raise e

    def load_gold(self, df: pl.DataFrame, table: str) -> int:
        """
        Carrega os dados no banco no estágio gold.
        Substitui os dados na tabela.
        """
        if df.is_empty():
            return 0

        # Converter o DataFrame Polars para uma lista de dicionários
        records = df.to_dicts()

        # Obter a conexão do engine
        with self.engine.connect() as conn:
            insert_cols = ", ".join(df.columns)
            insert_params = ", ".join([f":{col}" for col in df.columns])

            query = text(f"""
                INSERT INTO {table} ({insert_cols})
                VALUES ({insert_params})
            """)

            try:
                # Apagar dados antigos e inserir novos
                conn.execute(text(f"DELETE FROM {table}"))
                conn.execute(query, records)
                return len(records)

            except SQLAlchemyError as e:
                raise e

    def load_silver(self, df: pl.DataFrame, entity: Optional[str] = None, table_name: Optional[str] = None) -> int:
        """
        Carrega dados no estágio silver.
        Pode ser chamado de duas formas: 
        load_silver(df, entity="launches")
        load_silver(df, table_name="silver_launches")
        """
        if df.is_empty():
            return 0

        # Se 'entity' for passado, validamos o schema
        if entity:
            schema = self._validate_schema(df, entity)
            table = schema.silver_table
            df = df.select(schema.columns)

        # Se 'table_name' for passado diretamente
        elif table_name:
            table = table_name

        else:
            raise ValueError(
                "Either 'entity' or 'table_name' must be provided."
            )

        return self._upsert(df, table)

    def _upsert(self, df: pl.DataFrame, table_name: str) -> int:
        """
        Faz um insert simples (não usa ON CONFLICT) para manter compatibilidade com testes.
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
            # Usando a conexão dentro do contexto de 'with'
            with self.engine.connect() as conn:
                conn.execute(query, records)

            return len(records)

        except SQLAlchemyError as e:
            raise e
    
    def _validate_schema(self, df: pl.DataFrame, entity: str) -> Any:
        """
        Valida o schema dos dados (colunas e tipos) antes de carregar no banco.
        """
        schema_cfg = SCHEMA_REGISTRY.get(entity)
        if not schema_cfg:
            raise ValueError(f"Schema não encontrado para o endpoint {entity}")

        # Verificar se as colunas no dataframe correspondem ao schema
        expected_columns = schema_cfg.columns
        actual_columns = df.columns

        missing_columns = set(expected_columns) - set(actual_columns)
        if missing_columns:
            raise ValueError(f"Faltam colunas no dataframe: {', '.join(missing_columns)}")

        # Verificar se os tipos das colunas estão corretos
        for col, expected_type in schema_cfg.column_types.items():
            if col in actual_columns:
                actual_type = df.schema[col]
                if actual_type != expected_type:
                    raise ValueError(f"Tipo incorreto para coluna {col}. Esperado: {expected_type}, Encontrado: {actual_type}")

        return schema_cfg