import json
import pandas as pd
from sqlalchemy import create_engine, text
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)

class PostgresLoader:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)

    def _serialize_complex_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte colunas de listas ou dicionários em strings JSON.
        Rigor: Evita o erro 'can't adapt type dict/list'.
        """
        df = df.copy()
        for col in df.columns:
            # Se a célula contiver algo que não é um tipo primitivo simples
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                logger.info(f"Serializando coluna complexa: {col}")
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
        return df

    def load_bronze(self, df: pd.DataFrame, table_name: str):
        try:
            # 1. Garantir Schema
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
                conn.commit()

            # 2. Serializar dados complexos para JSON strings
            df_prepared = self._serialize_complex_columns(df)

            # 3. Carga
            df_prepared.to_sql(
                name=table_name,
                con=self.engine,
                schema='raw',
                if_exists='replace',
                index=False
            )
            logger.info(f"Sucesso: raw.{table_name} carregada com {len(df)} linhas.")
        except Exception as e:
            logger.critical(f"Falha no carregamento SQL: {e}")
            raise