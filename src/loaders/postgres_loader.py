import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect
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
            # Verifica se há qualquer dicionário ou lista na coluna
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                logger.info(f"Serializando coluna complexa: {col}")
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
        return df

    def load_bronze(self, df: pd.DataFrame, table_name: str):
        """
        Carga na camada Bronze (schema raw). 
        Rigor: Usa TRUNCATE se a tabela existe para não quebrar as Views do dbt.
        """
        try:
            # 1. Serializar dados complexos
            df_prepared = self._serialize_complex_columns(df)

            # 2. Verificar se a tabela já existe no schema 'raw'
            inspector = inspect(self.engine)
            table_exists = inspector.has_table(table_name, schema='raw')

            if table_exists:
                # Rigor: Se existe, limpa os dados (Truncate) sem deletar a estrutura (Drop)
                # Isso preserva as dependências (Views) do dbt.
                with self.engine.connect() as conn:
                    conn.execute(text(f'TRUNCATE TABLE raw."{table_name}"'))
                    conn.commit()
                
                # Se a tabela já existe, usamos 'append' para inserir os novos dados após o truncate
                mode = 'append'
                logger.info(f"Tabela raw.{table_name} truncada para nova carga.")
            else:
                # Se não existe (ex: nova tabela de Rockets), usamos 'fail' ou deixamos o pandas criar
                mode = 'replace' 
                logger.info(f"Criando nova tabela: raw.{table_name}")

            # 3. Carga efetiva
            df_prepared.to_sql(
                name=table_name,
                con=self.engine,
                schema='raw',
                if_exists=mode,
                index=False
            )
            logger.info(f"Sucesso: raw.{table_name} carregada com {len(df)} linhas via {mode}.")

        except Exception as e:
            logger.critical(f"Falha no carregamento SQL em {table_name}: {e}")
            raise