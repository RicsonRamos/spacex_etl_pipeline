import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

class PostgresLoader:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.db_url)

    def _serialize_complex_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Serializa listas e dicts para JSON para evitar erro de tipo no Postgres."""
        df = df.copy()
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                logger.info(f"Serializando coluna complexa: {col}")
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
        return df

    def load_bronze(self, df: pd.DataFrame, table_name: str):
        """
        Carga na camada Bronze.
        Rigor: Garante existência do schema, colunas de auditoria e preserva Views do dbt.
        """
        try:
            # 1. RIGOR: Garantir que o schema 'raw' existe (Auto-preparação do ambiente)
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
                conn.commit()
                logger.info(f"Schema 'raw' verificado/criado para {table_name}.")

            # 2. Metadado de Observabilidade (Certidão de nascimento do dado)
            df['loaded_at'] = datetime.now() 
            
            # 3. Preparação dos dados
            df_prepared = self._serialize_complex_columns(df)

            # 4. Lógica de Idempotência (Truncate vs Replace)
            inspector = inspect(self.engine)
            table_exists = inspector.has_table(table_name, schema='raw')

            if table_exists:
                # Se a tabela existe, limpa os dados mas mantém a estrutura para o dbt
                with self.engine.connect() as conn:
                    conn.execute(text(f'TRUNCATE TABLE raw."{table_name}"'))
                    conn.commit()
                mode = 'append'
                logger.info(f"Tabela raw.{table_name} truncada.")
            else:
                # Se não existe, cria a tabela do zero
                mode = 'replace'
                logger.info(f"Criando tabela raw.{table_name} pela primeira vez.")

            # 5. Carga Final
            df_prepared.to_sql(
                name=table_name,
                con=self.engine,
                schema='raw',
                if_exists=mode,
                index=False
            )
            logger.info(f"Sucesso: raw.{table_name} carregada ({len(df)} linhas) via {mode}.")

        except Exception as e:
            logger.critical(f"Falha no carregamento SQL em {table_name}: {e}")
            raise