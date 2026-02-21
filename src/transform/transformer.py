import polars as pl
import structlog
from typing import List, Dict, Any
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()

class SpaceXTransformer:
    def transform(
        self,
        endpoint: str,
        data: List[Dict[str, Any]]
    ) -> pl.DataFrame:

        if not data:
            logger.warning("Dataset vazio para transformação", endpoint=endpoint)
            return pl.DataFrame()

        # Validação de Registro: Garante que o endpoint está mapeado
        schema = SCHEMA_REGISTRY.get(endpoint)
        if not schema:
            logger.error("Endpoint não mapeado no SCHEMA_REGISTRY", endpoint=endpoint)
            raise ValueError(f"Endpoint sem schema definido: {endpoint}")

        try:
            # Converte lista de dicts para Polars DataFrame
            df = pl.from_dicts(data)

            # 1. RENAME: Fundamental para converter 'id' da API na PK definida (ex: 'launch_id')
            if schema.rename:
                # Filtra apenas colunas que de fato existem no DF para evitar KeyError
                rename_map = {k: v for k, v in schema.rename.items() if k in df.columns}
                df = df.rename(rename_map)

            # 2. CASTS: Converte tipos (ex: String para Datetime)
            for col, dtype in schema.casts.items():
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).cast(dtype, strict=False)
                    )

            # 3. SELEÇÃO DE COLUNAS: Garante que apenas o definido no contrato siga adiante
            available_cols = [c for c in schema.columns if c in df.columns]
            df = df.select(available_cols)

            # 4. PK VALIDATION & DEDUPLICATION
            if schema.pk not in df.columns:
                logger.error("Chave Primária ausente após transformação", pk=schema.pk)
                raise ValueError(f"PK '{schema.pk}' não encontrada no DataFrame transformado.")

            # Remove duplicatas baseadas na PK
            df = df.unique(subset=[schema.pk])

            logger.info("Transformação concluída", endpoint=endpoint, rows=df.height)
            return df

        except Exception as e:
            logger.exception("Falha crítica na transformação", endpoint=endpoint, error=str(e))
            raise
