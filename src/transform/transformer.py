import polars as pl
from src.config.schema_registry import SCHEMA_REGISTRY

class SpaceXTransformer:
    def transform(self, endpoint: str, data: List[Dict]) -> pl.DataFrame:
        if not data: return pl.DataFrame()
        
        schema = SCHEMA_REGISTRY.get(endpoint)
        if not schema: raise ValueError(f"Schema ausente: {endpoint}")

        df = pl.from_dicts(data)

        # 1. Rename imediato para garantir que PK exista com o nome correto
        if schema.rename:
            df = df.rename({k: v for k, v in schema.rename.items() if k in df.columns})

        # 2. Casts robustos
        for col, dtype in schema.casts.items():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(dtype, strict=False))

        # 3. Drop de duplicatas pela PK real
        return df.unique(subset=[schema.pk])
