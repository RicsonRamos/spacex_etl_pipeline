import polars as pl
import structlog
from typing import List, Dict, Any

logger = structlog.get_logger()

class SpaceXTransformer:
    def transform(self, endpoint: str, data: List[Dict[str, Any]]) -> pl.DataFrame:
        if not data:
            logger.warning("Nenhum dado recebido para transformação", endpoint=endpoint)
            return pl.DataFrame()
            
        df = pl.from_dicts(data)
        
        # Padronização de PK
        endpoint_pk_map = {
            "launches": "launch_id",
            "rockets": "rocket_id",
            "launchpads": "launchpad_id"
        }
        if "id" in df.columns:
            pk_name = endpoint_pk_map.get(endpoint, f"{endpoint.rstrip('s')}_id")
            df = df.rename({"id": pk_name})
        
        # Dispatcher baseado em mapeamento
        dispatch_map = {
            "launches": self._transform_launches,
            "rockets": self._transform_rockets,
            "launchpads": self._transform_launchpads
        }
        
        transform_func = dispatch_map.get(endpoint)
        if transform_func:
            return transform_func(df)
            
        return df

    def _transform_launches(self, df: pl.DataFrame) -> pl.DataFrame:
        # Otimização: Fazemos o parse UMA VEZ e derivamos as colunas
        return df.with_columns([
            pl.col("date_utc").str.strptime(
                pl.Datetime,
                format="%Y-%m-%dT%H:%M:%S%.fZ",
                strict=False
            ).dt.replace_time_zone("UTC")
        ]).with_columns([
            pl.col("date_utc").dt.year().alias("launch_year")
        ])

    def _transform_rockets(self, df: pl.DataFrame) -> pl.DataFrame:
        # Uso de select dinâmico para evitar quebra se a coluna faltar (opcional)
        # Mas aqui mantemos rigoroso: se a coluna é essencial para Gold, ela deve existir.
        return df.select([
            pl.col("rocket_id"),
            pl.col("name"),
            pl.col("active").cast(pl.Boolean),
            pl.col("cost_per_launch").cast(pl.Int64),
            pl.col("success_rate_pct").cast(pl.Float64)
        ])

    def _transform_launchpads(self, df: pl.DataFrame) -> pl.DataFrame:
        # Exemplo de tratamento para colunas que sabemos serem Listas Complexas
        # Isso facilita o trabalho do Loader que fará o json.dumps
        return df.select([
            pl.col("launchpad_id"),
            pl.col("full_name"),
            pl.col("status"),
            pl.col("rockets") # List[str]
        ])