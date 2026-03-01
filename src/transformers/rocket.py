import polars as pl
from src.transformers.base import BaseTransformer

class RocketTransformer(BaseTransformer):
    def __init__(self, columns_schema: list[str]):
        super().__init__(config={
            "name": "rockets",
            "pk": "rocket_id",
            "date_column": "first_flight",
            "columns": columns_schema,
            "rename": {
                "id": "rocket_id",
                "name": "rocket_name"
            },
            "casts": {
                "active": pl.Boolean,
                "stages": pl.Int32,
                "boosters": pl.Int32,
                "cost_per_launch": pl.Int64,
                "success_rate_pct": pl.Float64,
                "country": pl.String
            }
        })
