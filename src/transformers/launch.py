import polars as pl
from src.transformers.base import BaseTransformer

class LaunchTransformer(BaseTransformer):
    def __init__(self, columns_schema: list[str]):
        super().__init__(config={
            "name": "launches",
            "pk": "launch_id",
            "date_column": "date_utc",
            "columns": columns_schema,
            "rename": {
                "id": "launch_id",
                "rocket": "rocket_id"
            },
            "casts": {
                "success": pl.Boolean,
                "rocket_id": pl.String,
                "details": pl.String
            }
        })
