import polars as pl
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class EntitySchema:
    api_endpoint: str
    pk: str
    bronze_table: str
    silver_table: str
    columns: list[str]
    rename: Dict[str, str] = field(default_factory=dict)
    casts: Dict[str, pl.DataType] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)

SCHEMA_REGISTRY: Dict[str, EntitySchema] = {
    "launches": EntitySchema(
        api_endpoint="launches",
        pk="launch_id",
        bronze_table="bronze_launches",
        silver_table="silver_launches",
        rename={"id": "launch_id"}, # API 'rocket' já mapeia para DB 'rocket'
        columns=["launch_id", "name", "date_utc", "success", "rocket"], 
        casts={
            "date_utc": pl.Datetime, 
            "success": pl.Boolean,
            "name": pl.String,
            "rocket": pl.String
        },
        required=["launch_id", "rocket"]
    ),
    "rockets": EntitySchema(
        api_endpoint="rockets",
        pk="rocket_id",
        bronze_table="bronze_rockets",
        silver_table="silver_rockets",
        rename={"id": "rocket_id"},
        columns=["rocket_id", "name", "active", "cost_per_launch", "success_rate_pct"],
        casts={
            "success_rate_pct": pl.Float64, 
            "cost_per_launch": pl.Int64,
            "active": pl.Boolean,
            "name": pl.String
        },
        required=["rocket_id"]
    )
}

# Compatibilidade com Loader legado
TABLE_REGISTRY = {
    k: {
        "bronze": v.bronze_table,
        "silver": v.silver_table,
        "pk": v.pk,
        "columns": {c: "string" for c in v.columns},
        "required": v.required
    } for k, v in SCHEMA_REGISTRY.items()
}
