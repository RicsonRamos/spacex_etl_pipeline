from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable
import polars as pl

@dataclass
class EntitySchema:
    api_endpoint: str
    pk: str
    bronze_table: str
    silver_table: str
    columns: Dict[str, str]  # Para o Loader saber o que levar para o SQL
    casts: Dict[str, pl.DataType] = field(default_factory=dict)
    rename: Dict[str, str] = field(default_factory=dict)
    derived: Dict[str, Callable[[pl.DataFrame], pl.Expr]] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    gold_view: Optional[str] = None
    gold_definition: Optional[str] = None

# Registro Unificado (Substitui SCHEMAS e TABLE_REGISTRY)
SCHEMA_REGISTRY: Dict[str, EntitySchema] = {
    
    "launches": EntitySchema(
        api_endpoint="launches",
        pk="launch_id",
        bronze_table="bronze_launches",
        silver_table="silver_launches",
        rename={"id": "launch_id"}, # Resolve o conflito Script 2 (id) vs Script 1 (launch_id)
        columns={
            "launch_id": "string",
            "name": "string",
            "date_utc": "timestamp",
            "success": "boolean",
            "rocket": "string"
        },
        casts={
            "date_utc": pl.Datetime,
            "success": pl.Boolean
        },
        required=["launch_id", "rocket"]
    ),

    "rockets": EntitySchema(
        api_endpoint="rockets",
        pk="rocket_id",
        bronze_table="bronze_rockets",
        silver_table="silver_rockets",
        rename={"id": "rocket_id"},
        columns={
            "rocket_id": "string",
            "name": "string",
            "active": "boolean",
            "cost_per_launch": "int64",
            "success_rate_pct": "float64"
        },
        casts={
            "success_rate_pct": pl.Float64, # Resolve o erro int vs float do Pydantic
            "cost_per_launch": pl.Int64
        },
        required=["rocket_id"]
    )
}

# Alias para manter compatibilidade com o Loader que você enviou
TABLE_REGISTRY = {
    k: {
        "bronze": v.bronze_table,
        "silver": v.silver_table,
        "pk": v.pk,
        "columns": v.columns,
        "required": v.required,
        "gold_view": v.gold_view,
        "gold_definition": v.gold_definition
    }
    for k, v in SCHEMA_REGISTRY.items()
}
