from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import datetime

class RocketSchema(BaseModel):
    id: str = Field(..., alias="id")
    name: str
    type: str
    active: bool
    stages: int
    cost_per_launch: int
    success_rate_pct: int
    height_m: float
    mass_kg: float

class LaunchSchema(BaseModel):
    id: str
    name: str
    date_utc: datetime
    success: Optional[bool] = None
    rocket: str
    flight_number: int
    details: Optional[str] = None
    # Verifique se o campo da API é 'payloads'. Se for, use o alias:
    payloads: List[str] = Field(default_factory=list) 

    class Config:
        # Isso ignora campos extras que venham da API mas não estejam no Schema
        extra = "ignore"


class LaunchpadSchema(BaseModel):
    id: str = Field(..., alias="id")
    name: str
    region: str
    status: str

class PayloadSchema(BaseModel):
    id: str = Field(..., alias="id")
    name: str
    type: str
    mass_kg: float
    orbit: str
    reused: bool

# src/extract/schemas.py

ENDPOINT_SCHEMAS = {
    "launches": LaunchSchema,
    "rockets": RocketSchema,
    "launchpads": LaunchpadSchema,
    "payloads": PayloadSchema
}