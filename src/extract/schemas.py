from typing import Optional, List
from pydantic import BaseModel, ConfigDict, Field

class BaseAPISchema(BaseModel):
    # Configuração rigorosa: Se o campo não estiver aqui, ele não passa para a Bronze.
    model_config = ConfigDict(from_attributes=True, extra="ignore")

class RocketAPI(BaseAPISchema):
    id: str
    name: str
    active: bool
    stages: int
    boosters: int
    cost_per_launch: int
    success_rate_pct: float
    first_flight: str
    country: str
    company: str
    description: Optional[str] = None
    wikipedia: Optional[str] = None

class LaunchAPI(BaseAPISchema):
    id: str
    name: str
    date_utc: str
    success: Optional[bool] = None
    rocket: str
    details: Optional[str] = None

# Mapeamento centralizado para o BaseExtractor
API_SCHEMAS = {
    "rockets": RocketAPI,
    "launches": LaunchAPI
}
