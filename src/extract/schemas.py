from pydantic import BaseModel, ConfigDict
from typing import Optional

class BaseAPISchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="ignore")

class RocketAPI(BaseAPISchema):
    id: str
    name: str
    active: bool
    cost_per_launch: Optional[int] = None
    success_rate_pct: Optional[float] = None 

class LaunchAPI(BaseAPISchema):
    id: str
    name: str
    date_utc: str
    success: Optional[bool] = None
    rocket: str

API_SCHEMAS = {
    "rockets": RocketAPI,
    "launches": LaunchAPI
}
