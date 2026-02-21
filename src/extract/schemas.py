from pydantic import BaseModel, ConfigDict
from typing import Optional, List


class BaseAPISchema(BaseModel):

    model_config = ConfigDict(
        from_attributes=True,
        extra="forbid"   # explode se mudar
    )


# -------------------------
# RAW API MODELS
# -------------------------

class RocketAPI(BaseAPISchema):

    id: str
    name: str
    type: str
    active: bool

    stages: Optional[int] = None
    cost_per_launch: Optional[int] = None
    success_rate_pct: Optional[int] = None


class LaunchpadAPI(BaseAPISchema):

    id: str
    full_name: str
    status: str
    locality: str
    region: str

    rockets: List[str] = []


class LaunchAPI(BaseAPISchema):

    id: str
    name: str
    date_utc: str

    success: Optional[bool] = None
    flight_number: int

    rocket: str
    launchpad: str


class CrewAPI(BaseAPISchema):

    id: str
    name: str
    agency: str
    status: str

    image: Optional[str] = None


# -------------------------
# REGISTRY
# -------------------------

API_SCHEMAS = {
    "rockets": RocketAPI,
    "launches": LaunchAPI,
    "launchpads": LaunchpadAPI,
    "crew": CrewAPI,
}