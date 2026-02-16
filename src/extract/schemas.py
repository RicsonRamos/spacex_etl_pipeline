from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime


class RocketSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    rocket_id: str = Field(..., alias="id")
    name: str
    type: str
    active: bool


class LaunchSchema(BaseModel):
    """Strict schema for Launches."""

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )

    launch_id: str = Field(..., alias="id")
    name: str
    date_utc: datetime
    success: Optional[bool] = None
    flight_number: int
    rocket: str
    launchpad: str

    @property
    def launch_year(self) -> int:
        return self.date_utc.year


class LaunchpadSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    launchpad_id: str = Field(..., alias="id")
    name: str
    locality: str
    region: str

ENDPOINT_SCHEMAS = {
    "rockets": RocketSchema,
    "launches": LaunchSchema,
    "launchpads": LaunchpadSchema,
    # Adicione outros se houver
}