from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List

class BaseSchema(BaseModel):
    """Configuração base para garantir compatibilidade com aliases e ignorar lixo da API."""
    model_config = ConfigDict(
        from_attributes=True, 
        populate_by_name=True, 
        extra="ignore"
    )

class RocketSchema(BaseSchema):
    rocket_id: str = Field(..., alias="id")
    name: str
    type: str
    active: bool
    stages: Optional[int] = None
    cost_per_launch: Optional[float] = None
    success_rate_pct: Optional[float] = None

class LaunchpadSchema(BaseSchema):
    launchpad_id: str = Field(..., alias="id")
    full_name: str
    status: str
    locality: str
    region: str
    # IDs dos foguetes que operam nesta base
    rockets: List[str] = []

class LaunchSchema(BaseSchema):
    launch_id: str = Field(..., alias="id")
    name: str
    # Rigor: Mantido como str para evitar conflito com o Transformer.str.to_datetime() do Polars
    date_utc: str 
    success: Optional[bool] = None
    flight_number: int
    rocket_id: str = Field(..., alias="rocket")
    launchpad_id: str = Field(..., alias="launchpad")

class CrewSchema(BaseSchema):
    """O 4º endpoint comum em dados da SpaceX para fechar o ciclo básico."""
    crew_id: str = Field(..., alias="id")
    name: str
    agency: str
    status: str
    image: Optional[str] = None

ENDPOINT_SCHEMAS = {
    "rockets": RocketSchema,
    "launches": LaunchSchema,
    "launchpads": LaunchpadSchema,
    "crew": CrewSchema
}