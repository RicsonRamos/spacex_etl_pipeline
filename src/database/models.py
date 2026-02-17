from datetime import datetime
from typing import List, Optional
from sqlalchemy import String, Float, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class ETLMetrics(Base):
    __tablename__ = "etl_metrics"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(255))
    stage: Mapped[str] = mapped_column(String(50))
    rows_processed: Mapped[Optional[int]]
    status: Mapped[Optional[str]] = mapped_column(String(50))
    start_time: Mapped[datetime] = mapped_column(DateTime)
    end_time: Mapped[Optional[datetime]]
    error: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

class Rocket(Base):
    __tablename__ = "silver_rockets"

    rocket_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[Optional[str]]
    active: Mapped[Optional[bool]]
    stages: Mapped[Optional[int]]
    cost_per_launch: Mapped[Optional[float]]
    success_rate_pct: Mapped[Optional[float]]

    launches: Mapped[List["Launch"]] = relationship(back_populates="rocket")

class Launchpad(Base):
    __tablename__ = "silver_launchpads"

    launchpad_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    full_name: Mapped[Optional[str]]
    locality: Mapped[Optional[str]]
    region: Mapped[Optional[str]]
    status: Mapped[Optional[str]]

    launches: Mapped[List["Launch"]] = relationship(back_populates="launchpad")

class Payload(Base):
    __tablename__ = "silver_payloads"

    payload_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    type: Mapped[Optional[str]]
    reused: Mapped[Optional[bool]]
    mass_kg: Mapped[Optional[float]]
    orbit: Mapped[Optional[str]]
    date_created: Mapped[Optional[datetime]]
    year_created: Mapped[Optional[int]]

    # Nota técnica: Payloads na API v4 são ligados a Launches, 
    # mas geralmente um Launch tem múltiplos Payloads.
    launch_id: Mapped[Optional[str]] = mapped_column(ForeignKey("silver_launches.launch_id"))

class Launch(Base):
    __tablename__ = "silver_launches"

    launch_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    date_utc: Mapped[datetime]
    success: Mapped[Optional[bool]]
    flight_number: Mapped[Optional[int]]
    launch_year: Mapped[Optional[int]]

    rocket_id: Mapped[str] = mapped_column(ForeignKey("silver_rockets.rocket_id"))
    launchpad_id: Mapped[str] = mapped_column(ForeignKey("silver_launchpads.launchpad_id"))

    rocket: Mapped["Rocket"] = relationship(back_populates="launches")
    launchpad: Mapped["Launchpad"] = relationship(back_populates="launches")