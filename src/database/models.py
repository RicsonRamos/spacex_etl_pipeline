from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """
    Base class for SQLAlchemy models.
    Using DeclarativeBase is the recommended approach for SQLAlchemy 2.0.
    """
    pass


class Rocket(Base):
    __tablename__ = "rockets"

    rocket_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=True)
    active = Column(Boolean, nullable=True)
    stages = Column(Integer, nullable=True)
    cost_per_launch = Column(Float, nullable=True)
    success_rate_pct = Column(Float, nullable=True)

    # Relationship: One Rocket can have many Launches
    launches = relationship("Launch", back_populates="rocket")


class Launchpad(Base):
    __tablename__ = "launchpads"

    launchpad_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    full_name = Column(String, nullable=True)
    locality = Column(String, nullable=True)
    region = Column(String, nullable=True)
    status = Column(String, nullable=True)

    # Relationship: One Launchpad can have many Launches
    launches = relationship("Launch", back_populates="launchpad")


class Launch(Base):
    __tablename__ = "launches"

    launch_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    date_utc = Column(DateTime, nullable=False)
    success = Column(Boolean, nullable=True)
    flight_number = Column(Integer, nullable=True)
    launch_year = Column(Integer, nullable=True)

    # Foreign Keys
    rocket_id = Column(String, ForeignKey("rockets.rocket_id"), nullable=False)
    launchpad_id = Column(String, ForeignKey("launchpads.launchpad_id"), nullable=False)

    # Relationships: backrefs to Rocket and Launchpad
    rocket = relationship("Rocket", back_populates="launches")
    launchpad = relationship("Launchpad", back_populates="launches")
