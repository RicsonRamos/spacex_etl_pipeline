from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class ETLMetrics(Base):
    __tablename__ = "etl_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(255), nullable=False)
    stage = Column(String(50), nullable=False)  # "staging" or "final"
    rows_processed = Column(Integer)
    status = Column(String(50))  # "success" or "failure"
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    error = Column(Text)  # Store any error message in case of failure
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<ETLMetrics(id={self.id}, table_name={self.table_name}, stage={self.stage}, status={self.status})>"

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

    def __repr__(self):
        return f"<Rocket(rocket_id={self.rocket_id}, name={self.name})>"

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

    def __repr__(self):
        return f"<Launchpad(launchpad_id={self.launchpad_id}, name={self.name})>"

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

    def __repr__(self):
        return f"<Launch(launch_id={self.launch_id}, name={self.name}, date_utc={self.date_utc})>"
