from datetime import datetime
from sqlalchemy import String, Float, Boolean, DateTime, JSON, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class BronzeLaunch(Base):
    __tablename__ = "bronze_launches"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    raw_data: Mapped[dict] = mapped_column(JSON)
    extracted_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

class SilverRocket(Base):
    __tablename__ = "silver_rockets"
    rocket_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean)
    cost_per_launch: Mapped[float] = mapped_column(Float, nullable=True)