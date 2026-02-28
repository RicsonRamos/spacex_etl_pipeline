# src/db/session.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config.settings import settings

# Engine global
engine = create_engine(settings.DATABASE_URL, future=True, echo=False)

# Session factory
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)