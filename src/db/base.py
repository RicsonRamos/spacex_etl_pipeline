# src/db/base.py
from sqlalchemy.orm import declarative_base

# Base de todos os modelos, usada para gerar tabelas via Alembic
Base = declarative_base()