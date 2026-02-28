# alembic/env.py
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys

# Adiciona src ao sys.path para importar Base e settings
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.db.models_factory import Base  # Base de todos os modelos
from src.config.settings import settings

# Configuração Alembic
config = context.config

# Usa DATABASE_URL do settings
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

# Configura logging
fileConfig(config.config_file_name)

# Metadata alvo (Base dos modelos SQLAlchemy)
target_metadata = Base.metadata


def run_migrations_offline():
    """Executa migração sem conexão (gera SQL)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Executa migração com conexão ativa."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,  # detecta mudanças de tipo de coluna
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()