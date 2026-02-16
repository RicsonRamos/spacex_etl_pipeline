import pytest
from sqlalchemy import create_engine, text
from src.config.settings import settings

def test_database_connection():
    """Valida se o motor do ETL consegue alcançar o banco na rede interna."""
    # O segredo aqui é garantir que 'settings.DATABASE_URL' 
    # use o hostname definido no docker-compose (ex: db)
    engine = create_engine(settings.DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1
    except Exception as e:
        pytest.fail(f"Falha de conexão: Verifique se o host do banco está correto no .env. Erro: {e}")