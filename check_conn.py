import psycopg2
from src.config.settings import settings

try:
    print(f"Tentando conectar em: {settings.DATABASE_URL}")
    conn = psycopg2.connect(settings.DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print(f"Sucesso! Versão do banco: {cur.fetchone()}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"ERRO CRÍTICO: Não foi possível conectar ao banco.\nDetalhe: {e}")