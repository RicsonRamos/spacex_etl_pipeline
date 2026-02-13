import pandas as pd
from sqlalchemy import create_engine, text
from src.config.config import settings

def validate():
    # Usamos o mesmo engine do Loader para garantir o mesmo caminho
    engine = create_engine(settings.DATABASE_URL)
    
    print("\n" + "="*50)
    print("📊 RELATÓRIO DE VALIDAÇÃO DE ENGENHARIA")
    print("="*50)

    try:
        with engine.connect() as conn:
            # 1. Verificar quais tabelas realmente existem
            existing_tables = pd.read_sql(
                text("SELECT name FROM sqlite_master WHERE type='table'"), 
                conn
            )['name'].tolist()
            
            print(f"📂 Tabelas encontradas no DB: {existing_tables}\n")

            # 2. Contagem de Registros apenas para tabelas existentes
            for t in ['rockets', 'launches', 'ships']:
                if t in existing_tables:
                    count = pd.read_sql(text(f"SELECT COUNT(*) as total FROM {t}"), conn).iloc[0]['total']
                    print(f"✅ {t.upper()}: {count} registros.")
                else:
                    print(f"❌ {t.upper()}: Tabela não encontrada!")

            # 3. JOIN de Integridade (Rockets + Launches)
            if 'rockets' in existing_tables and 'launches' in existing_tables:
                query = """
                SELECT 
                    l.name as lancamento,
                    r.name as foguete,
                    l.date_utc
                FROM launches l
                JOIN rockets r ON l.rocket_id = r.rocket_id
                ORDER BY l.date_utc DESC
                LIMIT 5
                """
                print("\n🔍 INTEGRIDADE DE RELACIONAMENTO (JOIN):")
                df_join = pd.read_sql(text(query), conn)
                print(df_join if not df_join.empty else "Nenhum dado relacionado encontrado.")

    except Exception as e:
        print(f"💥 Erro ao validar: {e}")

if __name__ == "__main__":
    validate()