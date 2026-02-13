import pandas as pd
from sqlalchemy import create_engine, text
from src.config.config import settings

def validate_data():
    try:
        # Cria a conexão usando a string do seu .env
        engine = create_engine(settings.DATABASE_URL)
        
        query = text("""
        SELECT 
            r.name AS Foguete,
            COUNT(l.launch_id) AS Total_Lancamentos,
            SUM(CASE WHEN l.success = 1 THEN 1 ELSE 0 END) AS Sucessos,
            ROUND(AVG(CAST(l.success AS FLOAT)) * 100, 2) AS Taxa_Sucesso
        FROM rockets r
        LEFT JOIN launches l ON r.rocket_id = l.rocket_id
        GROUP BY r.name
        ORDER BY Total_Lancamentos DESC;
        """)
        
        # Executa a query e carrega em um DataFrame
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        print("\n" + "="*50)
        print("📊 RELATÓRIO DE INTEGRIDADE DOS DADOS (SpaceX)")
        print("="*50)
        
        if df.empty:
            print("❌ Erro: O banco de dados retornou zero registros.")
        else:
            print("✅ Dados cruzados com sucesso entre Rockets e Launches:")
            print("-" * 50)
            print(df.to_string(index=False))
            
        print("="*50 + "\n")

    except Exception as e:
        print(f"❌ Falha ao validar banco de dados: {e}")

if __name__ == "__main__":
    validate_data()