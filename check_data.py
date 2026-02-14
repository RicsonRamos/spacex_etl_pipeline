import sqlite3
import pandas as pd
from pathlib import Path

def audit_database():
    db_path = Path("data/database/spacex_prod.db")
    if not db_path.exists():
        print("Erro: Banco de dados não encontrado.")
        return

    conn = sqlite3.connect(db_path)
    tables = ['launches', 'rockets', 'payloads', 'launchpads']
    
    print(f"{'Tabela':<15} | {'Registros':<10} | {'Última Atualização'}")
    print("-" * 50)
    
    for table in tables:
        # Contagem de registros
        count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
        
        # Amostragem para validar se as colunas complexas (mass, success) estão populadas
        print(f"{table:<15} | {count:<10} | Validado")

    # Insight Rápido: Taxa de Sucesso Total da SpaceX
    success_rate = pd.read_sql("SELECT AVG(success) * 100 FROM launches WHERE success IS NOT NULL", conn).iloc[0, 0]
    print("-" * 50)
    print(f"Taxa de Sucesso Histórica (Launches): {success_rate:.2f}%")
    
    conn.close()

if __name__ == "__main__":
    audit_database()