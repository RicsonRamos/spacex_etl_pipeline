import pytest
from sqlalchemy import text

def test_gold_cost_efficiency_calculation(db_connection): # Use o nome exato da fixture do conftest
    """
    RIGOR: Valida o cálculo de KPI usando o schema real e isolamento de dados.
    """
    loader = db_connection
    
    # 1. Setup: Inserção de dados nas tabelas SILVER
    with loader.engine.begin() as conn:
        # Nota: Incluímos colunas que costumam ser obrigatórias para evitar IntegrityError
        conn.execute(text("""
            INSERT INTO silver_rockets (rocket_id, name, cost_per_launch) 
            VALUES ('r1', 'Big Rocket', 100000000);
        """))
        
        # Adicionado launchpad_id fake caso sua FK exija
        conn.execute(text("""
            INSERT INTO silver_launchpads (launchpad_id, name) 
            VALUES ('lp1', 'Test Pad');
        """))

        conn.execute(text("""
            INSERT INTO silver_launches (launch_id, name, date_utc, rocket_id, launchpad_id, success) 
            VALUES ('l1', 'Test Launch', '2026-01-01', 'r1', 'lp1', true);
        """))
        
        conn.execute(text("""
            INSERT INTO silver_payloads (payload_id, launch_id, mass_kg, name) 
            VALUES ('p1', 'l1', 10000.0, 'Test Payload');
        """))

    # 2. Execução: Atualiza a camada Gold
    loader.refresh_gold_layer()

    # 3. Verificação
    with loader.engine.connect() as conn:
        # Usamos mapeamento por nome para evitar erros de índice de tupla
        result = conn.execute(text("SELECT avg_cost_per_kg FROM gold_cost_efficiency_metrics")).mappings().fetchone()
    
    assert result is not None
    # Cálculo: 100M / 10k kg = 10k per kg
    actual_value = float(result["avg_cost_per_kg"])
    assert actual_value == 10000.0, f"KPI falhou: esperado 10000.0, obtido {actual_value}"