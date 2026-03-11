import os
from datetime import datetime, timedelta
from src.utils.logger import get_logger

logger = get_logger(__name__)

def get_endpoints_config():
    """
    Retorna a configuração dos endpoints com injeção dinâmica de dependências.
    Rigor: Datas e API Keys são resolvidas em tempo de execução (Runtime).
    """
    
    # Cálculo dinâmico da janela de observação (últimos 30 dias)
    # Isso garante que cada execução da DAG busque dados atualizados.
    today = datetime.now()
    start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    # Recuperação da API Key
    nasa_key = os.getenv("NASA_API_KEY")
    
    if not nasa_key or nasa_key == "DEMO_KEY":
        logger.warning("NASA_API_KEY não detectada ou em modo DEMO. Limites severos aplicados.")

    return {
        # ATIVOS: Capacidade técnica e especificações de hardware
        "spacex_rockets": {
            "url": "https://api.spacexdata.com/v4/rockets",
            "layer": "bronze",
            "params": None
        },
        
        # EXECUÇÃO: Histórico de lançamentos e telemetria básica
        "spacex_launches": {
            "url": "https://api.spacexdata.com/v4/launches",
            "layer": "bronze",
            "params": None
        },

        # CARGA: Detalhes sobre os clientes e massa transportada
        "spacex_payloads": {
            "url": "https://api.spacexdata.com/v4/payloads",
            "layer": "bronze",
            "params": None
        },

        # CICLO DE VIDA: Rastreabilidade de boosters para cálculo de ROI
        "spacex_cores": {
            "url": "https://api.spacexdata.com/v4/cores",
            "layer": "bronze",
            "params": None
        },

        # VARIÁVEL EXTERNA: Monitoramento de clima espacial (DONKI)
        "nasa_solar_events": {
            "url": "https://api.nasa.gov/DONKI/CME",
            "layer": "bronze",
            "params": {
                "api_key": nasa_key,
                "startDate": start_date,
                "endDate": end_date
            }
        }
    }