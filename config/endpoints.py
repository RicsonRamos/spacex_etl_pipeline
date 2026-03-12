import os
from datetime import datetime, timedelta
from src.utils.logger import get_logger

logger = get_logger(__name__)

def get_endpoints_config():
    """
    Protocolo de Sincronia: Alinha a janela da NASA com o teto da base SpaceX.
    Rigor: Evita o anacronismo identificado na auditoria anterior.
    """
    
    # ESTRATÉGIA DE ALINHAMENTO:
    # Como a base SpaceX enviada termina em Dez/2022, vamos buscar a NASA 
    # para o mesmo período histórico para garantir que o JOIN na camada Gold funcione.
    
    # Se quiser dados atuais, mantenha o datetime.now(). 
    # Para o SEU caso específico de teste, vamos usar o teto de 2022:
    target_date = datetime(2022, 12, 31) 
    start_date = (target_date - timedelta(days=60)).strftime('%Y-%m-%d')
    end_date = target_date.strftime('%Y-%m-%d')

    nasa_key = os.getenv("NASA_API_KEY", "DEMO_KEY")
    
    if nasa_key == "DEMO_KEY":
        logger.warning("MODO CRÍTICO: Usando DEMO_KEY. Limites de taxa iminentes.")

    return {
        "spacex_rockets": {
            "url": "https://api.spacexdata.com/v4/rockets",
            "layer": "bronze",
            "params": None
        },
        
        "spacex_launches": {
            "url": "https://api.spacexdata.com/v4/launches", # v4 é a mais estável
            "layer": "bronze",
            "params": None
        },

        "spacex_payloads": {
            "url": "https://api.spacexdata.com/v4/payloads",
            "layer": "bronze",
            "params": None
        },

        "spacex_cores": {
            "url": "https://api.spacexdata.com/v4/cores",
            "layer": "bronze",
            "params": None
        },

        "nasa_solar_events": {
            "url": "https://api.nasa.gov/DONKI/CME",
            "layer": "bronze",
            "params": {
                "api_key": nasa_key,
                "startDate": start_date,  # Agora sincronizado com o fim da base SpaceX
                "endDate": end_date
            }
        }
    }