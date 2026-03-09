import os
from datetime import datetime, timedelta

# Rigor: Buscamos os últimos 30 dias para garantir sobreposição com lançamentos recentes
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')


ENDPOINTS_CONFIG = {
    # ATIVOS: Para saber o custo fixo e capacidade de carga
    "spacex_rockets": {
        "url": "https://api.spacexdata.com/v4/rockets",
        "layer": "bronze"
    },
    
    # EXECUÇÃO: Para saber o que foi faturado e o sucesso da missão
    "spacex_launches": {
        "url": "https://api.spacexdata.com/v4/launches",
        "layer": "bronze"
    },

    # CARGA: Para calcular o "Ticket Médio" (Custo/kg)
    "spacex_payloads": {
        "url": "https://api.spacexdata.com/v4/payloads",
        "layer": "bronze"
    },

    # CICLO DE VIDA: Para medir o ROI de reuso (Profitability)
    "spacex_cores": {
        "url": "https://api.spacexdata.com/v4/cores",
        "layer": "bronze"
    },

    "nasa_solar_events": {
        "url": "https://api.nasa.gov/DONKI/CME",
        "params": {
            "api_key": os.getenv("NASA_API_KEY"),
            "startDate": start_date,
            "endDate": end_date
        },
        "layer": "bronze"
    }
}