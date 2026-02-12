import pandas as pd
import numpy as np
from src.logger import setup_logger

class SpaceXTransformer:
    def __init__(self):
        self.logger = setup_logger("SpaceXTransformer")

    def _safe_get(self, obj, *keys):
        for k in keys:
            if not isinstance(obj, dict):
                return None
            obj = obj.get(k)
        return obj

    def _clean_for_sql(self, df):
        """Converte NaNs do Pandas para None do Python (NULL no SQL)."""
        # Substitui NaN/NaT por None para compatibilidade com o banco
        return df.replace({np.nan: None}).to_dict(orient="records")

    def transform_rockets(self, raw_rockets):
        rockets, payloads, images, engines = [], [], [], []

        for r in raw_rockets:
            r_id = r.get("id")

            # ROCKETS
            rockets.append({
                "rocket_id": r_id,
                "name": r.get("name"),
                "type": r.get("type"),
                "active": 1 if r.get("active") else 0,
                "stages": r.get("stages"),
                "boosters": r.get("boosters"),
                "cost_per_launch": r.get("cost_per_launch"),
                "success_rate_pct": r.get("success_rate_pct"),
                "first_flight": r.get("first_flight"),
                "country": r.get("country"),
                "company": r.get("company"),
                "wikipedia": r.get("wikipedia"),
                "description": r.get("description"),
                "height_m": self._safe_get(r, "height", "meters"),
                "diameter_m": self._safe_get(r, "diameter", "meters"),
                "mass_kg": self._safe_get(r, "mass", "kg"),
            })

            # PAYLOADS
            for p in r.get("payload_weights", []):
                payloads.append({
                    "rocket_id": r_id,
                    "orbit": p.get("id"),
                    "kg": p.get("kg"),
                    "lb": p.get("lb")
                })

            # IMAGES
            for url in r.get("flickr_images", []):
                images.append({"rocket_id": r_id, "url": url})

            # ENGINES
            e = r.get("engines", {})
            engines.append({
                "rocket_id": r_id,
                "type": e.get("type"),
                "version": e.get("version"),
                "layout": e.get("layout"),
                "number": e.get("number"),
                # CORRIGIDO: thrust_sl_kn (sem o erro 'thust')
                "thrust_sl_kn": self._safe_get(e, "thrust_sea_level", "kN"),
                "thrust_vac_kn": self._safe_get(e, "thrust_vacuum", "kN"),
                "isp_sl": self._safe_get(e, "isp", "sea_level"),
                "isp_vac": self._safe_get(e, "isp", "vacuum"),
                "propellant_1": e.get("propellant_1"),
                "propellant_2": e.get("propellant_2"),
            })

        # Processamento de tipos com Pandas
        df_rockets = pd.DataFrame(rockets)
        df_engines = pd.DataFrame(engines)
        
        # Datas e Números
        df_rockets["first_flight"] = pd.to_datetime(df_rockets["first_flight"], errors="coerce").dt.date
        
        # Sanitização final: Retornar listas de dicionários limpas para o Loader
        self.logger.info("Transformação concluída. Sanitizando para carga SQL...")
        
        return {
            "rockets": self._clean_for_sql(df_rockets),
            "payloads": self._clean_for_sql(pd.DataFrame(payloads)),
            "images": self._clean_for_sql(pd.DataFrame(images)),
            "engines": self._clean_for_sql(df_engines)
        }
    def transform_launches(self, raw_launches):
        launches = []
        for l in raw_launches:
            launches.append({
                "launch_id": l.get("id"),
                "name": l.get("name"),
                "date_utc": l.get("date_utc"),
                "rocket_id": l.get("rocket"),
                "success": 1 if l.get("success") else 0,
                "flight_number": l.get("flight_number"),
                "details": l.get("details"),
                "webcast": self._safe_get(l, "links", "webcast"),
                "reused": 1 if self._safe_get(l, "fairings", "reused") else 0
            })
        
        df_launches = pd.DataFrame(launches)
        # Converter para datetime real do Python para o SQLAlchemy não reclamar
        df_launches["date_utc"] = pd.to_datetime(df_launches["date_utc"]).dt.to_pydatetime()
        
        return self._clean_for_sql(df_launches)