import pandas as pd

from src.logger import setup_logger


class SpaceXTransformer:

    def __init__(self):

        self.logger = setup_logger(
            "SpaceXTransformer",
            "transformer.log"
        )

    def _safe_get(self, obj, *keys):

        for k in keys:
            if obj is None:
                return None
            obj = obj.get(k)
        return obj

    def transform_rockets(self, raw_rockets):

        rockets = []
        payloads = []
        images = []
        engines = []

        for r in raw_rockets:

            rocket_id = r.get("id")

            
            # ROCKETS
            
            rockets.append({
                "rocket_id": rocket_id,
                "name": r.get("name"),
                "type": r.get("type"),
                "active": int(r.get("active", 0)),

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
                    "rocket_id": rocket_id,
                    "orbit": p.get("id"),
                    "kg": p.get("kg"),
                    "lb": p.get("lb")
                })

            
            # IMAGES
            
            for url in r.get("flickr_images", []):

                images.append({
                    "rocket_id": rocket_id,
                    "url": url
                })

            
            # ENGINES
            
            e = r.get("engines", {})

            engines.append({
                "rocket_id": rocket_id,

                "type": e.get("type"),
                "version": e.get("version"),
                "layout": e.get("layout"),

                "number": e.get("number"),

                "thrust_sl_kn": self._safe_get(e, "thrust_sea_level", "kN"),
                "thrust_vac_kn": self._safe_get(e, "thrust_vacuum", "kN"),

                "isp_sl": self._safe_get(e, "isp", "sea_level"),
                "isp_vac": self._safe_get(e, "isp", "vacuum"),

                "propellant_1": e.get("propellant_1"),
                "propellant_2": e.get("propellant_2"),
            })

        self.logger.info(
            f"Transformed {len(rockets)} rockets, "
            f"{len(payloads)} payloads, "
            f"{len(images)} images and "
            f"{len(engines)} engines"
        )

        
        # DATAFRAMES
        

        df_rockets = pd.DataFrame(rockets)
        df_payloads = pd.DataFrame(payloads)
        df_images = pd.DataFrame(images)
        df_engines = pd.DataFrame(engines)

        
        # TYPES
        

        df_rockets["active"] = df_rockets["active"].fillna(0).astype(int)

        df_rockets["first_flight"] = (
            pd.to_datetime(df_rockets["first_flight"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
        )

        numeric_cols = [
            "stages",
            "boosters",
            "cost_per_launch",
            "success_rate_pct",
            "height_m",
            "diameter_m",
            "mass_kg"
        ]

        for col in numeric_cols:
            df_rockets[col] = pd.to_numeric(
                df_rockets[col],
                errors="coerce"
            )

        df_payloads[["kg", "lb"]] = df_payloads[["kg", "lb"]].apply(
            pd.to_numeric,
            errors="coerce"
        )

        df_engines["number"] = pd.to_numeric(
            df_engines["number"],
            errors="coerce"
        )

        df_engines[[
            "thrust_sl_kn",
            "thrust_vac_kn",
            "isp_sl",
            "isp_vac"
        ]] = df_engines[[
            "thrust_sl_kn",
            "thrust_vac_kn",
            "isp_sl",
            "isp_vac"
        ]].apply(
            pd.to_numeric,
            errors="coerce"
        )

        self.logger.info("DataFrames created with normalized types")

        return {
            "rockets": df_rockets,
            "payloads": df_payloads,
            "images": df_images,
            "engines": df_engines
        }
