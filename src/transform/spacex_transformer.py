import pandas as pd

from src.logger import setup_logger


class SpaceXTransformer:

    def __init__(self):

        self.logger = setup_logger(
            'SpaceXTransformer',
            'transformer.log'
        )
    
    def transform_rockets(self, raw_rockets):

        rockets = []
        payloads = []
        images = []
        engines = []

        for r in raw_rockets:

            rocket_id = r["id"]

            #  Rockets Table
            rockets.append({
                "rocket_id": rocket_id,
                "name": r["name"],
                "type": r["type"],
                "active": r["active"],
                "stages": r["stages"],
                "boosters": r["boosters"],
                "cost_per_launch": r["cost_per_launch"],
                "success_rate_pct": r["success_rate_pct"],
                "first_flight": r["first_flight"],
                "country": r["country"],
                "company": r["company"],
                "wikipedia": r["wikipedia"],
                "description": r["description"],

                "height_m": r["height"]["meters"],
                'diameter_m': r["diameter"]["meters"],
                "mass_kg": r["mass"]["kg"]
            })

            # Payloads Table
            for p in r["payload_weights"]:
                payloads.append({
                    "payload_id": f"{rocket_id}_{p['id']}",
                    "rocket_id": rocket_id,
                    "name": p["name"],
                    "kg": p["kg"],
                    "lb": p["lb"]
                })

            # Images Table
            for i in r["flickr_images"]:
                images.append({
                    "rocket_id": rocket_id,
                    "url": i            
                })

            # Engines Table
            
            e = r["engines"]
            engines.append({
                "rocket_id": rocket_id,
                'type': e["type"],
                'number': e["number"],
                'version': e["version"],
                'layout': e["layout"],

                'thust_sea_level_kN': e["thrust_sea_level"]["kN"],
                'thust_vacuum_kN': e["thrust_vacuum"]["kN"],

                'isp_sl': e["isp"]["sea_level"],
                'isp_vacuum': e["isp"]["vacuum"],

                'propellant_1': e["propellant_1"],
                'propellant_2': e["propellant_2"],
            })

        self.logger.info(f"Transformed {len(rockets)} rockets, {len(payloads)} payloads, {len(images)} images and {len(engines)} engines")

        # DataFrames

        df_rockets = pd.DataFrame(rockets)
        df_payloads = pd.DataFrame(payloads)
        df_images = pd.DataFrame(images)
        df_engines = pd.DataFrame(engines)

        # Types
        df_rockets = df_rockets.astype({
            "rocket_id": "string",
            "name": "string",
            "type": "string",
            "active": "boolean",
            "stages": "int",
            "boosters": "int",
            "cost_per_launch": "int",
            "success_rate_pct": "int",
            "first_flight": "datetime64[ns]",
            "country": "string",
            "company": "string",
            "wikipedia": "string",
            "description": "string",

            'height_m': 'float',
            'diameter_m': 'float',
            'mass_kg': 'float'
        })

        df_payloads = df_payloads.astype({
            "payload_id": "string",
            "rocket_id": "string",
            "name": "string",
            "kg": "float",
            "lb": "float"
        })

        df_images = df_images.astype({
            "rocket_id": "string",
            "url": "string"
        })

        df_engines = df_engines.astype({
            "rocket_id": "string",
            "type": "string",
            "number": "int",
            "version": "string",
            "layout": "string",

            'thust_sea_level_kN': 'float',
            'thust_vacuum_kN': 'float',

            'isp_sl': 'float',
            'isp_vacuum': 'float',  

            'propellant_1': 'string',
            'propellant_2': 'string',
        })

        self.logger.info("DataFrames created with correct types")

        return {
            "rockets": df_rockets,
            "payloads": df_payloads,
            "images": df_images,
            "engines": df_engines
        }