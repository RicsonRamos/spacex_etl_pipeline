import pandas as pd


def transform_rockets(raw_data):

    rockets = []
    payloads = []
    images = []
    engines = []

    for r in raw_data:

        rocket_id = r["id"]

        # ------------------
        # TABELA ROCKETS
        # ------------------
        rockets.append({
            "id": rocket_id,
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
            "diameter_m": r["diameter"]["meters"],
            "mass_kg": r["mass"]["kg"]
        })

        # ------------------
        # PAYLOADS
        # ------------------
        for p in r["payload_weights"]:
            payloads.append({
                "rocket_id": rocket_id,
                "orbit": p["id"],
                "kg": p["kg"],
                "lb": p["lb"]
            })

        # ------------------
        # IMAGES
        # ------------------
        for url in r["flickr_images"]:
            images.append({
                "rocket_id": rocket_id,
                "url": url
            })

        # ------------------
        # ENGINES
        # ------------------
        e = r["engines"]

        engines.append({
            "rocket_id": rocket_id,
            "type": e["type"],
            "version": e["version"],
            "layout": e["layout"],
            "number": e["number"],

            "thrust_sl_kn": e["thrust_sea_level"]["kN"],
            "thrust_vac_kn": e["thrust_vacuum"]["kN"],

            "isp_sl": e["isp"]["sea_level"],
            "isp_vac": e["isp"]["vacuum"],

            "propellant_1": e["propellant_1"],
            "propellant_2": e["propellant_2"]
        })

    # ------------------
    # DATAFRAMES
    # ------------------

    df_rockets = pd.DataFrame(rockets)

    df_payloads = pd.DataFrame(payloads)

    df_images = pd.DataFrame(images)

    df_engines = pd.DataFrame(engines)

    # Datas
    df_rockets["first_flight"] = pd.to_datetime(
        df_rockets["first_flight"],
        utc=True,
        errors="coerce"
    )

    return {
        "rockets": df_rockets,
        "payloads": df_payloads,
        "images": df_images,
        "engines": df_engines
    }

def remove_complex(df):
    return df.loc[:, df.applymap(
        lambda x: not isinstance(x, (list, dict))
    ).all()]
