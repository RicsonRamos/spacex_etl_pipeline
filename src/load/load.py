from sqlalchemy import create_engine
from settings import load_config


config = load_config()

DB_URL = config["database"]["connection_string"]

engine = create_engine(DB_URL)


def load_tables(tables: dict):

    for name, df in tables.items():

        df.to_sql(
            name,
            engine,
            if_exists="replace",
            index=False
        )

        print(f"✔ Loaded: {name}")
