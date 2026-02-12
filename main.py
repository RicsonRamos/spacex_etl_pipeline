from src.settings import load_config
from src.extract.extract import SpaceXExtractor
from src.transform.spacex_transformer import SpaceXTransformer
from src.db.database import DatabaseManager
from src.db.schema import SchemaManager

from src.load.load import SpaceXLoader


def main():

    config = load_config()

    # -------- EXTRACT --------
    extractor = SpaceXExtractor(config)
    raw = extractor.fetch_all()

    # -------- TRANSFORM --------
    transformer = SpaceXTransformer()

    rockets_tables = transformer.transform_rockets(
        raw["rockets"]
    )

    # -------- DB --------
    db = DatabaseManager()
    schema = SchemaManager(db)

    schema.drop_all()
    schema.create_tables()

    # -------- LOAD --------
    loader = SpaceXLoader(db.get_engine())

    loader.load_tables(rockets_tables)

    print("Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    main()