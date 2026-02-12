from src.settings import load_config
from src.extract.extract import SpaceXExtractor
from src.transform.spacex_transformer import SpaceXTransformer


def main():

    config = load_config()

    extractor = SpaceXExtractor(config)
    transformer = SpaceXTransformer()

    raw = extractor.fetch_all()

    rockets_tables = transformer.transform_rockets(
        raw["rockets"]
    )

    for name, df in rockets_tables.items():
        print(name, df.shape)


if __name__ == "__main__":
    main()
