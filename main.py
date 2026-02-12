from extract.extract import extract_all
from src.transform import transform_rockets
from src.load import load_tables


def main():

    raw = extract_all()

    rockets_tables = transform_rockets(raw["rockets"])

    load_tables(rockets_tables)

    print("ETL executado com sucesso")


if __name__ == "__main__":
    main()
