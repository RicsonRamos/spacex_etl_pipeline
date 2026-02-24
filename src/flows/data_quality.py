import polars as pl

def validate_schema(df: pl.DataFrame, expected_columns: list[str]):
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        raise ValueError(f'Missing columns, {missing}')


def check_nulls(df: pl.DataFrame, columns: list[str]):
    for col in columns:
        if df[col].is_null().any():
            raise ValueError(f"Null values found in column {col}")


def check_duplicates(df: pl.DataFrame, subset: list[str]):
    # Seleciona apenas as colunas do subset e verifica duplicados
    if df.select(subset).is_duplicated().any():
        raise ValueError(f"Duplicate rows found based on {subset}")


def check_date_ranges(df: pl.DataFrame, column: str, min_date, max_date):
    if not df[column].is_between(min_date, max_date).all():
        raise ValueError(f"Values in {column} out of expected range")