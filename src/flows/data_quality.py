import polars as pl 

def validate_schema(df: pl.DataFrame, expected_columns: list[str]):
  missing = [c for c in expected_columns if c not in df.columns]
  if missing:
    return ValueError(f'Missing columns, {missing}')

def check_nulls(df: pl.DataFrame, columns: list[str]):
    for col in columns:
        if df[col].isnull().any():
            raise ValueError(f"Null values found in column {col}")

def check_duplicates(df: pl.DataFrame, subset: list[str]):
    if df.duplicated(subset=subset).any():
        raise ValueError(f"Duplicate rows found based on {subset}")

def check_date_ranges(df: pl.DataFrame, column: str, min_date, max_date):
    if not df[column].between(min_date, max_date).all():
        raise ValueError(f"Values in {column} out of expected range")
