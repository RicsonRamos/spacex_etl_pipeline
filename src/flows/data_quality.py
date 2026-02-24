import polars as pl
from typing import List

def validate_schema(df: pl.DataFrame, expected_columns: List[str]):
    """Valida se todas as colunas esperadas estão presentes no DataFrame."""
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(f'Missing columns: {missing}')


import polars as pl

def check_nulls(df: pl.DataFrame, columns: list[str]):
    """Verifica se há valores nulos nas colunas especificadas e os substitui."""
    for col in columns:
        if df[col].is_null().any():
            print(f"Aviso: Valores nulos encontrados na coluna {col}. Substituindo por False.")
            
            # Se a coluna for booleana, usa fill_null, caso contrário, usa fill_nan
            if df[col].dtype == pl.Boolean:
                df = df.with_columns(pl.col(col).fill_null(False).alias(col))  # Substitui nulos por False
            else:
                df = df.with_columns(pl.col(col).fill_nan(False).alias(col))  # Para outras colunas, usa fill_nan
    return df


def check_duplicates(df: pl.DataFrame, subset: List[str]):
    """Verifica se há linhas duplicadas com base nas colunas do subset."""
    if df.select(subset).is_duplicated().any():
        raise ValueError(f"Duplicate rows found based on: {subset}")


def check_date_ranges(df: pl.DataFrame, column: str, min_date: str, max_date: str):
    """Verifica se os valores de uma coluna de datas estão dentro de um intervalo esperado."""
    # Garantindo que as datas estejam no formato correto
    df = df.with_columns(pl.col(column).str.strptime(pl.Datetime, fmt="%Y-%m-%dT%H:%M:%S.%fZ").alias(column))
    
    # Verifica se todas as datas estão no intervalo desejado
    if not df[column].is_between(min_date, max_date).all():
        raise ValueError(f"Values in {column} are out of expected range ({min_date} to {max_date})")
