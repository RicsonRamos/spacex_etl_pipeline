import pandas as pd

class LaunchesSchema:
    REQUIRED_COLUMNS = ['name', 'date_utc', 'flight_number', 'success'] 

    @staticmethod
    def validate(df: pd.DataFrame):
        missing = [col for col in LaunchesSchema.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Schema Inválido! Colunas faltantes: {', '.join(missing)}")
        if df.empty:
            raise ValueError("Schema Inválido! DataFrame vazio.")
        
        return True