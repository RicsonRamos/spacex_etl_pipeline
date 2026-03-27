# ingestion_engine/tests/test_ingestion.py
import pytest
import pandas as pd
from ingestion_engine.utils.api_client import SpaceXAPIMocker, NASAAPIMocker

# ==========================
# Mocks das funções do main
# ==========================
def ingest_spacex_data(limit=2) -> pd.DataFrame:
    """Mock de ingestão de dados SpaceX"""
    launches = SpaceXAPIMocker.get_sample_launches(count=limit)
    df = pd.DataFrame(launches)
    df["source_endpoint"] = "spacex_launches"
    df["data_layer"] = "bronze"
    df["ingestion_timestamp"] = pd.Timestamp.utcnow()
    return df

def ingest_nasa_data(limit=2) -> pd.DataFrame:
    """Mock de ingestão de dados NASA"""
    events = NASAAPIMocker.get_sample_events(count=limit)
    df = pd.DataFrame(events)
    df["source_endpoint"] = "nasa_solar_events"
    df["data_layer"] = "bronze"
    df["ingestion_timestamp"] = pd.Timestamp.utcnow()
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Mock de validação de dados"""
    # Aqui poderíamos aplicar checagens de pré-voo (preflight)
    assert not df.empty, "DataFrame está vazio"
    return df

def load_to_bronze(df: pd.DataFrame):
    """Mock de load para camada bronze"""
    # Apenas valida que df tem linhas
    assert len(df) > 0, "Nenhum dado para carregar"


# ==========================
# Testes SpaceX
# ==========================
def test_ingest_spacex_data_runs():
    df = ingest_spacex_data(limit=3)
    assert not df.empty
    assert len(df) == 3
    assert "flight_number" in df.columns
    assert "source_endpoint" in df.columns

def test_validate_spacex_data():
    df = ingest_spacex_data(limit=2)
    validated_df = validate_data(df)
    assert validated_df.equals(df)

def test_load_spacex_to_bronze():
    df = ingest_spacex_data(limit=1)
    load_to_bronze(df)


# ==========================
# Testes NASA
# ==========================
def test_ingest_nasa_data_runs():
    df = ingest_nasa_data(limit=2)
    assert not df.empty
    assert len(df) == 2
    assert "activityID" in df.columns
    assert "source_endpoint" in df.columns

def test_validate_nasa_data():
    df = ingest_nasa_data(limit=2)
    validated_df = validate_data(df)
    assert validated_df.equals(df)

def test_load_nasa_to_bronze():
    df = ingest_nasa_data(limit=1)
    load_to_bronze(df)


# ==========================
# Teste integrado de fluxo completo
# ==========================
def test_full_pipeline_spacex():
    df = ingest_spacex_data(limit=2)
    df = validate_data(df)
    load_to_bronze(df)
    assert "flight_number" in df.columns
    assert df["source_endpoint"].iloc[0] == "spacex_launches"

def test_full_pipeline_nasa():
    df = ingest_nasa_data(limit=2)
    df = validate_data(df)
    load_to_bronze(df)
    assert "activityID" in df.columns
    assert df["source_endpoint"].iloc[0] == "nasa_solar_events"