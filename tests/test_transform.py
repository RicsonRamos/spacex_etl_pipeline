import polars as pl
from src.transform.transformer import SpaceXTransformer

def test_transform_rockets_schema(mock_rocket_data):
    transformer = SpaceXTransformer()
    
    # Executa a transformação
    df = transformer.transform("rockets", mock_rocket_data)
    
    # Validações de Rigor
    assert isinstance(df, pl.DataFrame)
    assert "rocket_id" in df.columns  # Verifica renomeação (id -> rocket_id)
    assert df["cost_per_launch"].dtype == pl.Float64
    assert df.height == 1