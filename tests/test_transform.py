from src.transform.transformer import SpaceXTransformer
import polars as pl

def test_transform_launches_success(sample_raw_launch):
    # Arrange (Preparar)
    transformer = SpaceXTransformer()
    raw_data = [sample_raw_launch]

    # Act (Executar)
    df = transformer.transform_launches(raw_data)

    # Assert (Verificar)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 1
    assert "launch_id" in df.columns
    assert df["launch_id"][0] == sample_raw_launch["id"]
    # Verifica se a tipagem foi convertida corretamente
    assert df["success"].dtype == pl.Boolean
