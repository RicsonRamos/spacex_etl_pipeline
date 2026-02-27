from datetime import datetime
import polars as pl
import pytest

from src.transformers.base import BaseTransformer


class DummyTransformer(BaseTransformer):
    schema = {
        "name": "dummy",
        "columns": ["id", "value", "date_utc"],
        "pk": "id",
        "casts": {"value": pl.Int64},
        "rename": {"val": "value"},
    }

    def transform(self, data, last_ingested=None):
        df = self._build_df(data)
        df = self._rename_columns(df)
        df = self._normalize_dates(df)
        df = self._apply_incremental_filter(df, last_ingested)
        df = self._apply_casts(df)
        self._validate_schema(df)
        df = self._select_columns(df)
        df = self._deduplicate(df)
        return df

    def _build_df(self, data):
        return pl.DataFrame(data)
    
    def test_rename_columns():
        transformer = DummyTransformer()

        data = [{"id": 1, "val": 10, "date_utc": "2020-01-01T00:00:00.000Z"}]
        df = transformer._build_df(data)
        df = transformer._rename_columns(df)

        assert "value" in df.columns
        assert "val" not in df.columns
    
    def test_normalize_dates():
        transformer = DummyTransformer()

        data = [{"id": 1, "value": 10, "date_utc": "2020-01-01T00:00:00.000Z"}]
        df = transformer._build_df(data)
        df = transformer._normalize_dates(df)

        assert df.schema["date_utc"] == pl.Datetime("us", time_zone="UTC")
    
    def test_incremental_filter():
        transformer = DummyTransformer()

        data = [
            {"id": 1, "value": 10, "date_utc": "2020-01-01T00:00:00.000Z"},
            {"id": 2, "value": 20, "date_utc": "2022-01-01T00:00:00.000Z"},
        ]

        last_ingested = datetime(2021, 1, 1)

        df = transformer.transform(data, last_ingested)

        assert df.height == 1
        assert df["id"][0] == 2

    def test_apply_casts():
        transformer = DummyTransformer()

        data = [{"id": 1, "value": "10", "date_utc": "2020-01-01T00:00:00.000Z"}]
        df = transformer.transform(data)

        assert df.schema["value"] == pl.Int64

    def test_deduplicate():
        transformer = DummyTransformer()

        data = [
            {"id": 1, "value": 10, "date_utc": "2020-01-01T00:00:00.000Z"},
            {"id": 1, "value": 10, "date_utc": "2020-01-01T00:00:00.000Z"},
        ]

        df = transformer.transform(data)

        assert df.height == 1
    
    def test_validate_schema_error():
        transformer = DummyTransformer()

        data = [{"id": 1}]  # faltando value e date_utc

        with pytest.raises(ValueError):
            transformer.transform(data)