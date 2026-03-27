import pytest
import pandas as pd
from ingestion_engine.utils.api_client import SpaceXAPIMocker
from ingestion_engine.utils.api_client import DataFrameAssertions

class TestDataFrameAssertions:

    def test_assert_has_columns_passes(self):
        df = pd.DataFrame(SpaceXAPIMocker.get_sample_launches(count=2))
        DataFrameAssertions.assert_has_columns(df, ["id", "name"])

    def test_assert_has_columns_fails(self):
        df = pd.DataFrame(SpaceXAPIMocker.get_sample_launches(count=2))
        with pytest.raises(AssertionError):
            DataFrameAssertions.assert_has_columns(df, ["nonexistent_column"])

    def test_assert_no_nulls_in_column_passes(self):
        df = pd.DataFrame(SpaceXAPIMocker.get_sample_launches(count=2))
        df["id"] = df["id"].fillna("placeholder")
        DataFrameAssertions.assert_no_nulls_in_column(df, "id")

    def test_assert_row_count_passes(self):
        df = pd.DataFrame(SpaceXAPIMocker.get_sample_launches(count=3))
        DataFrameAssertions.assert_row_count(df, 3)

    def test_assert_row_count_fails(self):
        df = pd.DataFrame(SpaceXAPIMocker.get_sample_launches(count=2))
        with pytest.raises(AssertionError):
            DataFrameAssertions.assert_row_count(df, 3)