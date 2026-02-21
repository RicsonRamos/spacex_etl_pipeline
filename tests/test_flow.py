import pytest
from src.flows.etl_flow import spacex_etl_flow

def test_flow_executes(mocker):
    mock_loader = mocker.Mock()
    mock_loader.engine = mocker.Mock()

    mocker.patch("src.flows.etl_flow.Base.metadata.create_all")
    mocker.patch("src.flows.etl_flow.extract_task", return_value=[])
    mocker.patch("src.flows.etl_flow.load_bronze_task")
    mocker.patch("src.flows.etl_flow.transform_task", return_value=None)
    mocker.patch("src.flows.etl_flow.load_silver_task")
    mocker.patch("src.flows.etl_flow.refresh_gold_task")

    spacex_etl_flow(loader=mock_loader)

    assert True