from prefect import flow
from prefect.testing.utilities import prefect_test_harness
from src.flows.etl_flow import spacex_main_pipeline

def test_flow_logic_integration():
    """Garante que as tasks do flow conversam entre si."""
    with prefect_test_harness():
        # Aqui você pode mockar o extractor para não gastar API
        # e rodar o pipeline para ver se ele chega ao fim (State: Completed)
        state = spacex_main_pipeline()
        assert state.is_completed()
