import pytest
from src.transformers.factory import TransformerFactory
from src.transformers.rocket import RocketTransformer
from src.transformers.launch import LaunchTransformer

def test_factory_create_returns_instance():
    rocket_transformer = TransformerFactory.create("rockets")
    launch_transformer = TransformerFactory.create("launches")
    assert isinstance(rocket_transformer, RocketTransformer)
    assert isinstance(launch_transformer, LaunchTransformer)

def test_factory_get_raises_for_unregistered():
    with pytest.raises(ValueError):
        TransformerFactory.get("nonexistent")

def test_factory_register_duplicate_raises():
    with pytest.raises(ValueError):
        TransformerFactory.register("rockets", RocketTransformer)