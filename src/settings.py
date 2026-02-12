import yaml
from pathlib import Path

def load_config():
    """
    Load configuration from the config.yaml file in the same directory as this module.

    :return: A dictionary containing the configuration.
    :raises FileNotFoundError: If the configuration file is not found.
    """
    settings_path = Path("config/settings.yaml")

    if not settings_path.exists():
        raise FileNotFoundError(f"Configuration file not found at {settings_path}")

    # Use the CLoader which is faster and more efficient than the default Loader
    with open(settings_path, 'r') as f:
        return yaml.load(f, Loader=yaml.CLoader)
