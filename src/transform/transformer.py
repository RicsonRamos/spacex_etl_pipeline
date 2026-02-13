import pandas as pd
from src.utils.logger import setup_logger
from src.config.config import settings

class SpaceXTransformer:
    """
    The Transformer class is responsible for transforming raw data into a standardized, cleaned DataFrame.
    It reads the manifest configuration to dynamically filter and rename columns based on the endpoint name.
    """
    def __init__(self):
        """
        Initializes the Transformer object.

        Loads dynamic mapping from settings object (which read manifest.json).
        Expected format is settings.API_ENDPOINTS[endpoint_name]['mapping']
        """
        self.logger = setup_logger("transformer")
        # Loads dynamic mapping from settings object (which read manifest.json)
        # Expected format is settings.API_ENDPOINTS[endpoint_name]['mapping']
        self.endpoints_config = settings.API_ENDPOINTS

    def transform(self, endpoint_name, raw_data):
        """
        Universal transformation method.
        Uses manifest mapping to filter and rename columns.

        1. Fetch endpoint configuration from manifest.
        2. Flatten JSON.
        3. Dynamic Filtering and Renaming.
        4. Basic Cleanup (Optional: remove completely null rows).

        :param endpoint_name: The name of the endpoint to transform.
        :param raw_data: The raw data to transform.
        :return: The transformed DataFrame.
        """
        if not raw_data:
            self.logger.warning(f"No data received for transformation: {endpoint_name}")
            return pd.DataFrame()

        try:
            # 1. Fetch endpoint configuration from manifest
            # Fetch the configuration from the manifest for the given endpoint
            config = self.endpoints_config.get(endpoint_name)
            if not config or "mapping" not in config:
                # If no mapping exists, return raw DataFrame as fallback for load_generic
                self.logger.error(f"Mapping not found for '{endpoint_name}' in manifest.")
                return pd.json_normalize(raw_data)

            # 2. Flatten JSON
            # Transforms structures like {'links': {'patch': {'small': 'url'}}} to 'links.patch.small'
            df = pd.json_normalize(raw_data)

            # 3. Dynamic Filtering and Renaming
            # Filters only columns that exist in both mapping and JSON
            available_cols = [col for col in config["mapping"].keys() if col in df.columns]
            
            # Warns if mapped columns are missing in JSON (API change)
            missing_cols = set(config["mapping"].keys()) - set(df.columns)
            if missing_cols:
                self.logger.warning(f"Mapped fields missing from API for {endpoint_name}: {missing_cols}")

            # Creates new DataFrame with only requested and renamed columns
            df_transformed = df[available_cols].rename(columns=config["mapping"])

            # 4. Basic Cleanup (Optional: remove completely null rows)
            # Removes rows with no values
            df_transformed = df_transformed.dropna(how='all')

            self.logger.info(f"{endpoint_name.upper()}: Transformation completed ({len(df_transformed.columns)} columns).")
            return df_transformed

        except Exception as e:
            self.logger.error(f"Error during transformation of {endpoint_name}: {e}")
            raise
