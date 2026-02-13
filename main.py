import concurrent
from src.extract.extract import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.load import SpaceXLoader
from src.utils.logger import setup_logger

def main():
    """
    Main entry point for the SpaceX ETL Pipeline (Hybrid Architecture)

    This function is responsible for initializing the modules, fetching the data,
    and dynamically executing the ETL cycle for each endpoint defined in the manifest.

    If any part of the pipeline fails, the entire pipeline will not crash but instead,
    the error will be logged and the pipeline will continue to the next endpoint.
    """
    # 1. Central Logger Configuration
    logger = setup_logger("main_pipeline")
    logger.info("Starting SpaceX ETL Pipeline (Hybrid Architecture)")

    try:
        # 2. Module Initialization (All now fetch config by themselves)
        extractor = SpaceXExtractor()
        transformer = SpaceXTransformer()
        loader = SpaceXLoader()

        # 3. EXTRACTION (Bulk)
        # fetch_all traverses the manifest and returns a dict {name: raw_data}
        raw_data_map = extractor.fetch_all()
        if not raw_data_map:
            logger.error("No data extracted. Check connection or manifest.")
            return

        # 4. DYNAMIC ETL CYCLE
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for endpoint_name, data in raw_data_map.items():
                # Submit each endpoint to the executor to be processed in parallel
                futures.append(executor.submit(transform_and_load, extractor, transformer, loader, endpoint_name, data))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    # Get the result of each endpoint (if successful)
                    future.result()
                except Exception as e:
                    # Rigor: An error in one endpoint should not crash the entire pipeline
                    logger.error(f"Failure in endpoint cycle '{future.result()[0]}': {e}")

        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.critical(f"Catastrophic failure in pipeline engine: {e}")


def transform_and_load(extractor, transformer, loader, endpoint_name, data):
    """
    Helper function to transform and load each endpoint in parallel.

    This function takes the extractor, transformer, loader, endpoint name, and raw data as input.
    It first transforms the data using the manifest mapping and checks if the DataFrame is empty.
    If not empty, it loads the DataFrame into the database using the loader's load method.
    If any exception occurs during transformation or loading, it logs the error and continues to the next endpoint.

    :param extractor: The extractor object.
    :param transformer: The transformer object.
    :param loader: The loader object.
    :param endpoint_name: The name of the endpoint.
    :param data: The raw data to transform and load.
    """
    logger = setup_logger("main_pipeline")
    try:
        # A: Transformation (Uses JSON mapping)
        df_processed = transformer.transform(endpoint_name, data)
        
        if df_processed.empty:
            logger.warning(f"{endpoint_name}: Empty DataFrame after transformation. Skipping load.")
            return
        
        # B: Load (Automatically decides between upsert or Generic)
        # Use apply to leverage multi-processing for faster loading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(loader.load, [endpoint_name] * len(df_processed), df_processed.to_records(index=False))
        
    except Exception as e:
        # Rigor: An error in one endpoint should not crash the entire pipeline
        logger.error(f"Failure in endpoint cycle '{endpoint_name}': {e}")

if __name__ == "__main__":
    main()