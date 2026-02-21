from src.flows.etl_flow import spacex_etl_pipeline
def main():
    # Executa o pipeline orquestrado pelo Prefect
    spacex_etl_pipeline()

if __name__ == "__main__":
    main()