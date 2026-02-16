from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader

@task(retries=2, name="Extração")
def extract():
    return SpaceXExtractor().fetch_data("launches")

@task(name="Transformação")
def transform(data):
    df = SpaceXTransformer().transform_launches(data)
    if "launch_id" not in df.columns:
        raise KeyError("Falha de esquema: launch_id ausente.")
    return df

@task(name="Carga")
def load(df):
    loader = PostgresLoader()
    loader.ensure_table() # Garante a tabela antes do upsert
    loader.upsert_dataframe(df, "launches", "launch_id")

@flow(name="SpaceX_ETL")
def main_flow():
    raw_data = extract()
    transformed_df = transform(raw_data)
    load(transformed_df)

@flow(name="SpaceX_ETL")
def spacex_etl_flow():  # <--- O nome definido aqui...
    raw_data = extract()
    transformed_df = transform(raw_data)
    load(transformed_df)

if __name__ == "__main__":
    # O método .serve() registra o flow na nuvem e fica ouvindo por execuções
    # Ele substitui a necessidade de um Worker separado
    spacex_etl_flow.serve(
        name="spacex-etl-prod",
        tags=["local-machine"],
        parameters={},
        # O agendamento pode ser definido aqui também
        cron="0 3 * * *" 
    )