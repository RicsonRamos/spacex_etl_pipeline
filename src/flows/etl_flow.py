import structlog
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader

# Configuração do logger
logger = structlog.get_logger()

def get_enriched_logger():
    """
    Retorna um logger que é enriquecido com informações de contexto do Prefect.
    """
    try:
        ctx = get_run_context()
        return logger.bind(
            flow_run_id=str(ctx.flow_run.id),
            flow_name=ctx.flow_run.name,
            task_run_id=getattr(ctx.task_run, 'id', None)
        )
    except Exception:
        return logger  # Retorna o logger padrão caso o contexto do Prefect não esteja disponível

# Função para inserir dados no banco (upsert)
class PostgresLoader:
    def upsert_dataframe(self, df, table_name, pk_col):
        # Definindo exc fora do bloco try-except para garantir que ela sempre tenha um valor
        exc = None
        try:
            # Exemplo de código de inserção no banco de dados
            # Aqui deve vir a lógica real de inserção (ex: conn.execute(...))
            pass  # Substitua com o código real de upsert
        except Exception as e:
            # Captura de erro e definição de exc
            exc = e
            raise  # Re-lança a exceção para o Prefect capturar

        # Agora podemos acessar a variável 'exc' sem causar erro
        status = "success" if not exc else "failure"
        error_message = str(exc) if exc else None
        
# Configuração de tarefas com tentativas e atrasos
@task(retries=5, retry_delay_seconds=15)
def process_endpoint(endpoint: str, table_name: str, pk_col: str):
    """
    Processa um endpoint da API SpaceX e carrega os dados no banco de dados PostgreSQL.
    """
    prefect_logger = get_run_logger()
    log = get_enriched_logger().bind(endpoint=endpoint, table=table_name)

    log.info(f"Iniciando o processamento do endpoint {endpoint}.")
    prefect_logger.info(f"Tarefa iniciada para o endpoint: {endpoint}")

    try:
        # Criando uma instância de SpaceXExtractor para buscar os dados da API
        extractor = SpaceXExtractor()  # Se fetch_data não for estático, crie uma instância

        log.info(f"Buscando dados para {endpoint}...")
        raw_data = extractor.fetch_data(endpoint)  # Passando o parâmetro corretamente
        
        # Verificando se os dados são válidos antes de prosseguir
        if raw_data is None or len(raw_data) == 0:
            log.error(f"Nenhum dado retornado para o endpoint {endpoint}.")
            prefect_logger.error(f"Nenhum dado retornado para o {endpoint}.")
            return  # Retorna para evitar erro nas etapas seguintes

        # Criando a instância do transformer
        transformer = SpaceXTransformer()

        # Transformando os dados
        log.info(f"Transformando dados para {endpoint}...")
        transform_method = getattr(transformer, f"transform_{endpoint}")
        df_clean = transform_method(raw_data)  # Passando raw_data para o método de transformação
        
        # Carregando os dados no PostgreSQL
        log.info(f"Carregando dados transformados para {endpoint} no PostgreSQL...")
        PostgresLoader().upsert_dataframe(df_clean, table_name, pk_col)  # Corrigido para instanciar o PostgresLoader
        
        log.info(f"Domínio {endpoint} processado com sucesso.")
        prefect_logger.info(f"Tarefa processada com sucesso: {endpoint}")
    
    except Exception as e:
        # Se ocorrer uma falha, capturamos o erro e logamos
        log.error(f"Falha ao processar o domínio {endpoint}", exc_info=e)  # Logando com exc_info para mostrar o traceback
        prefect_logger.error(f"Erro crítico ao processar o {endpoint}: {e}", exc_info=e)  # Com traceback
        raise

@flow(name="SpaceX ETL Production Pipeline")
def spacex_etl_flow():
    """
    O fluxo principal que processa dados da API SpaceX e os carrega no banco de dados PostgreSQL.
    """
    log = get_enriched_logger()
    log.info("Iniciando o fluxo de produção SpaceX")

    try:
        # Processa os endpoints "rockets", "launchpads", "payloads" e "launches"
        rockets_future = process_endpoint.submit("rockets", "rockets", "rocket_id")
        log.info("Processamento do endpoint 'rockets' iniciado")

        launchpads_future = process_endpoint.submit("launchpads", "launchpads", "launchpad_id")
        log.info("Processamento do endpoint 'launchpads' iniciado")

        payloads_future = process_endpoint.submit(
            "payloads", "payloads", "payload_id", 
            wait_for=[rockets_future, launchpads_future]
        )
        log.info("Processamento do endpoint 'payloads' iniciado (aguardando 'rockets' e 'launchpads' concluírem)")

        launches_future = process_endpoint.submit(
            "launches", "launches", "launch_id", 
            wait_for=[rockets_future, launchpads_future]
        )
        log.info("Processamento do endpoint 'launches' iniciado (aguardando 'rockets' e 'launchpads' concluírem)")

        # Aguarda as tarefas 'payloads' e 'launches' antes de finalizar o fluxo
        payloads_future.result()
        launches_future.result()
    
    except Exception as e:
        log.error(f"Fluxo falhou com erro: {e}", exc_info=e)
        raise

if __name__ == "__main__":
    spacex_etl_flow()
