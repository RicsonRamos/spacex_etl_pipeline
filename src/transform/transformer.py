import pandas as pd
from src.utils.logger import setup_logger
from src.config.config import settings

class SpaceXTransformer:
    def __init__(self):
        self.logger = setup_logger("transformer")
        # Carrega o mapeamento dinâmico do objeto settings (que leu o manifesto.json)
        # O formato esperado é settings.API_ENDPOINTS[endpoint_name]['mapping']
        self.endpoints_config = settings.API_ENDPOINTS

    def transform(self, endpoint_name, raw_data):
        """
        Método universal de transformação.
        Usa o mapeamento do manifesto para filtrar e renomear colunas.
        """
        if not raw_data:
            self.logger.warning(f"Nenhum dado recebido para transformar: {endpoint_name}")
            return pd.DataFrame()

        try:
            # 1. Busca a configuração do endpoint no manifesto
            config = self.endpoints_config.get(endpoint_name)
            if not config or "mapping" not in config:
                self.logger.error(f" Mapeamento não encontrado para '{endpoint_name}' no manifesto.")
                # Se não há mapeamento, retornamos o DF bruto como fallback para o load_generic
                return pd.json_normalize(raw_data)

            mapping = config["mapping"]

            # 2. Achata o JSON (Flattening)
            # Transforma estruturas como {'links': {'patch': {'small': 'url'}}} em 'links.patch.small'
            df = pd.json_normalize(raw_data)

            # 3. Filtragem e Renomeação Dinâmica
            # Filtra apenas as colunas que existem tanto no mapeamento quanto no JSON
            available_cols = [col for col in mapping.keys() if col in df.columns]
            
            # Avisa se colunas mapeadas estão faltando no JSON (Mudança na API)
            missing_cols = set(mapping.keys()) - set(df.columns)
            if missing_cols:
                self.logger.warning(f"Campos mapeados ausentes na API para {endpoint_name}: {missing_cols}")

            # Cria o novo DataFrame apenas com o que foi solicitado e renomeado
            df_transformed = df[available_cols].rename(columns=mapping)

            # 4. Limpeza Básica (Opcional: remover linhas totalmente nulas)
            df_transformed = df_transformed.dropna(how='all')

            self.logger.info(f"{endpoint_name.upper()}: Transformação concluída ({len(df_transformed.columns)} colunas).")
            return df_transformed

        except Exception as e:
            self.logger.error(f" Erro na transformação de {endpoint_name}: {e}")
            raise