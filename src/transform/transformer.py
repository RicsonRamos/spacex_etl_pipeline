import pandas as pd
import numpy as np
from typing import List, Dict, Any
import logging
from src.config.config import settings

class SpaceXTransformer:
    def __init__(self):
        self.logger = logging.getLogger("transformer")
        # Mapeamento estrito para o SQLite
        self.schema_map = {
            "mass_kg": "float64",
            "height_m": "float64",
            "success": "Int64", 
            "active": "Int64",
            "cost_per_launch": "Int64",
            "flight_number": "Int64",
            "reused": "Int64"
        }

    def _validate_data_quality(self, df: pd.DataFrame, endpoint: str) -> pd.DataFrame:
        """Camada de Integridade: PK Check e Deduplicação."""
        if 'id' not in df.columns:
            self.logger.error(f"PK 'id' ausente no payload de {endpoint}")
            return pd.DataFrame()

        initial_len = len(df)
        df = df.dropna(subset=['id'])
        
        dropped = initial_len - len(df)
        if dropped > 0:
            self.logger.warning(f"[DQ] {dropped} registros descartados em {endpoint} por PK nula.")
            
        return df.drop_duplicates(subset=['id'])

    def transform(self, endpoint: str, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        # 1. Flatten
        df = pd.json_normalize(raw_data, sep='.')

        # 2. Qualidade de Dados (Pré-rename)
        df = self._validate_data_quality(df, endpoint)
        if df.empty: return df

        # 3. Normalização de Nomes (API v4 -> DB Schema)
        mapping = {
            "id": "launch_id" if endpoint == "launches" else f"{endpoint.rstrip('s')}_id",
            "rocket": "rocket_id",
            "launchpad": "launchpad_id",
            "height.meters": "height_m",
            "mass.kg": "mass_kg",
            "mass_kg": "mass_kg" 
        }
        df = df.rename(columns=mapping)

        # 4. Whitelist Filter
        whitelist = settings.WHITELIST.get(endpoint, [])
        if whitelist:
            pk_final = mapping.get("id")
            if pk_final and pk_final not in whitelist: 
                whitelist.append(pk_final)
            df = df[[col for col in whitelist if col in df.columns]]

        # 5. Casting e Tipagem Defensiva
        df = self._enforce_schema(df)

        # 6. Serialização de listas para SQLite
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(
                    lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
                )

        return df

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Garante tipagem robusta e trata valores nulos/booleanos sem conflitos."""
        for col, dtype in self.schema_map.items():
            if col in df.columns:
                # 1. Tratamento Semântico de Booleanos PRIMEIRO
                if col in ['success', 'active', 'reused']:
                    # Usamos replace/map antes de qualquer conversão numérica
                    # 'Int64' permite manter NaNs onde o lançamento ainda não ocorreu
                    df[col] = df[col].map({True: 1, False: 0, 1: 1, 0: 0, 1.0: 1, 0.0: 0})
                
                # 2. Conversão Numérica para os demais casos ou fallback
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 3. Aplicação do Tipo Final (Int64 ou float64)
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logging.warning(f"Falha ao converter coluna {col} para {dtype}: {e}")
                    
        return df