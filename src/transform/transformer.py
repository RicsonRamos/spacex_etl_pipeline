import pandas as pd 
import json
from src.config.config import settings 

class SpaceXTransformer:
    def transform(self, endpoint: str, raw_data: list) -> pd.DataFrame:
        df = pd.json_normalize(raw_data)

        # Apply whitelist
        whitelist = settings.WHITELIST.get(endpoint, [])
        df = df[[c for c in whitelist if c in df.columns]]

        # Normalize column names and ids
        mapping = {
            'id': 'launch_id' if endpoint == 'launches' else 
                  'rocket_id' if endpoint == 'rockets' else 
                  'payload_id' if endpoint == 'payloads' else 
                  'launchpad_id',
            'rocket': 'rocket_id',
            'launchpad': 'launchpad_id',
            'height.meters': 'height_m', # Resolve o erro do "."
            'mass.kg': 'mass_kg'         # Resolve o erro do "."
        }
        whitelist = settings.WHITELIST.get(endpoint, [])
        df = df[[c for c in df.columns if c in whitelist]]

        df = df.rename(columns=mapping)

        # Converter types 
        
        for col in df.columns:
            if 'date' in col.lower():
                # Correção: to_datetime e strftime
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            elif df[col].dtype == 'bool':
                df[col] = df[col].astype(int)

        # Drop duplicates
        return df.drop_duplicates()