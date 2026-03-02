import pandas as pd

# Lista de colunas que permanecerão no DataFrame
COLUMNS_TO_KEEP = [
    'flight_number', 'name', 'id',
    'date_utc', 'window',
    'success', 'failures',
    'rocket', 'launchpad', 'cores',
    'crew', 'payloads'
]

# -----------------------
# Datas e janelas
# -----------------------
class DateTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'date_utc' in df.columns and 'date_utc' in COLUMNS_TO_KEEP:
            df['date_utc'] = pd.to_datetime(df['date_utc'], errors='coerce')
            df['year'] = df['date_utc'].dt.year
            df['month'] = df['date_utc'].dt.month
            df['day'] = df['date_utc'].dt.day
            df['hour'] = df['date_utc'].dt.hour

        if 'net' in df.columns and 'net' in COLUMNS_TO_KEEP:
            df['net'] = pd.to_datetime(df['net'], errors='coerce')
        return df


class WindowTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'window' in df.columns and 'window' in COLUMNS_TO_KEEP:
            df['window'] = pd.to_numeric(df['window'], errors='coerce').fillna(0)
        return df


# -----------------------
# Sucesso e falhas
# -----------------------
class SuccessFailureTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'success' in df.columns and 'success' in COLUMNS_TO_KEEP:
            df['success'] = df['success'].fillna(False)
        if 'failures' in df.columns and 'failures' in COLUMNS_TO_KEEP:
            df['num_failures'] = df['failures'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        return df


# -----------------------
# Tripulação e cargas
# -----------------------
class CrewPayloadTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'crew' in df.columns and 'crew' in COLUMNS_TO_KEEP:
            df['num_crew'] = df['crew'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        if 'payloads' in df.columns and 'payloads' in COLUMNS_TO_KEEP:
            df['num_payloads'] = df['payloads'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        return df


# -----------------------
# Boosters / cores
# -----------------------
class CoresTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'cores' in df.columns and 'cores' in COLUMNS_TO_KEEP:
            df['num_cores'] = df['cores'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        return df



# -----------------------
# Links e mídia
# -----------------------
class LinksTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'links.youtube_id' in df.columns and 'links.youtube_id' in COLUMNS_TO_KEEP:
            df['youtube_url'] = df['links.youtube_id'].apply(lambda x: f'https://youtu.be/{x}' if pd.notnull(x) else None)
        if 'links.flickr.original' in df.columns and 'links.flickr.original' in COLUMNS_TO_KEEP:
            df['num_flickr_photos'] = df['links.flickr.original'].apply(lambda x: len(x) if isinstance(x, list) else 0)

        link_cols = [
            'links.patch.large', 'links.reddit.campaign', 'links.reddit.launch',
            'links.reddit.media', 'links.reddit.recovery', 'links.presskit',
            'links.webcast', 'links.article', 'links.wikipedia'
        ]
        for col in link_cols:
            if col in df.columns and col in COLUMNS_TO_KEEP:
                df[col] = df[col].where(df[col].notna(), None)
        return df


# -----------------------
# Identificação / limpeza geral
# -----------------------
class IdentifierCleaner:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if 'name' in df.columns and 'name' in COLUMNS_TO_KEEP:
            df['name'] = df['name'].astype(str).str.strip()
        return df


# -----------------------
# Seleção final de colunas
# -----------------------
class ColumnsToKeepTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        # Mantém apenas as colunas definidas no COLUMNS_TO_KEEP
        cols_existentes = [c for c in COLUMNS_TO_KEEP if c in df.columns]
        return df[cols_existentes]


# -----------------------
# Orquestrador
# -----------------------
class LaunchTransformer:
    transformers = [
        DateTransformer,
        WindowTransformer,
        SuccessFailureTransformer,
        CrewPayloadTransformer,
        CoresTransformer,
        LinksTransformer,
        IdentifierCleaner,
        ColumnsToKeepTransformer
    ]

    @staticmethod
    def launches_transform(df: pd.DataFrame) -> pd.DataFrame:
        for transformer in LaunchTransformer.transformers:
            df = transformer.transform(df)
        return df