import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from src.config.config import settings
from src.utils.logger import setup_logger

class SpaceXLoader:
    def __init__(self):
        self.logger = setup_logger("loader")
        self.db_url = settings.DATABASE_URL
        
        # RIGOR: Extrair o caminho físico do banco para garantir que a pasta exista
        self._ensure_db_directory()
        
        self.engine = create_engine(self.db_url)
        self._init_db()

    def _ensure_db_directory(self):
        """Verifica e cria o diretório do banco de dados antes da conexão."""
        # Remove 'sqlite:///' para pegar o path puro
        path_str = self.db_url.replace("sqlite:///", "")
        db_path = Path(path_str).absolute()
        
        db_dir = db_path.parent
        if not db_dir.exists():
            self.logger.info(f"Criando diretório do banco: {db_dir}")
            db_dir.mkdir(parents=True, exist_ok=True)

    def _init_db(self):
        """Inicializa o schema sincronizado com o Transformer."""
        queries = [
            """CREATE TABLE IF NOT EXISTS rockets (
                rocket_id TEXT PRIMARY KEY, name TEXT, type TEXT, active INTEGER, 
                cost_per_launch INTEGER, success_rate_pct INTEGER, 
                height_m REAL, mass_kg REAL);""",
            
            """CREATE TABLE IF NOT EXISTS launches (
                launch_id TEXT PRIMARY KEY, name TEXT, date_utc TEXT, 
                success INTEGER, rocket_id TEXT, flight_number INTEGER, 
                launchpad_id TEXT);""",
            
            """CREATE TABLE IF NOT EXISTS payloads (
                payload_id TEXT PRIMARY KEY, name TEXT, type TEXT, 
                mass_kg REAL, orbit TEXT, reused INTEGER);""",
            
            """CREATE TABLE IF NOT EXISTS launchpads (
                launchpad_id TEXT PRIMARY KEY, full_name TEXT, 
                region TEXT, status TEXT);"""
        ]
        with self.engine.begin() as conn:
            for q in queries:
                conn.execute(text(q))
        self.logger.info("Schema sincronizado e validado.")

    def upsert(self, endpoint: str, df: pd.DataFrame):
        if df.empty: return
        
        table = endpoint
        
        # RIGOR: Mapeamento explícito para evitar erros de plural (launches -> launch_id)
        pk_map = {
            'launches': 'launch_id',
            'rockets': 'rocket_id',
            'payloads': 'payload_id',
            'launchpads': 'launchpad_id'
        }
        
        pk = pk_map.get(table)
        if not pk:
            self.logger.error(f"PK não definida para a tabela {table}")
            return

        cols = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in cols])
        col_names = ", ".join(cols)
        
        # Define quais colunas serão atualizadas em caso de conflito (todas exceto a PK)
        update_cols = [c for c in cols if c != pk]
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

        sql = text(f"""
            INSERT INTO {table} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT({pk}) DO UPDATE SET {set_clause};
        """)

        try:
            with self.engine.begin() as conn:
                records = df.to_dict(orient="records")
                conn.execute(sql, records)
            self.logger.info(f"UPSERT SUCCESS: {table.upper()} ({len(df)} rows)")
        except Exception as e:
            self.logger.error(f"Erro no UPSERT de {table}: {e}")
            raise