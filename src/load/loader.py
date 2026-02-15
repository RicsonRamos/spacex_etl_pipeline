import pandas as pd
import time
from pathlib import Path
from sqlalchemy import create_engine, text
from src.config.config import settings
from src.utils.logger import setup_logger


class SpaceXLoader:
    """
    High-performance loader with batch upsert and SQLite tuning.
    """

    BATCH_SIZE = 100  # You can move this to settings

    def __init__(self):
        self.logger = setup_logger("loader")
        self.db_url = settings.DATABASE_URL

        self._ensure_db_directory()

        # Performance-oriented engine config
        self.engine = create_engine(
            self.db_url,
            future=True,
        )

        self._init_db()
        self._apply_sqlite_pragmas()

    def _ensure_db_directory(self):
        db_path = Path(self.db_url.replace("sqlite:///", "")).absolute()
        db_path.parent.mkdir(parents=True, exist_ok=True)

    def _apply_sqlite_pragmas(self):
        """Apply SQLite performance optimizations."""
        with self.engine.begin() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL;"))
            conn.execute(text("PRAGMA synchronous=NORMAL;"))
            conn.execute(text("PRAGMA temp_store=MEMORY;"))
            conn.execute(text("PRAGMA cache_size=10000;"))

    def _init_db(self):
        """Initialize database schema with all required tables."""
        queries = [
            """CREATE TABLE IF NOT EXISTS rockets (
                rocket_id TEXT PRIMARY KEY, name TEXT, type TEXT, active INTEGER, 
                cost_per_launch INTEGER, success_rate_pct INTEGER, 
                height_m REAL, mass_kg REAL
            );""",
            """CREATE TABLE IF NOT EXISTS launches (
                launch_id TEXT PRIMARY KEY, name TEXT, date_utc TEXT, 
                success INTEGER, rocket_id TEXT, flight_number INTEGER, 
                launchpad_id TEXT
            );""",
            """CREATE TABLE IF NOT EXISTS payloads (
                payload_id TEXT PRIMARY KEY, name TEXT, type TEXT, 
                mass_kg REAL, orbit TEXT, reused INTEGER
            );""",
            """CREATE TABLE IF NOT EXISTS launchpads (
                launchpad_id TEXT PRIMARY KEY, full_name TEXT, 
                region TEXT, status TEXT
            );""",
            """CREATE TABLE IF NOT EXISTS landpads (
                landpad_id TEXT PRIMARY KEY, full_name TEXT,
                type TEXT, locality TEXT, region TEXT, status TEXT
            );"""
        ]

        with self.engine.begin() as conn:
            for query in queries:
                conn.execute(text(query))

            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_launches_rocket ON launches(rocket_id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_launches_launchpad ON launches(launchpad_id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_payloads_orbit ON payloads(orbit);"))

        self.logger.info("Schema synchronized and optimized.")

    def _normalize_records(self, records: list[dict]) -> list[dict]:
        """
        Replace dots in dictionary keys with underscores for SQL compatibility.
        """
        normalized = []
        for r in records:
            new_r = {k.replace(".", "_"): v for k, v in r.items()}
            normalized.append(new_r)
        return normalized

    def upsert(self, endpoint: str, df: pd.DataFrame):
        """
        Batch UPSERT with chunking and timing metrics.
        Normalizes column names to avoid SQLAlchemy bind errors.
        """
        if df.empty:
            self.logger.info(f"No data to upsert for {endpoint}.")
            return

        # Mapeamento de colunas específicas da API para o DB
        rename_maps = {
            "rockets": {"height.meters": "height_m", "mass.kg": "mass_kg"},
            # se outros endpoints tiverem colunas com pontos, adicione aqui
        }

        if endpoint in rename_maps:
            df = df.rename(columns=rename_maps[endpoint])

        # Substituir quaisquer outros pontos genéricos
        df.columns = [c.replace(".", "_") for c in df.columns]

        pk_map = {
            "launches": "launch_id",
            "rockets": "rocket_id",
            "payloads": "payload_id",
            "launchpads": "launchpad_id",
            "landpads": "landpad_id"
        }

        pk = pk_map.get(endpoint)
        if not pk:
            raise ValueError(f"No primary key defined for {endpoint}")

        columns = list(df.columns)
        update_columns = [c for c in columns if c != pk]

        insert_clause = ", ".join(columns)
        values_clause = ", ".join([f":{c}" for c in columns])
        update_clause = ", ".join([f"{c}=excluded.{c}" for c in update_columns])

        sql = text(f"""
            INSERT INTO {endpoint} ({insert_clause})
            VALUES ({values_clause})
            ON CONFLICT({pk})
            DO UPDATE SET {update_clause};
        """)

        records = df.to_dict(orient="records")
        records = self._normalize_records(records)  # ensure no dots

        start_time = time.time()
        total = len(records)

        try:
            with self.engine.begin() as conn:
                for i in range(0, total, self.BATCH_SIZE):
                    batch = records[i:i + self.BATCH_SIZE]
                    conn.execute(sql, batch)

            elapsed = round(time.time() - start_time, 3)
            self.logger.info(
                f"UPSERT SUCCESS: {endpoint.upper()} "
                f"({total} rows) | Time: {elapsed}s | "
                f"Batch size: {self.BATCH_SIZE}"
            )

        except Exception as e:
            self.logger.error(f"UPSERT failed for {endpoint}: {e}")
            raise
