import pandas as pd
from sqlalchemy import create_engine, text
from src.config.config import settings
from src.utils.logger import setup_logger

class SpaceXLoader:
    """
    Handles data ingestion into the SQLite database.
    Supports a hybrid architecture: 
    - Specialized UPSERTs for core entities (Rockets, Launches).
    - Generic Schema-on-Write for experimental endpoints (Ships, etc.).
    """

    def __init__(self):
        """
        Initializes the engine, logger, and triggers the database bootstrap.
        """
        self.db_url = settings.DATABASE_URL
        self.engine = create_engine(self.db_url, echo=False)
        self.logger = setup_logger("loader")
        
        # Ensures core tables exist before processing
        self._bootstrap_database()
        self.logger.info("SpaceXLoader initialized and schema verified.")

    def _bootstrap_database(self):
        """
        Creates mandatory tables if they do not exist.
        Uses a single transaction to ensure atomicity.
        """
        queries = [
            """
            CREATE TABLE IF NOT EXISTS rockets (
                rocket_id TEXT PRIMARY KEY,
                name TEXT, type TEXT, active BOOLEAN, stages INTEGER,
                cost_per_launch INTEGER, success_rate_pct INTEGER,
                height_m REAL, diameter_m REAL, mass_kg REAL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS launches (
                launch_id TEXT PRIMARY KEY,
                name TEXT, date_utc TEXT, rocket_id TEXT,
                success BOOLEAN, flight_number INTEGER, details TEXT,
                FOREIGN KEY(rocket_id) REFERENCES rockets(rocket_id)
            );
            """
        ]
        
        try:
            with self.engine.begin() as conn:
                for query in queries:
                    conn.execute(text(query))
        except Exception as e:
            self.logger.error(f"Failed to bootstrap database schema: {e}")
            raise

    def load(self, endpoint_name: str, df: pd.DataFrame):
        """
        Orchestrates the loading process. Switches between specialized 
        UPSERT methods and generic Pandas to_sql.
        """
        if df is None or df.empty:
            self.logger.warning(f"No data provided for {endpoint_name}. Skipping load.")
            return

        # Dynamic method discovery
        upsert_method = getattr(self, f"_upsert_{endpoint_name}", None)

        try:
            if upsert_method:
                self.logger.info(f"Applying RIGID LOAD (Upsert) for: {endpoint_name}")
                records = df.to_dict(orient='records')
                # begin() handles transaction COMMIT/ROLLBACK automatically
                with self.engine.begin() as conn:
                    upsert_method(conn, records)
            else:
                self.logger.warning(f"Specialized method not found. Using GENERIC LOAD for: {endpoint_name}")
                df.to_sql(
                    name=endpoint_name,
                    con=self.engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )
            
            self.logger.info(f"Successfully processed {endpoint_name}.")
            
        except Exception as e:
            self.logger.error(f"Critical error loading {endpoint_name}: {e}")
            raise

    # --- SPECIALIZED UPSERT METHODS ---

    def _upsert_rockets(self, conn, records: list):
        """
        Performs an Upsert for the rockets table.
        Ensures data sanitization against missing API keys.
        """
        required_fields = [
            'rocket_id', 'name', 'type', 'active', 'stages',
            'cost_per_launch', 'success_rate_pct', 'height_m', 
            'diameter_m', 'mass_kg'
        ]
        
        sanitized = [{f: r.get(f) for f in required_fields} for r in records]

        sql = text("""
            INSERT INTO rockets (
                rocket_id, name, type, active, stages,
                cost_per_launch, success_rate_pct, height_m, diameter_m, mass_kg
            ) VALUES (
                :rocket_id, :name, :type, :active, :stages,
                :cost_per_launch, :success_rate_pct, :height_m, :diameter_m, :mass_kg
            ) ON CONFLICT(rocket_id) DO UPDATE SET
                name=EXCLUDED.name,
                active=EXCLUDED.active,
                cost_per_launch=EXCLUDED.cost_per_launch;
        """)
        conn.execute(sql, sanitized)

    def _upsert_launches(self, conn, records: list):
        """
        Performs an Upsert for the launches table.
        """
        required_fields = ['launch_id', 'name', 'date_utc', 'rocket_id', 'success', 'flight_number']
        
        sanitized = [{f: r.get(f) for f in required_fields} for r in records]

        sql = text("""
            INSERT INTO launches (launch_id, name, date_utc, rocket_id, success, flight_number)
            VALUES (:launch_id, :name, :date_utc, :rocket_id, :success, :flight_number)
            ON CONFLICT(launch_id) DO UPDATE SET
                success=EXCLUDED.success,
                name=COALESCE(EXCLUDED.name, launches.name);
        """)
        conn.execute(sql, sanitized)