from src.db.database import DatabaseManager

class SchemaManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def create_tables(self):
        """Cria as tabelas usando IF NOT EXISTS para garantir idempotência."""
        
        statements = [
            # 1. ROCKETS
            """
            CREATE TABLE IF NOT EXISTS rockets (
                rocket_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT,
                active INTEGER,
                stages INTEGER,
                boosters INTEGER,
                cost_per_launch INTEGER,
                success_rate_pct INTEGER,
                first_flight TEXT,
                country TEXT,
                company TEXT,
                wikipedia TEXT,
                description TEXT,
                height_m REAL,
                diameter_m REAL,
                mass_kg REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """,
            # 2. PAYLOADS
            """
            CREATE TABLE IF NOT EXISTS rocket_payloads (
                rocket_id TEXT,
                orbit TEXT,
                kg INTEGER,
                lb INTEGER,
                PRIMARY KEY (rocket_id, orbit),
                FOREIGN KEY (rocket_id) REFERENCES rockets (rocket_id) ON DELETE CASCADE
            )
            """,
            # 3. IMAGES
            """
            CREATE TABLE IF NOT EXISTS rocket_images (
                rocket_id TEXT NOT NULL,
                url TEXT NOT NULL,
                PRIMARY KEY (rocket_id, url),
                FOREIGN KEY (rocket_id) REFERENCES rockets (rocket_id) ON DELETE CASCADE
            )
            """,
            # 4. ENGINES
            """
            CREATE TABLE IF NOT EXISTS rocket_engines (
                rocket_id TEXT PRIMARY KEY,
                type TEXT,
                version TEXT,
                layout TEXT,
                number INTEGER,
                thrust_sl_kn REAL,
                thrust_vac_kn REAL,
                isp_sl INTEGER,
                isp_vac INTEGER,
                propellant_1 TEXT,
                propellant_2 TEXT,
                FOREIGN KEY (rocket_id) REFERENCES rockets (rocket_id) ON DELETE CASCADE
            )
            """,  
            # 5. LAUNCHES (Note que removi o '#' de dentro da string)
            """
            CREATE TABLE IF NOT EXISTS launches (
                launch_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                date_utc DATETIME,
                rocket_id TEXT,
                success INTEGER,
                flight_number INTEGER,
                details TEXT,
                webcast TEXT,
                reused INTEGER,
                FOREIGN KEY (rocket_id) REFERENCES rockets (rocket_id)
            )
            """
        ]

        for stmt in statements:
            # strip() remove quebras de linha inúteis no início/fim
            self.db_manager.execute_query(stmt.strip())
        
        # Índices para otimização de busca
        self.db_manager.execute_query("CREATE INDEX IF NOT EXISTS idx_rocket_name ON rockets(name)")
        self.db_manager.execute_query("CREATE INDEX IF NOT EXISTS idx_launch_date ON launches(date_utc)")