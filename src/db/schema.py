from src.db.database import DatabaseManager


class SchemaManager:

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def drop_all(self):

        tables = [
            "rocket_payloads",
            "rocket_engines",
            "rocket_images",
            "rockets"
        ]

        for table in tables:
            self.db_manager.execute_query(
                f"DROP TABLE IF EXISTS {table}"
            )

    def create_tables(self):

        statements = [

            
            # ROCKETS
            
            """
            CREATE TABLE IF NOT EXISTS rockets (

                rocket_id TEXT PRIMARY KEY,

                name TEXT,
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
                mass_kg REAL
            )
            """,

            
            # PAYLOADS
            
            """
            CREATE TABLE IF NOT EXISTS rocket_payloads (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                rocket_id TEXT,

                orbit TEXT,

                kg REAL,
                lb REAL,

                FOREIGN KEY (rocket_id)
                    REFERENCES rockets (rocket_id)
            )
            """,

            
            # IMAGES
            
            """
            CREATE TABLE IF NOT EXISTS rocket_images (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                rocket_id TEXT,

                url TEXT,

                FOREIGN KEY (rocket_id)
                    REFERENCES rockets (rocket_id)
            )
            """,

            
            # ENGINES
            
            """
            CREATE TABLE IF NOT EXISTS rocket_engines (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                rocket_id TEXT,

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

                FOREIGN KEY (rocket_id)
                    REFERENCES rockets (rocket_id)
            )
            """
        ]

        for stmt in statements:
            self.db_manager.execute_query(stmt)
        print("Tabelas criadas com sucesso.")