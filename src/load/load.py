from sqlalchemy import create_engine, text
from src.config.config import settings
from src.utils.logger import setup_logger

class SpaceXLoader:
    def __init__(self):
        self.db_url = settings.DATABASE_URL
        self.engine = create_engine(self.db_url)
        self.logger = setup_logger("loader")
        
        # BOOTSTRAP: Cria as tabelas íntegras antes de qualquer carga
        self._create_tables_if_not_exists()
        self.logger.info(f"SpaceXLoader inicializado e tabelas verificadas.")

    def _create_tables_if_not_exists(self):
        """Garante que o schema íntegro exista no SQLite."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS rockets (
                rocket_id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                active BOOLEAN,
                stages INTEGER,
                cost_per_launch INTEGER,
                success_rate_pct INTEGER,
                height_m REAL,
                diameter_m REAL,
                mass_kg REAL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS launches (
                launch_id TEXT PRIMARY KEY,
                name TEXT,
                date_utc TEXT,
                rocket_id TEXT,
                success BOOLEAN,
                flight_number INTEGER,
                details TEXT,
                FOREIGN KEY(rocket_id) REFERENCES rockets(rocket_id)
            );
            """
        ]
        with self.engine.begin() as conn:
            for query in queries:
                conn.execute(text(query))

    def load(self, endpoint_name, df):
        """
        O Cérebro do Loader: Decide entre carga Íntegra (UPSERT) 
        ou Genérica (Pandas to_sql).
        """
        if df is None or df.empty:
            self.logger.warning(f"Dados vazios para {endpoint_name}. Ignorando.")
            return

        # Busca se existe um método de UPSERT específico para este endpoint
        # Ex: se endpoint_name for 'rockets', procura '_upsert_rockets'
        method_name = f"_upsert_{endpoint_name}"
        upsert_method = getattr(self, method_name, None)

        try:
            if upsert_method:
                self.logger.info(f"Utilizando carga ÍNTEGRA (Upsert) para: {endpoint_name}")
                records = df.to_dict(orient='records')
                with self.engine.begin() as conn:
                    upsert_method(conn, records)
            else:
                self.logger.warning(f"Método especializado não encontrado. Usando carga GENÉRICA para: {endpoint_name}")
                self.load_generic(endpoint_name, df)
            
            self.logger.info(f"✅ Processamento de {endpoint_name} concluído.")
            
        except Exception as e:
            self.logger.error(f"❌ Falha crítica na carga de {endpoint_name}: {e}")
            raise

    def load_generic(self, table_name, df):
        """Cria tabelas dinamicamente para novos endpoints."""
        try:
            with self.engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False
                )
        except Exception as e:
            self.logger.error(f"Erro na carga genérica de {table_name}: {e}")
            raise

    # --- MÉTODOS DE UPSERT (RIGOR E INTEGRIDADE) ---
    
    def _upsert_rockets(self, conn, records):
        if not records: return
        
        # Lista de campos que sua query SQL exige
        required_fields = [
            'rocket_id', 'name', 'type', 'active', 'stages',
            'cost_per_launch', 'success_rate_pct', 'height_m', 
            'diameter_m', 'mass_kg'
        ]
        
        # Higienização: Garante que todos os campos existam no dict, mesmo que como None
        sanitized_records = []
        for rec in records:
            sanitized_records.append({field: rec.get(field) for field in required_fields})

        sql = text("""
            INSERT INTO rockets (
                rocket_id, name, type, active, stages,
                cost_per_launch, success_rate_pct, height_m, diameter_m, mass_kg        
            )
            VALUES (
                :rocket_id, :name, :type, :active, :stages,
                :cost_per_launch, :success_rate_pct, :height_m, :diameter_m, :mass_kg
            )
            ON CONFLICT(rocket_id) DO UPDATE SET
                name=EXCLUDED.name, active=EXCLUDED.active,
                cost_per_launch=EXCLUDED.cost_per_launch;
        """)
        conn.execute(sql, sanitized_records)

    def _upsert_launches(self, conn, records):
        if not records: return
        sql = text("""
            INSERT INTO launches (launch_id, name, date_utc, rocket_id, success, flight_number)
            VALUES (:launch_id, :name, :date_utc, :rocket_id, :success, :flight_number)
            ON CONFLICT(launch_id) DO UPDATE SET
                success=EXCLUDED.success,
                name=EXCLUDED.name;
        """)
        conn.execute(sql, records)