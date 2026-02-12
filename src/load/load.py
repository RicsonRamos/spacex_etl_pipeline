from sqlalchemy import text
from src.logger import setup_logger

class SpaceXLoader:
    def __init__(self, engine):
        self.engine = engine
        self.logger = setup_logger("loader")

    def load_tables(self, tables_dict):
        """Orquestra a carga de todas as tabelas em uma única transação."""
        try:
            with self.engine.begin() as conn:
                # 1. Dimensão Principal (Deve ser a primeira)
                self.logger.info("Iniciando carga: Rockets")
                self._upsert_rockets(conn, tables_dict["rockets"])
                
                # 2. Tabelas de Detalhes (Dependentes de Rockets)
                self.logger.info("Iniciando carga: Payloads")
                self._upsert_payloads(conn, tables_dict["payloads"])
                
                self.logger.info("Iniciando carga: Images")
                self._upsert_images(conn, tables_dict["images"])
                
                self.logger.info("Iniciando carga: Engines")
                self._upsert_engines(conn, tables_dict["engines"])

                # 3. Tabela de Fatos (Dependente de Rockets)
                # ADICIONADO: Chamada para processar os lançamentos
                self.logger.info("Iniciando carga: Launches")
                self._upsert_launches(conn, tables_dict["launches"])
                
            self.logger.info("Carga concluída com sucesso (Upsert).")
        except Exception as e:
            self.logger.error(f"Falha crítica durante o Load: {e}")
            raise

    def _upsert_rockets(self, conn, records):
        if not records: return
        sql = text("""
            INSERT INTO rockets (
                rocket_id, name, type, active, stages, boosters,
                cost_per_launch, success_rate_pct, first_flight,
                country, company, wikipedia, description,
                height_m, diameter_m, mass_kg
            )
            VALUES (
                :rocket_id, :name, :type, :active, :stages, :boosters,
                :cost_per_launch, :success_rate_pct, :first_flight,
                :country, :company, :wikipedia, :description,
                :height_m, :diameter_m, :mass_kg
            )
            ON CONFLICT(rocket_id) DO UPDATE SET
                name=EXCLUDED.name, type=EXCLUDED.type, active=EXCLUDED.active,
                stages=EXCLUDED.stages, boosters=EXCLUDED.boosters,
                cost_per_launch=EXCLUDED.cost_per_launch, 
                success_rate_pct=EXCLUDED.success_rate_pct,
                first_flight=EXCLUDED.first_flight, country=EXCLUDED.country,
                company=EXCLUDED.company, wikipedia=EXCLUDED.wikipedia,
                description=EXCLUDED.description, height_m=EXCLUDED.height_m,
                diameter_m=EXCLUDED.diameter_m, mass_kg=EXCLUDED.mass_kg;
        """)
        conn.execute(sql, records)

    def _upsert_payloads(self, conn, records):
        if not records: return
        sql = text("""
            INSERT INTO rocket_payloads (rocket_id, orbit, kg, lb)
            VALUES (:rocket_id, :orbit, :kg, :lb)
            ON CONFLICT(rocket_id, orbit) DO UPDATE SET
                kg=EXCLUDED.kg, lb=EXCLUDED.lb;
        """)
        conn.execute(sql, records)

    def _upsert_images(self, conn, records):
        if not records: return
        sql = text("""
            INSERT INTO rocket_images (rocket_id, url)
            VALUES (:rocket_id, :url)
            ON CONFLICT(rocket_id, url) DO NOTHING;
        """)
        conn.execute(sql, records)

    def _upsert_engines(self, conn, records):
        if not records: return
        sql = text("""
            INSERT INTO rocket_engines (
                rocket_id, type, version, layout, number,
                thrust_sl_kn, thrust_vac_kn, isp_sl, isp_vac,
                propellant_1, propellant_2
            )
            VALUES (
                :rocket_id, :type, :version, :layout, :number,
                :thrust_sl_kn, :thrust_vac_kn, :isp_sl, :isp_vac,
                :propellant_1, :propellant_2
            )
            ON CONFLICT(rocket_id) DO UPDATE SET
                type=EXCLUDED.type, version=EXCLUDED.version,
                layout=EXCLUDED.layout, number=EXCLUDED.number,
                thrust_sl_kn=EXCLUDED.thrust_sl_kn,
                thrust_vac_kn=EXCLUDED.thrust_vac_kn,
                isp_sl=EXCLUDED.isp_sl, isp_vac=EXCLUDED.isp_vac,
                propellant_1=EXCLUDED.propellant_1,
                propellant_2=EXCLUDED.propellant_2;
        """)
        conn.execute(sql, records)

    def _upsert_launches(self, conn, records):
        if not records: return
        sql = text("""
            INSERT INTO launches (
                launch_id, name, date_utc, rocket_id, 
                success, flight_number, details, webcast, reused
            )
            VALUES (
                :launch_id, :name, :date_utc, :rocket_id, 
                :success, :flight_number, :details, :webcast, :reused
            )
            ON CONFLICT(launch_id) DO UPDATE SET
                success=EXCLUDED.success,
                details=EXCLUDED.details,
                webcast=EXCLUDED.webcast;
        """)
        conn.execute(sql, records)