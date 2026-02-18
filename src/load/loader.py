import re
import structlog
import json
import polars as pl
from datetime import datetime, date  # Adicionado date para rigor
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from src.config.settings import settings
from src.database.models import Base, ETLMetrics
import requests

logger = structlog.get_logger()

# RIGOR: Função auxiliar para serialização JSON
def json_serial(obj):
    """Serializador JSON para tipos não suportados nativamente (datetime, date)."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

class AlertHandler:
    def __init__(self, slack_webhook_url: str = None):
        self.slack_webhook_url = slack_webhook_url

    def send_slack_alert(self, message: str):
        if not self.slack_webhook_url: return
        try:
            requests.post(self.slack_webhook_url, json={"text": message}, timeout=5)
        except Exception as e:
            logger.error("Failed to send Slack alert", error=str(e))

class PostgresLoader:
    def __init__(self, alert_handler: AlertHandler = None) -> None:
        self.engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, future=True)
        self.Session = sessionmaker(bind=self.engine)
        self.alert_handler = alert_handler

    def _validate_identifier(self, name: str) -> None:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
            raise ValueError(f"Invalid identifier: {name}")

    def log_metrics(self, table_name: str, stage: str, rows_processed: int, status: str, 
                    start_time: datetime, end_time: datetime, error: str = None) -> None:
        session = self.Session()
        try:
            metrics = ETLMetrics(
                table_name=table_name, stage=stage, rows_processed=rows_processed,
                status=status, start_time=start_time, end_time=end_time, error=error
            )
            session.add(metrics)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to log metrics: {e}")
        finally:
            session.close()

    def load_to_bronze(self, data: List[Dict[str, Any]], endpoint: str) -> None:
        table_name = f"bronze_{endpoint}"
        start_time = datetime.now()
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, raw_data JSONB, extracted_at TIMESTAMP DEFAULT NOW())"
        
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
                
                # CORREÇÃO RIGOROSA: default=json_serial resolve o erro de 'datetime is not JSON serializable'
                params = [{"raw_data": json.dumps(d, default=json_serial)} for d in data]
                
                conn.execute(
                    text(f"INSERT INTO {table_name} (raw_data) VALUES (:raw_data)"), 
                    params
                )
            self.log_metrics(table_name, "bronze", len(data), "success", start_time, datetime.now())
        except Exception as e:
            self.log_metrics(table_name, "bronze", len(data), "failure", start_time, datetime.now(), str(e))
            raise

    def load_to_silver(self, df: pl.DataFrame, table_name: str, pk_col: str) -> None:
        full_table_name = f"silver_{table_name}"
        self.upsert_dataframe(df, full_table_name, pk_col)

    def upsert_dataframe(self, df: pl.DataFrame, table_name: str, pk_col: str) -> None:
        if df.is_empty(): return
        self._validate_identifier(table_name)
        self._validate_identifier(pk_col)
        
        # Garante criação das tabelas baseadas no models.py
        Base.metadata.create_all(self.engine)
        
        columns = df.columns
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != pk_col)
        insert_stmt = text(f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({", ".join(f":{c}" for c in columns)})
            ON CONFLICT ({pk_col}) DO UPDATE SET {update_clause};
        """)
        
        start_time = datetime.now()
        error_to_log = None
        try:
            with self.engine.begin() as conn:
                conn.execute(insert_stmt, df.to_dicts())
            logger.info("Upsert completed", table=table_name)
        except SQLAlchemyError as e:
            error_to_log = str(e)
            if self.alert_handler: self.alert_handler.send_slack_alert(f"Fail {table_name}: {e}")
            raise 
        finally:
            self.log_metrics(table_name, "silver", len(df), "success" if not error_to_log else "failure",
                            start_time, datetime.now(), error_to_log)

    def refresh_gold_layer(self):
        """
        Garante a atualização da camada Gold deletando a anterior 
        para evitar conflitos de definição de colunas.
        """
        query = """
            -- Passo 1: Remover a view antiga para resetar o esquema
            DROP VIEW IF EXISTS gold_cost_efficiency_metrics;

            -- Passo 2: Criar a nova estrutura com as colunas necessárias
            CREATE VIEW gold_cost_efficiency_metrics AS
            WITH launch_stats AS (
                SELECT 
                    r.name AS rocket_name,
                    COUNT(l.launch_id) AS total_launches,
                    (SUM(CASE WHEN l.success THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(l.launch_id), 0)) * 100 AS success_rate,
                    AVG(r.cost_per_launch) AS avg_rocket_cost
                FROM silver_rockets r
                LEFT JOIN silver_launches l ON r.rocket_id = l.rocket_id
                GROUP BY r.name
            ),
            payload_stats AS (
                SELECT 
                    r.name AS rocket_name,
                    SUM(p.mass_kg) AS total_kg_delivered
                FROM silver_rockets r
                JOIN silver_launches l ON r.rocket_id = l.rocket_id
                JOIN silver_payloads p ON l.launch_id = p.launch_id
                GROUP BY r.name
            )
            SELECT 
                ls.rocket_name,
                ls.total_launches,
                ls.success_rate,
                ls.avg_rocket_cost,
                COALESCE(ps.total_kg_delivered, 0) AS total_kg_delivered,
                CASE 
                    WHEN COALESCE(ps.total_kg_delivered, 0) > 0 
                    THEN ls.avg_rocket_cost / ps.total_kg_delivered 
                    ELSE 0 
                END AS avg_cost_per_kg
            FROM launch_stats ls
            LEFT JOIN payload_stats ps ON ls.rocket_name = ps.rocket_name;
        """
        # Execução via SQLAlchemy (certifique-se de estar dentro de uma transação ou conexão)
        with self.engine.begin() as conn:
            conn.execute(text(query))
            logger.info("Gold layer refreshed", view="gold_cost_efficiency_metrics")
# Instâncias globais
alerts = AlertHandler(slack_webhook_url=settings.SLACK_WEBHOOK_URL)
loader = PostgresLoader(alert_handler=alerts)