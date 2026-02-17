import re
import structlog
import polars as pl
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from src.config.settings import settings
from src.database.models import Base, ETLMetrics
import requests

# Logger configuration
logger = structlog.get_logger()

# Alert Handler Class to send Slack notifications
class AlertHandler:
    """Simple alert handler to send notifications to Slack."""

    def __init__(self, slack_webhook_url: str = None):
        self.slack_webhook_url = slack_webhook_url

    def send_slack_alert(self, message: str):
        """Send an alert to Slack if webhook URL is configured."""
        if not self.slack_webhook_url:
            return
        payload = {"text": message}
        try:
            requests.post(self.slack_webhook_url, json=payload, timeout=5)
        except Exception as exc:
            logger.error("Failed to send Slack alert", error=str(exc))


class PostgresLoader:
    def __init__(self, alert_handler: AlertHandler = None) -> None:
        """Initializes the Postgres loader."""
        self.engine = create_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            future=True
        )
        self.Session = sessionmaker(bind=self.engine)
        self.alert_handler = alert_handler

    def log_metrics(
        self,
        table_name: str,
        stage: str,
        rows_processed: int,
        status: str,
        start_time: datetime,
        end_time: datetime,
        error: str = None
    ) -> None:
        """Log ETL metrics into the database using ORM."""
        session = self.Session()

        # Create ETLMetrics instance
        metrics = ETLMetrics(
            table_name=table_name,
            stage=stage,
            rows_processed=rows_processed,
            status=status,
            start_time=start_time,
            end_time=end_time,
            error=error
        )

        try:
            session.add(metrics)
            session.commit()
        except Exception as exc:
            session.rollback()
            logger.error(f"Failed to log metrics: {exc}")
        finally:
            session.close()

    def ensure_tables(self) -> None:
        """Ensure ORM tables exist in the database."""
        Base.metadata.create_all(self.engine)

    def ensure_staging_table(self) -> None:
        """Ensure staging table exists in the database."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS staging_launches (
            id SERIAL PRIMARY KEY,
            launch_id TEXT,
            name TEXT,
            date_utc TIMESTAMP,
            raw_data JSONB,
            ingestion_time TIMESTAMP DEFAULT NOW()
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info(
            "Staging table ensured",
            stage="staging",
            table="staging_launches",
            timestamp=datetime.now().isoformat()
        )

    def _validate_identifier(self, name: str) -> None:
        """Basic protection against SQL injection for table names/columns."""
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
            raise ValueError(f"Invalid identifier: {name}")

    def upsert_dataframe(self, df: pl.DataFrame, table_name: str, pk_col: str) -> None:
        """Perform UPSERT operation (INSERT ON CONFLICT DO UPDATE) using Polars DataFrame."""
        if df.is_empty():
            logger.warning(
                "Upsert skipped: empty DataFrame",
                table=table_name,
                stage="final"
            )
            return

        # Validate table name and primary key column
        self._validate_identifier(table_name)
        self._validate_identifier(pk_col)

        # Ensure tables are created before performing upsert
        self.ensure_tables()

        records = df.to_dicts()
        columns = df.columns

        # Generate update clause excluding the primary key column
        update_clause = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in columns if col != pk_col
        )

        # SQL insert statement with upsert logic (ON CONFLICT DO UPDATE)
        insert_stmt = text(f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({", ".join(f":{col}" for col in columns)})
            ON CONFLICT ({pk_col})
            DO UPDATE SET {update_clause};
        """)

        start_time = datetime.now()
        try:
            with self.engine.begin() as conn:
                conn.execute(insert_stmt, records)

            logger.info(
                "Upsert completed",
                stage="final",
                table=table_name,
                rows=len(records),
                timestamp=datetime.now().isoformat()
            )

        except SQLAlchemyError as exc:
            logger.error(
                "Critical failure during upsert",
                stage="final",
                table=table_name,
                error=str(exc),
                timestamp=datetime.now().isoformat()
            )
            if self.alert_handler:
                self.alert_handler.send_slack_alert(
                    f"ETL Failure on table {table_name}: {exc}"
                )
            raise
        finally:
            end_time = datetime.now()
            self.log_metrics(
                table_name=table_name,
                stage="final",
                rows_processed=len(records),
                status="success" if not exc else "failure",
                start_time=start_time,
                end_time=end_time,
                error=str(exc) if exc else None
            )

    def load_to_staging(self, df: pl.DataFrame) -> None:
        """Load raw data into the staging table."""
        start_time = datetime.now()
        rows = len(df)
        status = "success"
        error_msg = None

        try:
            if df.is_empty():
                logger.warning(
                    "Load skipped: empty DataFrame for staging",
                    stage="staging",
                    table="staging_launches"
                )
                rows = 0
                return

            self.ensure_staging_table()
            records = df.to_dicts()

            # Keep a copy of raw data
            for rec in records:
                rec['raw_data'] = rec.copy()

            columns = list(records[0].keys())
            insert_stmt = text(f"""
                INSERT INTO staging_launches ({', '.join(columns)})
                VALUES ({', '.join(f':{col}' for col in columns)});
            """)

            with self.engine.begin() as conn:
                conn.execute(insert_stmt, records)

            logger.info(
                "Staging load completed",
                stage="staging",
                table="staging_launches",
                rows=rows,
                timestamp=datetime.now().isoformat()
            )

        except SQLAlchemyError as exc:
            status = "failure"
            error_msg = str(exc)
            logger.error(
                "Critical failure during staging load",
                stage="staging",
                table="staging_launches",
                error=error_msg,
                timestamp=datetime.now().isoformat()
            )
            if self.alert_handler:
                self.alert_handler.send_slack_alert(
                    f"ETL Failure during staging load: {error_msg}"
                )
            raise

        finally:
            end_time = datetime.now()
            self.log_metrics(
                table_name="staging_launches",
                stage="staging",
                rows_processed=rows,
                status=status,
                start_time=start_time,
                end_time=end_time,
                error=error_msg
            )

    def process_staging_to_final(self, final_table: str, pk_col: str) -> None:
        """Move data from staging to the final table using upsert."""
        self._validate_identifier(final_table)
        self._validate_identifier(pk_col)

        # Read data from staging table
        df = pl.read_sql("SELECT launch_id, name, date_utc FROM staging_launches", self.engine)

        # Example validation: discard rows without primary key
        df_clean = df.filter(df['launch_id'].is_not_null())

        # Perform upsert into final table
        self.upsert_dataframe(df_clean, final_table, pk_col)

        logger.info(f"Upsert iniciado para a tabela: {final_table}, PK: {pk_col}")


# Create AlertHandler instance for Slack alerts
alerts = AlertHandler(slack_webhook_url=settings.SLACK_WEBHOOK_URL)

# Initialize the Postgres loader with the alert handler
loader = PostgresLoader(alert_handler=alerts)
