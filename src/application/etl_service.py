from datetime import datetime, timezone
from typing import Optional
import structlog

logger = structlog.get_logger()


class ETLService:
    """
    Serviço responsável por orquestrar o fluxo ETL.

    Princípios aplicados:
    - SRP: Apenas orquestra
    - DIP: Depende de abstrações injetadas
    - Sem conhecimento de nomes físicos de tabela
    """

    def __init__(
        self,
        entity: str,
        extractor,
        transformer,
        bronze_loader,
        silver_loader,
        watermark,
        metrics,
        notifier,
        schema_validator,
        incremental: bool,
        real_api: bool,
    ):
        self.entity = entity
        self.extractor = extractor
        self.transformer = transformer
        self.bronze_loader = bronze_loader
        self.silver_loader = silver_loader
        self.watermark = watermark
        self.metrics = metrics
        self.notifier = notifier
        self.schema_validator = schema_validator
        self.incremental = incremental
        self.real_api = real_api

    
    # PRIVATE HELPERS
    

    def _get_last_ingested(self) -> Optional[datetime]:
        """
        Retorna última data processada (watermark).
        Watermark já sabe qual tabela consultar.
        """
        if not self.incremental:
            return None

        last = self.watermark.get_last_ingested()

        if last and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)

        return last

    def _validate_schema(self) -> None:
        """
        Valida schema da tabela Silver.
        O loader conhece o nome físico da tabela.
        """
        expected = getattr(self.silver_loader, "expected_schema", None)

        if not expected:
            logger.warning(
                "No expected_schema defined, skipping validation",
                entity=self.entity,
            )
            return

        self.schema_validator.validate_table_columns(
            table_name=self.silver_loader.table_name,
            expected_columns=expected,
        )

    
    # PUBLIC EXECUTION
    

    def run(self) -> int:
        """
        Executa pipeline ETL completo.

        Returns:
            int: Número de linhas processadas na camada Silver.
        """
        try:
            # Validate schema (Silver)
            self._validate_schema()

            # Incremental watermark
            last_date = self._get_last_ingested()

            # Extract
            raw_data = self.extractor.extract(real_api=self.real_api)

            if not raw_data:
                logger.info("Empty dataset", entity=self.entity)
                return 0

            self.metrics.inc_extract(self.entity, len(raw_data))

            # Bronze load (raw JSON)
            self.bronze_loader.load(
                raw_data=raw_data,
                source="spacex_api_v5",
            )

            # Transform
            df_silver = self.transformer.transform(
                raw_data,
                last_ingested=last_date,
            )

            if df_silver.is_empty():
                logger.info(
                    "No new records after transformation",
                    entity=self.entity,
                )
                return 0

            # Silver upsert
            rows = self.silver_loader.upsert(df_silver)
            self.metrics.inc_silver(self.entity, rows)

            # Update watermark
            if self.incremental and rows > 0:
                max_date = df_silver["date_utc"].max()
                self.watermark.update(max_date)

            logger.info(
                "ETL completed successfully",
                entity=self.entity,
                rows_processed=rows,
            )

            return rows

        except Exception as e:
            logger.error(
                "ETL error",
                entity=self.entity,
                error=str(e),
            )

            self.notifier.notify(
                f"Critical pipeline failure SpaceX: {self.entity} - {str(e)}"
            )

            raise