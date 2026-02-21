import requests
import structlog
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

from src.extract.schemas import ENDPOINT_SCHEMAS
from src.config.settings import settings

logger = structlog.get_logger()


class SpaceXExtractor:

    def __init__(self):
        self.session = self._setup_session()

    def _setup_session(self) -> requests.Session:
        session = requests.Session()

        retries = Retry(
            total=settings.API_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)

        return session

    def _parse_date(self, value: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    def fetch(
        self,
        endpoint: str,
        incremental: bool = False,
        last_ingested: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:

        url = f"{settings.SPACEX_API_URL}/{endpoint}"

        try:
            logger.info(
                "Iniciando extração",
                endpoint=endpoint,
                incremental=incremental
            )

            response = self.session.get(
                url,
                timeout=settings.API_TIMEOUT
            )

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError("Resposta da API não é uma lista")

            # -----------------------------
            # FILTRAGEM INCREMENTAL
            # -----------------------------
            if incremental and last_ingested:

                filtered = []

                for item in data:
                    date_str = item.get("date_utc")

                    if not date_str:
                        continue

                    parsed = self._parse_date(date_str)

                    if parsed and parsed > last_ingested:
                        filtered.append(item)

                logger.info(
                    "Filtro incremental aplicado",
                    endpoint=endpoint,
                    original=len(data),
                    filtered=len(filtered)
                )

                data = filtered

            # -----------------------------
            # VALIDAÇÃO DE SCHEMA
            # -----------------------------
            schema = ENDPOINT_SCHEMAS.get(endpoint)

            if schema:
                validated = []

                for item in data:
                    validated.append(
                        schema(**item).model_dump()
                    )

                data = validated

            else:
                logger.warning(
                    "Schema não encontrado",
                    endpoint=endpoint
                )

            logger.info(
                "Extração finalizada",
                endpoint=endpoint,
                count=len(data)
            )

            return data

        except requests.exceptions.Timeout:
            logger.error(
                "Timeout na API",
                endpoint=endpoint
            )
            raise

        except requests.exceptions.HTTPError as e:
            logger.error(
                "Erro HTTP",
                endpoint=endpoint,
                status=e.response.status_code
            )
            raise

        except Exception as e:
            logger.exception(
                "Erro inesperado na extração",
                endpoint=endpoint,
                error=str(e)
            )
            raise