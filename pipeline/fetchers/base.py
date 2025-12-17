"""Classe de base pour les fetchers."""
import time
from abc import ABC, abstractmethod
from typing import Generator
import httpx
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import logging
from tqdm import tqdm

from ..config import APIConfig

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """Classe abstraite pour les fetchers d'API."""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.stats = {
            "requests_made": 0,
            "requests_failed": 0,
            "items_fetched": 0,
            "start_time": None,
            "end_time": None,
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """Effectue une requête avec retry automatique."""
        url = f"{self.config.base_url}{endpoint}"
        
        with httpx.Client(
            timeout=self.config.timeout,
            headers=self.config.headers
        ) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            self.stats["requests_made"] += 1
            return response.json()
    
    def _rate_limit(self):
        """Applique le rate limiting."""
        time.sleep(self.config.rate_limit)
    
    @abstractmethod
    def fetch_batch(self, **kwargs) -> list[dict]:
        """Récupère un lot de données. À implémenter."""
        pass
    
    @abstractmethod
    def fetch_all(self, **kwargs) -> Generator[dict, None, None]:
        """Récupère toutes les données avec pagination. À implémenter."""
        pass
    
    def get_stats(self) -> dict:
        """Retourne les statistiques d'acquisition."""
        return self.stats.copy()