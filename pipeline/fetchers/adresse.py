"""Fetcher pour l'API Adresse (géocodage)."""
from typing import Generator
from tqdm import tqdm

from .base import BaseFetcher
from ..config import ADRESSE_CONFIG
from ..models import GeocodingResult


class AdresseFetcher(BaseFetcher):
    """Fetcher pour l'API Adresse (géocodage)."""
    
    def __init__(self):
        super().__init__(ADRESSE_CONFIG)
    
    def geocode_single(self, address: str) -> GeocodingResult:
        """Géocode une adresse unique."""
        if not address or address.strip() == "":
            return GeocodingResult(original_address=address or "", score=0)
        
        try:
            data = self._make_request("/search/", params={"q": address, "limit": 1})
            
            if not data.get("features"):
                return GeocodingResult(original_address=address, score=0)
            
            feature = data["features"][0]
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [None, None])
            
            self.stats["items_fetched"] += 1
            
            return GeocodingResult(
                original_address=address,
                label=props.get("label"),
                latitude=coords[1] if len(coords) > 1 else None,
                longitude=coords[0] if len(coords) > 0 else None,
                score=props.get("score", 0),
                postal_code=props.get("postcode"),
                city_code=props.get("citycode"),
                city=props.get("city"),
            )
        
        except Exception as e:
            self.stats["requests_failed"] += 1
            return GeocodingResult(original_address=address, score=0)
    
    def fetch_batch(self, addresses: list[str]) -> list[GeocodingResult]:
        """Géocode un lot d'adresses."""
        results = []
        for address in addresses:
            result = self.geocode_single(address)
            results.append(result)
            self._rate_limit()
        return results
    
    def fetch_all(
        self, 
        addresses: list[str], 
        verbose: bool = True
    ) -> Generator[GeocodingResult, None, None]:
        """Géocode toutes les adresses."""
        from datetime import datetime
        
        self.stats["start_time"] = datetime.now()
        
        iterator = tqdm(addresses, desc="Géocodage", disable=not verbose)
        
        for address in iterator:
            result = self.geocode_single(address)
            yield result
            self._rate_limit()
        
        self.stats["end_time"] = datetime.now()
        
        if verbose:
            success = sum(1 for _ in range(self.stats["items_fetched"]))
            print(f"✅ {self.stats['items_fetched']} adresses géocodées")