import pandas as pd
from tqdm import tqdm

from .fetchers.adresse import AdresseFetcher
from .fetchers.secondary_api import SecondaryFetcher
from .models import GeocodingResult, SecondaryResult


class DataEnricher:
    """Enrichit les données en combinant plusieurs sources/API."""

    def __init__(self):
        self.geocoder = AdresseFetcher()
        self.secondary_api = SecondaryFetcher()
        self.enrichment_stats = {
            "total_processed": 0,
            "successfully_enriched": 0,
            "failed_enrichment": 0,
        }

    def extract_addresses(
        self,
        products: list[dict],
        address_field: str = "stores"
    ) -> list[str]:
        """Extrait les adresses uniques des produits."""
        addresses = set()

        for product in products:
            addr = product.get(address_field, "")
            if isinstance(addr, str) and addr.strip():
                for part in addr.split(","):
                    cleaned = part.strip()
                    if len(cleaned) > 3:
                        addresses.add(cleaned)

        return list(addresses)

    def build_geocoding_cache(
        self,
        addresses: list[str]
    ) -> dict[str, GeocodingResult]:
        """Construit un cache de géocodage."""
        cache = {}
        print(f"🌍 Géocodage de {len(addresses)} adresses uniques...")

        for result in self.geocoder.fetch_all(addresses):
            cache[result.original_address] = result

        return cache

    def build_secondary_cache(
        self,
        addresses: list[str]
    ) -> dict[str, SecondaryResult]:
        """Construit un cache pour l'API secondaire."""
        cache = {}
        print(f"🔍 Enrichissement secondaire pour {len(addresses)} adresses...")

        for result in self.secondary_api.fetch_all(addresses):
            cache[result.original_address] = result

        return cache

    def enrich_products(
        self,
        products: list[dict],
        geocoding_cache: dict[str, GeocodingResult],
        secondary_cache: dict[str, SecondaryResult],
        address_field: str = "stores"
    ) -> list[dict]:
        """Enrichit les produits avec les données des deux APIs."""
        enriched_products = []

        for product in tqdm(products, desc="Enrichissement"):
            self.enrichment_stats["total_processed"] += 1
            enriched = product.copy()

            addr = product.get(address_field, "")
            if isinstance(addr, str) and addr.strip():
                first_addr = addr.split(",")[0].strip()

                # 🌍 Géocodage principal
                geo = geocoding_cache.get(first_addr)
                if geo:
                    enriched["store_address"] = geo.label
                    enriched["latitude"] = geo.latitude
                    enriched["longitude"] = geo.longitude
                    enriched["city"] = geo.city
                    enriched["postal_code"] = geo.postal_code
                    enriched["geocoding_score"] = geo.score

                    if geo.is_valid:
                        self.enrichment_stats["successfully_enriched"] += 1
                    else:
                        self.enrichment_stats["failed_enrichment"] += 1

                # 🔍 Enrichissement secondaire (optionnel)
                secondary = secondary_cache.get(first_addr)
                if secondary:
                    enriched.update(secondary.to_dict())

            enriched_products.append(enriched)

        return enriched_products

    def get_stats(self) -> dict:
        """Retourne les statistiques d'enrichissement."""
        stats = self.enrichment_stats.copy()
        stats["geocoder_stats"] = self.geocoder.get_stats()

        if stats["total_processed"] > 0:
            stats["success_rate"] = (
                stats["successfully_enriched"]
                / stats["total_processed"]
                * 100
            )
        else:
            stats["success_rate"] = 0

        return stats
