"""Fetcher pour l'API OpenFoodFacts."""
from typing import Generator
from tqdm import tqdm

from .base import BaseFetcher
from ..config import OPENFOODFACTS_CONFIG, MAX_ITEMS, BATCH_SIZE
from ..models import Product


class OpenFoodFactsFetcher(BaseFetcher):
    """Fetcher pour OpenFoodFacts."""
    
    def __init__(self):
        super().__init__(OPENFOODFACTS_CONFIG)
        self.fields = [
            "code", "product_name", "brands", "categories",
            "nutriscore_grade", "nova_group", "energy_100g",
            "sugars_100g", "fat_100g", "salt_100g", "stores"
        ]
    
    def fetch_batch(self, category: str, page: int = 1, page_size: int = BATCH_SIZE) -> list[dict]:
        """Récupère une page de produits."""
        params = {
            "categories_tags": category,
            "page": page,
            "page_size": page_size,
            "fields": ",".join(self.fields)
        }
        
        try:
            data = self._make_request("/search", params)
            products = data.get("products", [])
            self.stats["items_fetched"] += len(products)
            return products
        except Exception as e:
            self.stats["requests_failed"] += 1
            print(f"⚠️ Erreur page {page}: {e}")
            return []
    
    def fetch_all(
        self, 
        category: str, 
        max_items: int = MAX_ITEMS,
        verbose: bool = True
    ) -> Generator[dict, None, None]:
        """Récupère tous les produits avec pagination."""
        from datetime import datetime
        
        self.stats["start_time"] = datetime.now()
        page = 1
        total_fetched = 0
        
        pbar = tqdm(total=max_items, desc=f"OpenFoodFacts [{category}]", disable=not verbose)
        
        while total_fetched < max_items:
            remaining = max_items - total_fetched
            page_size = min(BATCH_SIZE, remaining)
            
            products = self.fetch_batch(category, page, page_size)
            
            if not products:
                break
            
            for product in products:
                yield product
                total_fetched += 1
                pbar.update(1)
                
                if total_fetched >= max_items:
                    break
            
            page += 1
            self._rate_limit()
        
        pbar.close()
        self.stats["end_time"] = datetime.now()
        
        if verbose:
            duration = (self.stats["end_time"] - self.stats["start_time"]).seconds
            print(f"✅ {total_fetched} produits récupérés en {duration}s")