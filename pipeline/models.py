"""Modèles de données avec validation."""
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime


class Product(BaseModel):
    """Modèle d'un produit alimentaire."""
    code: str
    product_name: Optional[str] = None
    brands: Optional[str] = None
    categories: Optional[str] = None
    nutriscore_grade: Optional[str] = None
    nova_group: Optional[int] = None
    energy_100g: Optional[float] = None
    sugars_100g: Optional[float] = None
    fat_100g: Optional[float] = None
    salt_100g: Optional[float] = None
    
    # Champs d'enrichissement (ajoutés après géocodage)
    store_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None
    geocoding_score: Optional[float] = None
    
    # Métadonnées
    fetched_at: datetime = Field(default_factory=datetime.now)
    quality_score: Optional[float] = None
    
    @validator('nutriscore_grade')
    def validate_nutriscore(cls, v):
        if v and v.lower() not in ['a', 'b', 'c', 'd', 'e']:
            return None
        return v.lower() if v else None
    
    @validator('energy_100g', 'sugars_100g', 'fat_100g', 'salt_100g')
    def validate_positive(cls, v):
        if v is not None and v < 0:
            return None
        return v


class GeocodingResult(BaseModel):
    """Résultat de géocodage."""
    original_address: str
    label: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    score: float = 0.0
    postal_code: Optional[str] = None
    city_code: Optional[str] = None
    city: Optional[str] = None
    
    @property
    def is_valid(self) -> bool:
        return self.score >= 0.5 and self.latitude is not None
    

class SecondaryResult(BaseModel):

    source: str = "secondary_api"
    score: Optional[float] = None
    label: Optional[str] = None
    comment: Optional[str] = None


class QualityMetrics(BaseModel):
    """Métriques de qualité du dataset."""
    total_records: int
    valid_records: int
    completeness_score: float
    duplicates_count: int
    duplicates_pct: float
    geocoding_success_rate: float
    avg_geocoding_score: float
    null_counts: dict
    quality_grade: str  # A, B, C, D, F
    
    @property
    def is_acceptable(self) -> bool:
        return self.quality_grade in ['A', 'B', 'C']
    

   