"""Configuration centralisée du pipeline."""
from pathlib import Path
from dataclasses import dataclass

# === Chemins ===
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = DATA_DIR / "reports"

for dir_path in [RAW_DIR, PROCESSED_DIR, REPORTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)


@dataclass
class APIConfig:
    """Configuration d'une API."""
    name: str
    base_url: str
    timeout: int
    rate_limit: float  # secondes entre requêtes
    headers: dict = None
    
    def __post_init__(self):
        self.headers = self.headers or {}


# === Configurations des APIs ===
OPENFOODFACTS_CONFIG = APIConfig(
    name="OpenFoodFacts",
    base_url="https://world.openfoodfacts.org/api/v2",
    timeout=60,
    rate_limit=1.5,
    headers={"User-Agent": "IPSSI-TP-Pipeline/1.0 (contact@ipssi.fr)"}
)

ADRESSE_CONFIG = APIConfig(
    name="API Adresse",
    base_url="https://api-adresse.data.gouv.fr",
    timeout=10,
    rate_limit=0.1,  # Très rapide, peu de limite
)

# === Paramètres d'acquisition ===
MAX_ITEMS = 500  # Limite pour le TP
BATCH_SIZE = 50  # Taille des lots

# === Seuils de qualité ===
QUALITY_THRESHOLDS = {
    "completeness_min": 0.7,      # 70% des champs remplis
    "geocoding_score_min": 0.5,   # Score géocodage minimum
    "duplicates_max_pct": 5.0,    # Max 5% de doublons
}