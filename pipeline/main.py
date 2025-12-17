#!/usr/bin/env python3
"""Script principal du pipeline."""
import argparse
from datetime import datetime
import pandas as pd

from .fetchers.openfoodfacts import OpenFoodFactsFetcher
from .enricher import DataEnricher
from .transformer import DataTransformer
from .quality import QualityAnalyzer
from .storage import save_raw_json, save_parquet
from .config import MAX_ITEMS


def run_pipeline(
    category: str,
    max_items: int = MAX_ITEMS,
    skip_enrichment: bool = False,
    verbose: bool = True
) -> dict:
    """
    Exécute le pipeline complet.
    """
    stats = {"start_time": datetime.now()}

    print("=" * 60)
    print(f"🚀 PIPELINE OPEN DATA - {category.upper()}")
    print("=" * 60)

    # === ÉTAPE 1 : Acquisition ===
    print("\n📥 ÉTAPE 1 : Acquisition des données")
    fetcher = OpenFoodFactsFetcher()
    products = list(fetcher.fetch_all(category, max_items, verbose))

    if not products:
        print("❌ Aucun produit récupéré. Arrêt.")
        return {"error": "No data fetched"}

    save_raw_json(products, f"{category}_raw")
    stats["fetcher"] = fetcher.get_stats()

    # === ÉTAPE 2 : Enrichissement ===
    if not skip_enrichment:
        print("\n🌍 ÉTAPE 2 : Enrichissement (géocodage)")
        enricher = DataEnricher()

        addresses = enricher.extract_addresses(products, "stores")

        if addresses:
            geo_cache = enricher.build_geocoding_cache(addresses[:100])

            # ✅ Cache secondaire (vide mais prêt, comme ton camarade)
            secondary_cache = {}

            products = enricher.enrich_products(
                products,
                geo_cache,
                secondary_cache
            )

            stats["enricher"] = enricher.get_stats()
        else:
            print("⚠️ Pas d'adresses à géocoder")
    else:
        print("\n⏭️ ÉTAPE 2 : Enrichissement (ignoré)")

    # === ÉTAPE 3 : Transformation ===
    print("\n🔧 ÉTAPE 3 : Transformation et nettoyage")
    df = pd.DataFrame(products)

    transformer = DataTransformer(df)
    df_clean = (
        transformer
        .remove_duplicates()
        .handle_missing_values(
            numeric_strategy='median',
            text_strategy='unknown'
        )
        .normalize_text_columns(['brands', 'categories'])
        .add_derived_columns()
        .get_result()
    )

    print(f"   Résumé des transformations:\n{transformer.get_summary()}")
    stats["transformer"] = {
        "transformations": transformer.transformations_applied
    }

    # === ÉTAPE 4 : Qualité ===
    print("\n📊 ÉTAPE 4 : Analyse de qualité")
    analyzer = QualityAnalyzer(df_clean)
    metrics = analyzer.analyze()

    print(f"   Note: {metrics.quality_grade}")
    print(f"   Complétude: {metrics.completeness_score * 100:.1f}%")
    print(f"   Doublons: {metrics.duplicates_pct:.1f}%")

    analyzer.generate_report(f"{category}_quality")
    stats["quality"] = metrics.dict()

    # === ÉTAPE 5 : Stockage ===
    print("\n💾 ÉTAPE 5 : Stockage final")
    output_path = save_parquet(df_clean, category)
    stats["output_path"] = str(output_path)

    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (
        stats["end_time"] - stats["start_time"]
    ).seconds

    print("\n" + "=" * 60)
    print("✅ PIPELINE TERMINÉ")
    print("=" * 60)
    print(f"   Durée: {stats['duration_seconds']}s")
    print(f"   Produits: {len(df_clean)}")
    print(f"   Qualité: {metrics.quality_grade}")
    print(f"   Fichier: {output_path}")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Pipeline Open Data")
    parser.add_argument(
        "--category", "-c",
        default="chocolats",
        help="Catégorie"
    )
    parser.add_argument(
        "--max-items", "-m",
        type=int,
        default=MAX_ITEMS,
        help="Nombre max"
    )
    parser.add_argument(
        "--skip-enrichment", "-s",
        action="store_true",
        help="Ignorer l'enrichissement"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=True
    )

    args = parser.parse_args()

    run_pipeline(
        category=args.category,
        max_items=args.max_items,
        skip_enrichment=args.skip_enrichment,
        verbose=args.verbose
    )


if __name__ == "__main__":
    main()
