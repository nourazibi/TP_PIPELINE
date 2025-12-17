"""Module de scoring et rapport de qualité avec recommandations IA locales."""
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from .config import QUALITY_THRESHOLDS, REPORTS_DIR
from .models import QualityMetrics

load_dotenv()


class QualityAnalyzer:
    """Analyse et score la qualité des données."""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.metrics = None

    def calculate_completeness(self) -> float:
        total_cells = self.df.size
        non_null_cells = self.df.notna().sum().sum()
        return non_null_cells / total_cells if total_cells > 0 else 0

    def count_duplicates(self) -> tuple[int, float]:
        id_col = 'code' if 'code' in self.df.columns else self.df.columns[0]
        duplicates = self.df.duplicated(subset=[id_col]).sum()
        pct = duplicates / len(self.df) * 100 if len(self.df) > 0 else 0
        return duplicates, pct

    def calculate_geocoding_stats(self) -> tuple[float, float]:
        if 'geocoding_score' not in self.df.columns:
            return 0, 0
        valid_geo = self.df['geocoding_score'].notna() & (self.df['geocoding_score'] > 0)
        success_rate = valid_geo.sum() / len(self.df) * 100 if len(self.df) > 0 else 0
        avg_score = self.df.loc[valid_geo, 'geocoding_score'].mean() if valid_geo.any() else 0
        return success_rate, avg_score

    def calculate_null_counts(self) -> dict:
        return self.df.isnull().sum().to_dict()

    def determine_grade(self, completeness: float, duplicates_pct: float, geo_rate: float) -> str:
        score = min(completeness * 40, 40)
        if duplicates_pct <= 1:
            score += 30
        elif duplicates_pct <= 5:
            score += 20
        elif duplicates_pct <= 10:
            score += 10
        score += min(geo_rate / 100 * 30, 30) if 'geocoding_score' in self.df.columns else 30
        if score >= 90:
            return 'A'
        elif score >= 75:
            return 'B'
        elif score >= 60:
            return 'C'
        elif score >= 40:
            return 'D'
        else:
            return 'F'

    def analyze(self) -> QualityMetrics:
        completeness = self.calculate_completeness()
        duplicates, duplicates_pct = self.count_duplicates()
        geo_rate, geo_avg = self.calculate_geocoding_stats()
        null_counts = self.calculate_null_counts()
        valid_records = len(self.df) - duplicates
        grade = self.determine_grade(completeness, duplicates_pct, geo_rate)

        self.metrics = QualityMetrics(
            total_records=len(self.df),
            valid_records=valid_records,
            completeness_score=round(completeness, 3),
            duplicates_count=duplicates,
            duplicates_pct=round(duplicates_pct, 2),
            geocoding_success_rate=round(geo_rate, 2),
            avg_geocoding_score=round(geo_avg, 3),
            null_counts=null_counts,
            quality_grade=grade,
        )
        return self.metrics

    def generate_ai_recommendations(self) -> str:
        """Génère des recommandations IA locales avec GPT4All si le modèle est disponible."""
        if not self.metrics:
            self.analyze()

        model_path = os.getenv("GPT4ALL_MODEL_PATH")  # Chemin vers le modèle local
        if not model_path or not Path(model_path).exists():
            return "⚠️ Recommandations IA désactivées (modèle local GPT4All manquant)."

        try:
            from gpt4all import GPT4All

            # Initialisation et ouverture du modèle
            model = GPT4All(model_path)
            model.open()

            context = f"""
Analyse de qualité d'un dataset :
- Total: {self.metrics.total_records}
- Complétude: {self.metrics.completeness_score * 100:.1f}%
- Doublons: {self.metrics.duplicates_pct:.1f}%
- Note: {self.metrics.quality_grade}

Valeurs nulles par colonne:
{self.metrics.null_counts}
"""
            prompt = f"{context}\n\nDonne 5 recommandations concrètes et actionnables."

            # Génération des recommandations
            response = model.generate(prompt)
            model.close()
            return response

        except Exception as e:
            return f"⚠️ Recommandations IA indisponibles : {str(e)}"

    def generate_report(self, output_name: str = "quality_report") -> Path:
        if not self.metrics:
            self.analyze()

        recommendations = self.generate_ai_recommendations()
        report = f"""# Rapport de Qualité des Données

**Généré le** : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Métriques Globales

| Métrique | Valeur | Seuil |
|----------|--------|-------|
| **Note globale** | **{self.metrics.quality_grade}** | A-B-C = Acceptable |
| Total enregistrements | {self.metrics.total_records} | - |
| Enregistrements valides | {self.metrics.valid_records} | - |
| Complétude | {self.metrics.completeness_score * 100:.1f}% | ≥ 70% |
| Doublons | {self.metrics.duplicates_pct:.1f}% | ≤ 5% |
| Géocodage réussi | {self.metrics.geocoding_success_rate:.1f}% | ≥ 50% |
| Score géocodage moyen | {self.metrics.avg_geocoding_score:.2f} | ≥ 0.5 |

## 📋 Valeurs Manquantes par Colonne
| Colonne | Valeurs nulles | % |
"""
        for col, count in sorted(self.metrics.null_counts.items(), key=lambda x: x[1], reverse=True):
            pct = count / self.metrics.total_records * 100 if self.metrics.total_records > 0 else 0
            report += f"| {col} | {count} | {pct:.1f}% |\n"

        report += f"""

## 🤖 Recommandations IA
{recommendations}

## ✅ Conclusion
{"✅ **Dataset acceptable** pour l'analyse." if getattr(self.metrics, 'is_acceptable', True) else "⚠️ **Dataset nécessite des corrections** avant utilisation."}

---
*Rapport généré automatiquement par le pipeline Open Data*
"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = REPORTS_DIR / f"{output_name}_{timestamp}.md"
        filepath.write_text(report, encoding='utf-8')
        print(f"📄 Rapport sauvegardé : {filepath}")
        return filepath
