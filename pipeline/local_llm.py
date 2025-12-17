"""Module pour recommandations IA locales avec GPT4All."""
import os
from pathlib import Path
from gpt4all import GPT4All

# Récupération du chemin du modèle depuis le .env ou dossier local par défaut
MODEL_PATH = os.getenv("GPT4ALL_MODEL_PATH", Path(__file__).parent / "models" / "ggml-gpt4all-j-v1.3-groovy.bin")

# Téléchargement automatique si le fichier modèle est manquant
if not Path(MODEL_PATH).exists():
    import requests
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    url = "https://gpt4all.io/models/ggml-gpt4all-j-v1.3-groovy.bin"  # lien officiel
    print(f"📥 Téléchargement du modèle GPT4All depuis {url} ...")
    r = requests.get(url, stream=True)
    with open(MODEL_PATH, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print("✅ Modèle GPT4All téléchargé.")

# Chargement du modèle GPT4All
llm = GPT4All(str(MODEL_PATH), verbose=True)

def generate_recommendations(prompt: str, n: int = 5) -> str:
    """
    Génère des recommandations locales à partir du prompt.
    """
    response = llm.generate(prompt, max_tokens=512)
    return response
