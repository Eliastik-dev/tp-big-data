# Telechargement du dataset NBA depuis kaggle puis copie des CSV utiles dans data/
# lancement (python pas installe sur mon pc donc je passe par docker) :
#   docker run --rm -v "$PWD":/projet -w /projet python:3.11-slim bash -c "pip install kagglehub && python ingestion/telechargement.py"

import os
import shutil
import kagglehub

# on met le cache kaggle dans data/ pour ne pas le perdre quand le conteneur s'arrete
os.environ.setdefault("KAGGLEHUB_CACHE", "data/kaggle_cache")

# Download latest version
path = kagglehub.dataset_download("wyattowalsh/basketball")
print("Path to dataset files:", path)

# on ne garde que les fichiers dont on a besoin
FICHIERS = ["play_by_play.csv", "game.csv", "team.csv"]

for f in FICHIERS:
    trouve = False
    for dossier, _, fichiers in os.walk(path):
        if f in fichiers:
            shutil.copy(os.path.join(dossier, f), os.path.join("data", f))
            taille = os.path.getsize(os.path.join("data", f)) / 1e6
            print(f"{f} copie dans data/ ({taille:.0f} Mo)")
            trouve = True
            break
    if not trouve:
        print("ATTENTION :", f, "pas trouve dans le dataset")
