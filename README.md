# Projet Big Data - Prediction NBA en temps reel (momentum + probabilite de victoire)

Projet du cours Hadoop / Spark.

Dataset : NBA (Kaggle) https://www.kaggle.com/datasets/wyattowalsh/basketball

Idee : a partir du play-by-play (13M d'actions) on essaye de detecter les "runs"
(series de points d'une equipe) et de predire le gagnant du match action par action
avec un LSTM. Le resultat est affiche dans un dashboard (React / Node) avec la courbe
de probabilite de victoire qui bouge pendant le match.

## Stack

| Role | Outil |
|---|---|
| Stockage brut + nettoye (data lake) | HDFS |
| Traitement / feature engineering / agregations | Spark (PySpark) |
| Base relationnelle (equipes, matchs, actions, stats, predictions) | PostgreSQL |
| Simulation du temps reel | Kafka |
| Modele | LSTM (PyTorch) |
| Dashboard | React + Node.js |

Schema d'architecture : `docs/architecture.png` - diagramme d'activites : `docs/diagramme_activites.png`

## Organisation

```
docker-compose.yml   tous les services
hadoop.env           config hadoop
data/                CSV kaggle (pas versionne, trop gros)
ingestion/           telechargement kaggle + envoi dans HDFS
spark/jobs/          jobs spark : 01 nettoyage, 02 features, 03 analyses
modele/              LSTM (definition + entrainement)
temps_reel/          service de prediction (lit kafka, fait tourner le LSTM)
dashboard/           serveur node (api, rejeu, websocket) + client react
docs/                schemas
```

Dans HDFS :
- `/nba/raw`   : CSV bruts
- `/nba/clean` : donnees nettoyees en parquet
- `/nba/gold`  : features pour le modele + agregations

## Lancer le projet

Sous Git Bash il faut mettre `MSYS_NO_PATHCONV=1` devant les commandes `docker compose exec`
sinon les chemins commencant par / sont modifies.

1. Demarrer le cluster :
   ```
   docker compose up -d
   ```
   Interfaces : HDFS http://localhost:9871 , Spark http://localhost:8080

2. Telecharger le dataset (python pas installe sur mon pc donc via docker) :
   ```
   docker run --rm -v "$PWD":/projet -w /projet python:3.11-slim bash -c "pip install kagglehub && python ingestion/telechargement.py"
   ```

3. Envoyer les donnees dans HDFS :
   ```
   bash ingestion/envoi_hdfs.sh
   ```

4. Jobs Spark :
   ```
   docker compose exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/01_nettoyage.py
   docker compose exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /jobs/02_features.py
   docker compose exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /jobs/03_analyses.py
   ```

5. Recuperer les features en local et entrainer le LSTM (GPU) :
   ```
   docker compose exec namenode hdfs dfs -get /nba/gold/features /donnees/gold/features
   docker run --rm --gpus all -v "$PWD":/projet -w /projet pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime bash -c "pip install pandas pyarrow && python modele/entrainement.py"
   ```

6. Lancer le service de prediction et le dashboard :
   ```
   docker compose --profile app up -d --build
   ```
   Dashboard : http://localhost:3000

## Avancement

- [x] cluster docker
- [x] ingestion dans HDFS
- [x] nettoyage play-by-play
- [x] schema d'architecture + diagramme d'activites
- [ ] features (ecart, temps restant, series, runs) + chargement PostgreSQL
- [ ] agregations (job 3)
- [ ] LSTM (pytorch)
- [ ] rejeu des matchs dans kafka + predictions en direct
- [ ] dashboard react / node
- [ ] rapport + slides
