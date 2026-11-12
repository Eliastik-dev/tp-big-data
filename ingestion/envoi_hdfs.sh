#!/bin/bash
# Envoie les CSV du dataset NBA (kaggle) dans HDFS, zone "raw"
# a lancer depuis la racine du projet une fois le cluster demarre :
#   bash ingestion/envoi_hdfs.sh

# fichiers dont on a besoin pour le projet
FICHIERS="play_by_play.csv game.csv team.csv"

for f in $FICHIERS; do
    if [ ! -f "data/$f" ]; then
        echo "ERREUR : data/$f introuvable (il faut dezipper le dataset kaggle dans data/)"
        exit 1
    fi
done

echo "Creation des dossiers dans HDFS..."
docker compose exec -T namenode hdfs dfs -mkdir -p /nba/raw /nba/clean /nba/gold

for f in $FICHIERS; do
    nom="${f%.csv}"
    echo "Envoi de $f ..."
    docker compose exec -T namenode hdfs dfs -mkdir -p /nba/raw/$nom
    # le dossier ./data est monte dans /donnees du conteneur namenode
    docker compose exec -T namenode hdfs dfs -put -f /donnees/$f /nba/raw/$nom/$f
done

echo "Termine. Contenu de /nba/raw :"
docker compose exec -T namenode hdfs dfs -du -h /nba/raw
