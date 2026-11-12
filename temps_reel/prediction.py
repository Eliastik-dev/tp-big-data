# Service de prediction en temps reel
# - lit les actions du match dans le topic kafka "actions"
# - fait avancer le LSTM d'une action (on garde l'etat du LSTM pour chaque match)
# - envoie la probabilite de victoire dans le topic "predictions" et la sauvegarde dans postgres

import json
import os
import time

import psycopg2
import torch
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from lstm import ModeleLSTM, preparer_entrees

KAFKA = os.environ.get("KAFKA", "kafka:29092")
PG = dict(host=os.environ.get("PG_HOST", "postgres"), dbname="nba", user="nba", password="nba")

# chargement du modele entraine
modele = ModeleLSTM()
modele.load_state_dict(torch.load("lstm_nba.pt", map_location="cpu"))
modele.eval()
print("modele charge")

# kafka met quelques secondes a demarrer, on reessaie
while True:
    try:
        consumer = KafkaConsumer("actions", bootstrap_servers=KAFKA, group_id="prediction",
                                 value_deserializer=lambda v: json.loads(v))
        producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode())
        break
    except NoBrokersAvailable:
        print("kafka pas encore pret...")
        time.sleep(3)

conn = psycopg2.connect(**PG)
conn.autocommit = True
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
        game_id BIGINT, rang INT, proba_dom REAL, ecart INT, temps_restant INT,
        date_prediction TIMESTAMP DEFAULT now()
    )
""")

etats = {}   # game_id -> etat (h, c) du LSTM
print("en attente d'actions...")

for msg in consumer:
    a = msg.value
    game_id = a["game_id"]

    # nouveau match (ou match relance depuis le debut) : on repart d'un etat vide
    if a["rang"] == 1:
        etats[game_id] = None
        cur.execute("DELETE FROM predictions WHERE game_id = %s", (game_id,))

    num, typ = preparer_entrees({k: [v] for k, v in a.items()})
    x = torch.tensor(num).unsqueeze(0)      # (1 match, 1 action, nb features)
    t = torch.tensor(typ).unsqueeze(0)
    with torch.no_grad():
        logit, etats[game_id] = modele(x, t, etat=etats.get(game_id))
    proba = torch.sigmoid(logit).item()

    resultat = {
        "game_id": game_id,
        "rang": a["rang"],
        "proba_dom": round(proba, 4),
        "score_dom": a["score_dom"],
        "score_ext": a["score_ext"],
        "ecart": a["ecart"],
        "periode": a["periode"],
        "temps_restant": a["temps_restant"],
        "en_run": a["en_run"],
        "type_action": a["type_action"],
        "fin": a.get("fin", False),
    }
    producer.send("predictions", resultat)
    cur.execute("INSERT INTO predictions (game_id, rang, proba_dom, ecart, temps_restant) VALUES (%s,%s,%s,%s,%s)",
                (game_id, a["rang"], proba, a["ecart"], a["temps_restant"]))

    if a.get("fin"):
        etats.pop(game_id, None)
        print(f"match {game_id} termine, derniere proba domicile = {proba:.2f}")
