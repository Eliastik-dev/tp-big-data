// Serveur du dashboard
//  - API REST pour lister les matchs et les statistiques (PostgreSQL)
//  - rejeu d'un match : envoie les actions une par une dans kafka (topic "actions")
//  - lit les predictions du LSTM (topic "predictions") et les envoie au navigateur en websocket

const express = require("express");
const http = require("http");
const path = require("path");
const { Pool } = require("pg");
const { Kafka } = require("kafkajs");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const pool = new Pool({
  host: process.env.PG_HOST || "localhost",
  user: "nba",
  password: "nba",
  database: "nba",
});
const kafka = new Kafka({ clientId: "dashboard", brokers: [process.env.KAFKA || "localhost:9092"] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "dashboard-" + Date.now() });

const app = express();
app.use(express.static(path.join(__dirname, "public")));

// liste des matchs des saisons de test (le modele ne les a jamais vus)
app.get("/api/matchs", async (req, res) => {
  const saison = parseInt(req.query.saison) || 2022;
  const r = await pool.query(
    `SELECT m.game_id, m.date_match, m.equipe_dom, m.equipe_ext, m.victoire_dom, m.type_saison
     FROM matchs m
     WHERE m.saison = $1 AND EXISTS (SELECT 1 FROM actions a WHERE a.game_id = m.game_id)
     ORDER BY m.date_match DESC LIMIT 300`,
    [saison]
  );
  res.json(r.rows);
});

// statistiques calculees par spark (job 3)
app.get("/api/stats/:nom", async (req, res) => {
  const tables = ["stats_saisons", "stats_remontees", "stats_avant_run", "stats_modele"];
  if (!tables.includes(req.params.nom)) return res.status(404).json({ erreur: "stat inconnue" });
  try {
    const r = await pool.query(`SELECT * FROM ${req.params.nom}`);
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ erreur: "table pas encore calculee (lancer le job 3)" });
  }
});

// rejeu d'un match : on envoie les actions dans kafka avec une pause entre chaque
let rejeuEnCours = null;
app.post("/api/rejeu/:gameId", async (req, res) => {
  const gameId = parseInt(req.params.gameId);
  const pause = Math.max(10, parseInt(req.query.pause) || 100); // ms entre deux actions
  if (rejeuEnCours) rejeuEnCours.stop = true; // on arrete le match precedent

  const r = await pool.query("SELECT * FROM actions WHERE game_id = $1 ORDER BY rang", [gameId]);
  if (r.rows.length === 0) return res.status(404).json({ erreur: "match introuvable" });
  res.json({ game_id: gameId, nb_actions: r.rows.length });

  const rejeu = { stop: false };
  rejeuEnCours = rejeu;
  try {
    for (let i = 0; i < r.rows.length && !rejeu.stop; i++) {
    const action = { ...r.rows[i], fin: i === r.rows.length - 1 };
    action.game_id = Number(action.game_id); // bigint -> nombre
    await producer.send({
      topic: "actions",
      messages: [{ key: String(gameId), value: JSON.stringify(action) }],
    });
    await new Promise((ok) => setTimeout(ok, pause));
    }
  } catch (e) {
    console.log("erreur pendant le rejeu :", e.message);
  }
});

app.post("/api/stop", (req, res) => {
  if (rejeuEnCours) rejeuEnCours.stop = true;
  res.json({ ok: true });
});

// websocket : on renvoie toutes les predictions recues a tous les navigateurs connectes
const serveur = http.createServer(app);
const wss = new WebSocketServer({ server: serveur });

async function demarrer() {
  // kafka peut mettre un peu de temps a etre pret
  for (let essai = 1; ; essai++) {
    try {
      await producer.connect();
      await consumer.connect();
      break;
    } catch (e) {
      console.log("kafka pas pret, essai", essai);
      await new Promise((ok) => setTimeout(ok, 3000));
    }
  }
  await consumer.subscribe({ topic: "predictions", fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const texte = message.value.toString();
      wss.clients.forEach((c) => c.readyState === 1 && c.send(texte));
    },
  });
  serveur.listen(PORT, () => console.log("dashboard sur http://localhost:" + PORT));
}

demarrer();
