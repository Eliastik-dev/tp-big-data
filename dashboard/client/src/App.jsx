import { useEffect, useRef, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine,
  ResponsiveContainer, AreaChart, Area, BarChart, Bar, Legend,
} from "recharts";

// libelles des types d'action (eventmsgtype du dataset)
const TYPES = {
  1: "Panier", 2: "Tir rate", 3: "Lancer franc", 4: "Rebond", 5: "Perte de balle",
  6: "Faute", 7: "Violation", 8: "Changement", 9: "Temps mort", 10: "Entre-deux",
  11: "Expulsion", 12: "Debut periode", 13: "Fin periode", 18: "Video",
};

// minute du match a partir de la periode et du temps restant
function minute(p) {
  if (p.periode <= 4) return +(48 - p.temps_restant / 60).toFixed(2);
  return +(48 + (p.periode - 5) * 5 + (300 - p.temps_restant) / 60).toFixed(2);
}

// temps restant dans la periode en cours (temps_restant = temps restant dans le match)
function tempsPeriode(p) {
  return p.periode <= 4 ? p.temps_restant - (4 - p.periode) * 720 : p.temps_restant;
}

function chrono(s) {
  const m = Math.floor(s / 60);
  const sec = String(s % 60).padStart(2, "0");
  return m + ":" + sec;
}

export default function App() {
  const [onglet, setOnglet] = useState("direct");
  return (
    <div className="page">
      <header>
        <h1>NBA - Probabilite de victoire en temps reel</h1>
        <nav>
          <button className={onglet === "direct" ? "actif" : ""} onClick={() => setOnglet("direct")}>Match en direct</button>
          <button className={onglet === "analyses" ? "actif" : ""} onClick={() => setOnglet("analyses")}>Analyses</button>
        </nav>
      </header>
      {onglet === "direct" ? <Direct /> : <Analyses />}
    </div>
  );
}

function Direct() {
  const [saison, setSaison] = useState(2022);
  const [matchs, setMatchs] = useState([]);
  const [gameId, setGameId] = useState("");
  const [pause, setPause] = useState(100);
  const [points, setPoints] = useState([]);
  const [derniers, setDerniers] = useState([]);
  const matchCourant = useRef(null);

  useEffect(() => {
    fetch("/api/matchs?saison=" + saison).then((r) => r.json()).then((m) => {
      setMatchs(m);
      if (m.length) setGameId(m[0].game_id);
    });
  }, [saison]);

  // connexion websocket : on recoit une prediction par action
  useEffect(() => {
    const proto = location.protocol === "https:" ? "wss://" : "ws://";
    const ws = new WebSocket(proto + location.host + "/ws");
    ws.onmessage = (e) => {
      const p = JSON.parse(e.data);
      if (String(p.game_id) !== String(matchCourant.current)) return;
      p.minute = minute(p);
      p.proba = +(p.proba_dom * 100).toFixed(1);
      setPoints((avant) => (p.rang === 1 ? [p] : [...avant, p]));
      setDerniers((avant) => [p, ...avant].slice(0, 8));
    };
    return () => ws.close();
  }, []);

  function lancer() {
    matchCourant.current = gameId;
    setPoints([]);
    setDerniers([]);
    fetch(`/api/rejeu/${gameId}?pause=${pause}`, { method: "POST" });
  }

  function arreter() {
    fetch("/api/stop", { method: "POST" });
  }

  const match = matchs.find((m) => String(m.game_id) === String(gameId));
  const dernier = points[points.length - 1];

  return (
    <div>
      <div className="controles">
        <label>Saison
          <select value={saison} onChange={(e) => setSaison(+e.target.value)}>
            <option value={2022}>2022-23</option>
            <option value={2021}>2021-22</option>
          </select>
        </label>
        <label>Match
          <select value={gameId} onChange={(e) => setGameId(e.target.value)}>
            {matchs.map((m) => (
              <option key={m.game_id} value={m.game_id}>
                {m.date_match.slice(0, 10)} - {m.equipe_ext} @ {m.equipe_dom} {m.type_saison === "Playoffs" ? "(playoffs)" : ""}
              </option>
            ))}
          </select>
        </label>
        <label>Vitesse
          <select value={pause} onChange={(e) => setPause(+e.target.value)}>
            <option value={300}>lente</option>
            <option value={100}>normale</option>
            <option value={30}>rapide</option>
          </select>
        </label>
        <button onClick={lancer}>Lancer le match</button>
        <button className="secondaire" onClick={arreter}>Stop</button>
      </div>

      {match && (
        <div className="tableau-score">
          <div className="equipe">
            <span>{match.equipe_ext}</span>
            <strong>{dernier ? dernier.score_ext : 0}</strong>
            <em>{dernier ? (100 - dernier.proba).toFixed(1) : 50} %</em>
          </div>
          <div className="centre">
            {dernier ? (dernier.periode <= 4 ? "Q" + dernier.periode : "Prol. " + (dernier.periode - 4)) : "-"}
            <br />
            {dernier ? chrono(tempsPeriode(dernier)) : ""}
            {dernier && dernier.en_run !== 0 && (
              <div className="run">RUN {dernier.en_run === 1 ? match.equipe_dom : match.equipe_ext}</div>
            )}
          </div>
          <div className="equipe">
            <span>{match.equipe_dom}</span>
            <strong>{dernier ? dernier.score_dom : 0}</strong>
            <em>{dernier ? dernier.proba.toFixed(1) : 50} %</em>
          </div>
        </div>
      )}

      <div className="carte">
        <h2>Probabilite de victoire de {match ? match.equipe_dom : "l'equipe a domicile"} (%)</h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={points}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="minute" type="number" domain={[0, 48]} allowDataOverflow={false}
                   ticks={[0, 12, 24, 36, 48]} label={{ value: "minute", position: "insideBottomRight", offset: -5 }} />
            <YAxis domain={[0, 100]} />
            <Tooltip formatter={(v) => v + " %"} labelFormatter={(l) => "minute " + l} />
            <ReferenceLine y={50} stroke="#999" strokeDasharray="4 4" />
            {[12, 24, 36].map((x) => <ReferenceLine key={x} x={x} stroke="#ccc" />)}
            <Line type="monotone" dataKey="proba" stroke="#1d428a" dot={false} strokeWidth={2} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="deux-colonnes">
        <div className="carte">
          <h2>Ecart au score (domicile - exterieur)</h2>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={points}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="minute" type="number" domain={[0, 48]} ticks={[0, 12, 24, 36, 48]} />
              <YAxis />
              <Tooltip labelFormatter={(l) => "minute " + l} />
              <ReferenceLine y={0} stroke="#666" />
              <Area type="stepAfter" dataKey="ecart" stroke="#c8102e" fill="#c8102e33" isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="carte">
          <h2>Dernieres actions</h2>
          <table>
            <thead><tr><th>Action</th><th>Score</th><th>Proba dom.</th></tr></thead>
            <tbody>
              {derniers.map((p) => (
                <tr key={p.rang}>
                  <td>{TYPES[p.type_action] || "Autre"}</td>
                  <td>{p.score_ext} - {p.score_dom}</td>
                  <td>{p.proba} %</td>
                </tr>
              ))}
            </tbody>
          </table>
          {dernier && dernier.fin && match && (
            <p className="resultat">
              Fin du match : {match.victoire_dom ? match.equipe_dom : match.equipe_ext} gagne
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

function Analyses() {
  const [saisons, setSaisons] = useState([]);
  const [remontees, setRemontees] = useState([]);
  const [avantRun, setAvantRun] = useState([]);
  const [erreur, setErreur] = useState("");

  useEffect(() => {
    const charger = (nom, set) =>
      fetch("/api/stats/" + nom).then((r) => r.json()).then((d) => (d.erreur ? setErreur(d.erreur) : set(d)));
    charger("stats_saisons", setSaisons);
    charger("stats_remontees", setRemontees);
    charger("stats_avant_run", (d) => setAvantRun(d.map((x) => ({ ...x, action: TYPES[x.type_action] || "Autre" }))));
  }, []);

  if (erreur) return <p className="erreur">{erreur}</p>;

  return (
    <div>
      <div className="carte">
        <h2>Evolution du jeu par saison</h2>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={saisons}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="saison" />
            <YAxis yAxisId="g" />
            <YAxis yAxisId="d" orientation="right" />
            <Tooltip />
            <Legend />
            <Line yAxisId="g" dataKey="pts_par_match" name="points par match" stroke="#1d428a" dot={false} />
            <Line yAxisId="d" dataKey="pct_tirs_3pts" name="% des tirs a 3 points" stroke="#c8102e" dot={false} />
            <Line yAxisId="d" dataKey="runs_par_match" name="runs par match" stroke="#2a9d8f" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="deux-colonnes">
        <div className="carte">
          <h2>% de victoire a domicile selon l'ecart a la mi-temps</h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={remontees}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ecart_mi_temps" />
              <YAxis domain={[0, 100]} />
              <Tooltip formatter={(v, n) => (n === "pct_victoire_dom" ? v + " %" : v)} />
              <ReferenceLine y={50} stroke="#999" strokeDasharray="4 4" />
              <Bar dataKey="pct_victoire_dom" name="% victoire domicile" fill="#1d428a" />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="carte">
          <h2>Actions qui precedent un run (vs. le reste du match)</h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={avantRun} layout="vertical" margin={{ left: 30 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis type="category" dataKey="action" />
              <Tooltip />
              <Legend />
              <Bar dataKey="pct_avant_run" name="% avant un run" fill="#c8102e" />
              <Bar dataKey="pct_normal" name="% en temps normal" fill="#bbb" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
