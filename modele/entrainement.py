# Entrainement du LSTM qui predit la probabilite de victoire de l'equipe a domicile
# a chaque action du match.
#
# Entree : les features calculees par spark (/nba/gold/features), recuperees en local dans data/gold
# Sortie : modele/lstm_nba.pt + modele/resultats.json
#
# lancement (dans docker avec le GPU) :
#   docker run --rm --gpus all -v "$PWD":/projet -w /projet pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime \
#       bash -c "pip install -q pandas pyarrow && python modele/entrainement.py"

import json
import time
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.nn.utils.rnn import pad_sequence

from lstm import ModeleLSTM, preparer_entrees

torch.manual_seed(42)
np.random.seed(42)
device = "cuda" if torch.cuda.is_available() else "cpu"
print("device :", device)

# ---------- chargement ----------
t0 = time.time()
df = pd.read_parquet("data/gold/features")
df["saison"] = df["saison"].astype(int)
df = df.sort_values(["game_id", "rang"])
print(f"{len(df)} actions chargees en {time.time() - t0:.0f}s")

# decoupage par saison (et pas au hasard) pour ne pas tricher :
# on entraine sur le passe et on teste sur les saisons les plus recentes
SAISONS_VAL = [2019, 2020]
SAISONS_TEST = [2021, 2022]


def construire_sequences(d):
    # une sequence = un match
    sequences, types, labels = [], [], []
    for _, m in d.groupby("game_id", sort=False):
        num, typ = preparer_entrees(m)
        sequences.append(torch.tensor(num))
        types.append(torch.tensor(typ))
        labels.append(float(m["victoire_dom"].iloc[0]))
    return sequences, types, labels


train = construire_sequences(df[df["saison"] < min(SAISONS_VAL)])
val = construire_sequences(df[df["saison"].isin(SAISONS_VAL)])
test = construire_sequences(df[df["saison"].isin(SAISONS_TEST)])
print("matchs train / val / test :", len(train[2]), len(val[2]), len(test[2]))


def lots(donnees, taille, melanger):
    seqs, typs, labs = donnees
    idx = np.random.permutation(len(seqs)) if melanger else np.arange(len(seqs))
    for i in range(0, len(idx), taille):
        b = idx[i:i + taille]
        longueurs = torch.tensor([len(seqs[j]) for j in b])
        x = pad_sequence([seqs[j] for j in b], batch_first=True)
        t = pad_sequence([typs[j] for j in b], batch_first=True)
        y = torch.tensor([labs[j] for j in b])
        yield x, t, y, longueurs


def predire_lot(modele, x, t, longueurs):
    x, t = x.to(device), t.to(device)
    sortie = modele(x, t, longueurs)          # (batch, temps) logits
    masque = torch.arange(x.shape[1], device=device)[None, :] < longueurs.to(device)[:, None]
    return sortie, masque


modele = ModeleLSTM().to(device)
optim = torch.optim.Adam(modele.parameters(), lr=1e-3)
perte_fn = nn.BCEWithLogitsLoss(reduction="none")


def evaluer(donnees):
    # renvoie la log-loss, l'accuracy sur toutes les actions et l'accuracy par quart-temps
    modele.eval()
    total_perte, total_ok, total_n = 0.0, 0, 0
    par_periode = {}
    with torch.no_grad():
        for x, t, y, L in lots(donnees, 128, False):
            sortie, masque = predire_lot(modele, x, t, L)
            yy = y.to(device)[:, None].expand_as(sortie)
            perte = perte_fn(sortie, yy)[masque]
            ok = ((sortie > 0).float() == yy)[masque]
            total_perte += perte.sum().item()
            total_ok += ok.sum().item()
            total_n += masque.sum().item()
            # accuracy selon la periode (colonne 0 de x = periode / 4)
            periodes = torch.clamp((x[:, :, 0] * 4).round().long(), 1, 5).to(device)
            for p in range(1, 6):
                m = masque & (periodes == p)
                if m.sum() > 0:
                    a, n = par_periode.get(p, (0, 0))
                    par_periode[p] = (a + ((sortie > 0).float() == yy)[m].sum().item(), n + m.sum().item())
    acc_periode = {("Q" + str(p) if p <= 4 else "prolong."): round(a / n, 4) for p, (a, n) in sorted(par_periode.items())}
    return total_perte / total_n, total_ok / total_n, acc_periode


# ---------- entrainement ----------
NB_EPOQUES = 8
meilleure_perte = 1e9
for epoque in range(1, NB_EPOQUES + 1):
    modele.train()
    t0 = time.time()
    for x, t, y, L in lots(train, 64, True):
        sortie, masque = predire_lot(modele, x, t, L)
        yy = y.to(device)[:, None].expand_as(sortie)
        perte = perte_fn(sortie, yy)[masque].mean()
        optim.zero_grad()
        perte.backward()
        nn.utils.clip_grad_norm_(modele.parameters(), 1.0)
        optim.step()
    perte_val, acc_val, _ = evaluer(val)
    print(f"epoque {epoque} : perte val {perte_val:.4f}  accuracy val {acc_val:.3f}  ({time.time() - t0:.0f}s)")
    if perte_val < meilleure_perte:
        meilleure_perte = perte_val
        torch.save(modele.state_dict(), "modele/lstm_nba.pt")

# ---------- test final ----------
modele.load_state_dict(torch.load("modele/lstm_nba.pt"))
perte_test, acc_test, acc_periode = evaluer(test)
print(f"TEST : perte {perte_test:.4f}  accuracy {acc_test:.3f}")
print("accuracy par periode :", acc_periode)

# modele de reference tres simple : "l'equipe qui mene gagne" (domicile si egalite)
d_test = df[df["saison"].isin(SAISONS_TEST)]
ref = ((d_test["ecart"] >= 0).astype(int) == d_test["victoire_dom"]).mean()
print(f"reference 'celui qui mene gagne' : accuracy {ref:.3f}")

with open("modele/resultats.json", "w") as f:
    json.dump({
        "perte_test": round(perte_test, 4),
        "accuracy_test": round(acc_test, 4),
        "accuracy_par_periode": acc_periode,
        "accuracy_reference_qui_mene": round(float(ref), 4),
        "saisons_val": SAISONS_VAL,
        "saisons_test": SAISONS_TEST,
        "nb_matchs": {"train": len(train[2]), "val": len(val[2]), "test": len(test[2])},
    }, f, indent=2)
print("modele sauvegarde dans modele/lstm_nba.pt")
