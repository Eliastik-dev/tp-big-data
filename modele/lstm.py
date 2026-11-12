# Definition du modele LSTM + preparation des entrees
# (dans un fichier a part car on s'en sert aussi pour les predictions en temps reel)

import numpy as np
import torch
import torch.nn as nn
from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence

NB_TYPES = 21   # eventmsgtype va de 0 a 20 environ (1 = tir reussi, 2 = rate, 4 = rebond...)

# colonnes numeriques donnees au modele, avec une echelle pour les ramener vers [-1, 1]
COLONNES = {
    "periode": 4,
    "temps_restant": 2880,
    "temps_ecoule": 2880,
    "ecart": 20,
    "equipe_action": 1,
    "possession": 1,
    "pts_dom": 3,
    "pts_ext": 3,
    "run_3min": 10,
    "en_run": 1,
    "serie_points": 10,
    "serie_tirs_dom": 5,
    "serie_tirs_ext": 5,
}


def preparer_entrees(d):
    # d = DataFrame pandas (un match) ou dict d'une seule action
    num = np.stack([np.asarray(d[c], dtype=np.float32) / echelle for c, echelle in COLONNES.items()], axis=-1)
    typ = np.clip(np.asarray(d["type_action"], dtype=np.int64), 0, NB_TYPES - 1)
    return num, typ


class ModeleLSTM(nn.Module):
    def __init__(self, taille_cachee=64):
        super().__init__()
        self.emb_type = nn.Embedding(NB_TYPES, 8)       # le type d'action est une categorie
        self.lstm = nn.LSTM(len(COLONNES) + 8, taille_cachee, num_layers=2, batch_first=True, dropout=0.1)
        self.sortie = nn.Linear(taille_cachee, 1)

    def forward(self, x, types, longueurs=None, etat=None):
        z = torch.cat([x, self.emb_type(types)], dim=-1)
        if longueurs is not None:
            z = pack_padded_sequence(z, longueurs.cpu(), batch_first=True, enforce_sorted=False)
            h, _ = self.lstm(z)
            h, _ = pad_packed_sequence(h, batch_first=True)
            return self.sortie(h).squeeze(-1)
        # mode temps reel : une action a la fois, on garde l'etat (h, c) entre les appels
        h, etat = self.lstm(z, etat)
        return self.sortie(h).squeeze(-1), etat
