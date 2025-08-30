#!/usr/bin/env python3
# ============================================================
# Objectif :
#   Fusionner de façon incrémentale un dump JSONL "du jour" (src_jsonl)
#   dans un fichier final cumulatif (final_jsonl) SANS doublons d'ID,
#   avec normalisations minimales par type d’entité.
#
# Garanties :
#   - Idempotence par ID : un ID ne sera jamais écrit en double.
#   - Écriture atomique : pas de fichier final partiellement écrit.
#   - Tolérance aux lignes invalides : ignorées proprement.
#
# Entrées :
#   mode       ∈ {movies, tv, people, networks, keywords, companies}
#   src_jsonl  = dump décompressé du jour (JSON Lines)
#   final_jsonl= fichier cumulatif dédupliqué
#
# Sortie standard :
#   Chaîne de statut : "<mode>: added=X total=Y -> <final_jsonl>"
#   → Consommée par le script Bash pour logger added/total.
# ============================================================

import os
import sys
import json
import tempfile

USAGE = "usage: merge_tmdb_into_final.py <movies|tv|people|networks|keywords|companies> <src_jsonl> <final_jsonl>"

# ---------- I/O atomique ----------
def atomic_replace_write(lines, final_path):
    """
    Écrit la liste de lignes `lines` dans `final_path` de façon atomique :
      1) Création d’un fichier temporaire dans le même dossier.
      2) Écriture intégrale et flush.
      3) os.replace -> swap atomique (sur le même filesystem).
    Avantages :
      - Jamais de final corrompu si crash pendant l’écriture.
      - Compatibilité multi-plateforme raisonnable.
    """
    d = os.path.dirname(final_path) or "."
    os.makedirs(d, exist_ok=True)  # Assure l’existence du dossier final.

    # mkstemp : crée un fichier temp unique, on ferme immédiatement le fd pour réouvrir proprement.
    fd, tmp = tempfile.mkstemp(prefix=".tmp_merge_", dir=d)
    os.close(fd)

    # Écriture stricte en UTF-8, une ligne par JSON, terminée par '\n'
    with open(tmp, "w", encoding="utf-8") as f:
        for line in lines:
            # Sécurise le séparateur de ligne : un et un seul '\n'
            f.write(line.rstrip("\n") + "\n")

    # Remplacement atomique → le final apparaît d'un coup une fois prêt.
    os.replace(tmp, final_path)

# ---------- Lecture JSONL tolérante ----------
def load_jsonl(path):
    """
    Charge un JSONL en mémoire sous forme de liste de chaînes brutes.
    - Retourne [] si le fichier n’existe pas (cas initial).
    - Ignore les lignes vides.
    Note : on garde les lignes brutes pour re-encoder après normalisation.
    """
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.rstrip("\n") for ln in f if ln.strip()]

# ---------- Normalisations minimales ----------
def normalize_payload(mode, obj):
    """
    Harmonisations légères pour faciliter la consommation downstream.
    Logique :
      - movies   : assurer 'title' si 'original_title' existe.
      - tv       : assurer 'name'  si 'original_name' existe.
      - people   : assurer 'name' (à défaut: original_name/original_title/""), pas d’autre enrichissement.
      - networks : idem 'name'.
      - keywords : idem 'name'.
      - companies: idem 'name'.
    On reste minimaliste pour ne pas altérer la donnée TMDB au-delà du strict nécessaire.
    """
    if mode == "movies":
        if "title" not in obj and "original_title" in obj:
            obj["title"] = obj.get("original_title", "")
    elif mode == "tv":
        if "name" not in obj and "original_name" in obj:
            obj["name"] = obj.get("original_name", "")
    elif mode in ("people", "networks", "keywords", "companies"):
        # On tente plusieurs alias raisonnables avant de tomber sur vide.
        obj.setdefault("name", obj.get("original_name", obj.get("original_title", "")))
    return obj

# ---------- Parsing tolérant d’une ligne JSONL ----------
def parse_id_and_payload(mode, line):
    """
    Transforme une ligne JSON brute en couple (iid:int, payload:str JSON compact).
    Règles :
      - Ignore silencieusement si JSON invalide ou champ 'id' absent/malsain.
      - 'id' doit être convertible en int → c’est la clé de déduplication.
      - Applique normalize_payload(mode, obj) puis re-dump JSON.
    """
    try:
        obj = json.loads(line)
    except Exception:
        return None, None  # JSON illisible → on skip.

    if "id" not in obj:
        return None, None

    iid = obj["id"]
    try:
        iid = int(iid)  # Force l’ID en entier (sécurité sur types inattendus).
    except Exception:
        return None, None

    obj = normalize_payload(mode, obj)
    # ensure_ascii=False → conserve accents/UTF-8.
    return iid, json.dumps(obj, ensure_ascii=False)

# ---------- Merge principal ----------
def merge(mode, src_path, final_path):
    """
    Algorithme :
      1) Lire le final existant et construire un set d'IDs connus (déduplication).
      2) Lire le source du jour, ne conserver que les IDs nouveaux.
      3) Réécrire le final (ancien dédupliqué + ajouts) de façon atomique.
    Complexité :
      - Linéaire en nombre de lignes (O(N)).
      - Set en mémoire pour les IDs : rapide pour lookup, dimensionné par volume TMDB.
    """
    # Validation du mode
    if mode not in ("movies", "tv", "people", "networks", "keywords", "companies"):
      # Erreur d’usage → code 2 (convention POSIX pour misuse)
        print(USAGE, file=sys.stderr); sys.exit(2)

    # 1) Déduplication du final existant
    final_lines = load_jsonl(final_path)  # Lignes brutes déjà cumulées
    existing_ids = set()                  # Ensemble des IDs déjà vus dans le final
    kept_final_lines = []                 # Lignes finales conservées (sans doublons internes)

    for ln in final_lines:
        iid, payload = parse_id_and_payload(mode, ln)
        if iid is None:
            # Ligne invalide → on l’ignore pour ne pas propager de bruit
            continue
        if iid in existing_ids:
            # Double déjà présent dans le final → on le saute (nettoyage opportuniste)
            continue
        existing_ids.add(iid)
        kept_final_lines.append(payload)

    # 2) Ajout incrémental depuis le dump du jour
    new_lines = load_jsonl(src_path)  # Source du jour
    added = 0
    appended_lines = []               # Nouvelles lignes valides à ajouter

    for ln in new_lines:
        iid, payload = parse_id_and_payload(mode, ln)
        if iid is None:
            # Ligne invalide/ID manquant → on skip
            continue
        if iid in existing_ids:
            # Déjà connu → on n’ajoute pas
            continue
        existing_ids.add(iid)
        appended_lines.append(payload)
        added += 1

    # 3) Écriture atomique du nouveau final
    out_lines = kept_final_lines + appended_lines
    atomic_replace_write(out_lines, final_path)

    # 4) Bilan chiffré pour log Bash
    print(f"{mode}: added={added} total={len(out_lines)} -> {final_path}")

# ---------- Entrée CLI ----------
if __name__ == "__main__":
    # Contrôle strict des arguments
    if len(sys.argv) != 4:
        print(USAGE, file=sys.stderr); sys.exit(2)

    mode_arg = sys.argv[1]
    src_arg = sys.argv[2]
    final_arg = sys.argv[3]

    # Exécution
    merge(mode_arg, src_arg, final_arg)
#Fin du script