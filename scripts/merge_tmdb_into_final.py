#!/usr/bin/env python3
import os, sys, json, tempfile

# Message d'usage en cas de mauvaise invocation
USAGE = "usage: merge_tmdb_into_final.py <movies|tv> <src_jsonl> <final_jsonl>"

def atomic_replace_write(lines, final_path):
    """
    Écrit une liste de lignes dans un fichier final de manière atomique.
    1. Crée un fichier temporaire dans le même dossier que le fichier final.
    2. Écrit toutes les lignes dedans.
    3. Remplace le fichier final par le temporaire.
    """
    d = os.path.dirname(final_path) or "."
    os.makedirs(d, exist_ok=True)  # S'assure que le dossier existe
    fd, tmp = tempfile.mkstemp(prefix=".tmp_merge_", dir=d)  # Fichier temporaire
    os.close(fd)
    with open(tmp, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n") + "\n")  # Supprime éventuel \n en trop et réécrit un seul \n
    os.replace(tmp, final_path)  # Remplacement atomique du fichier

def load_jsonl(path):
    """
    Charge un fichier JSONL (1 objet JSON par ligne) en mémoire sous forme de liste de chaînes brutes.
    Si le fichier n'existe pas, retourne une liste vide.
    """
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.rstrip("\n") for ln in f if ln.strip()]  # Ignore lignes vides

def parse_id_and_payload(mode, line):
    """
    Analyse une ligne JSONL pour extraire :
    - l'ID (iid)
    - le JSON ré-encodé comme chaîne (payload)
    Retourne (None, None) si ligne invalide ou sans ID.
    Applique aussi une normalisation de certains champs :
      - films : s'assure qu'un champ 'title' existe (sinon copie 'original_title')
      - séries : s'assure qu'un champ 'name' existe (sinon copie 'original_name')
    """
    try:
        obj = json.loads(line)
    except Exception:
        return None, None
    if "id" not in obj:
        return None, None
    iid = obj["id"]
    if not isinstance(iid, int):
        try:
            iid = int(iid)  # Conversion forcée en int
        except Exception:
            return None, None
    # Normalisation légère des champs utiles
    if mode == "movies":
        # Si pas de "title" mais "original_title" existe, on copie
        if "title" not in obj and "original_title" in obj:
            obj["title"] = obj.get("original_title", "")
    else:  # mode == "tv"
        # Si pas de "name" mais "original_name" existe, on copie
        if "name" not in obj and "original_name" in obj:
            obj["name"] = obj.get("original_name", "")
        # On ne touche pas à d'autres champs (comme episodes)
    return iid, json.dumps(obj, ensure_ascii=False)  # Retourne ID + JSON formaté

def merge(mode, src_path, final_path):
    """
    Fusionne le fichier source (src_path) dans le fichier final (final_path).
    Étapes :
    1. Charge le fichier final existant et en retire les doublons d'ID.
    2. Charge le fichier source du jour, et ajoute uniquement les IDs absents.
    3. Réécrit le fichier final en y ajoutant les nouvelles lignes.
    """
    if mode not in ("movies", "tv"):
        print(USAGE, file=sys.stderr); sys.exit(2)

    # --- Étape 1 : lire final existant (JSONL) ---
    final_lines = load_jsonl(final_path)
    existing_ids = set()  # IDs déjà présents
    kept_final_lines = []  # Lignes finales conservées après déduplication
    for ln in final_lines:
        iid, payload = parse_id_and_payload(mode, ln)
        if iid is None:
            continue
        if iid in existing_ids:
            continue  # On ignore silencieusement les doublons déjà dans le fichier final
        existing_ids.add(iid)
        kept_final_lines.append(payload)

    # --- Étape 2 : lire source du jour (JSONL) ---
    new_lines = load_jsonl(src_path)
    added = 0
    appended_lines = []  # Nouvelles lignes à ajouter
    for ln in new_lines:
        iid, payload = parse_id_and_payload(mode, ln)
        if iid is None:
            continue
        if iid in existing_ids:
            continue  # On ignore si l'ID est déjà dans le final
        existing_ids.add(iid)
        appended_lines.append(payload)
        added += 1

    # --- Étape 3 : réécriture atomique ---
    out_lines = kept_final_lines + appended_lines
    atomic_replace_write(out_lines, final_path)
    print(f"{mode}: added={added} total={len(out_lines)} -> {final_path}")

if __name__ == "__main__":
    # Vérifie le nombre d'arguments et exécute la fusion
    if len(sys.argv) != 4:
        print(USAGE, file=sys.stderr); sys.exit(2)
    merge(sys.argv[1], sys.argv[2], sys.argv[3])
