#!/usr/bin/env python3
# ============================================================
# Objectif :
#   Fusion incrémentale d’un dump JSONL du jour dans un final cumulatif,
#   sans doublons d’ID, avec normalisations minimales par type d’entité.
#
# Emplacement: scripts/dumps_daily/merge_dumps_final.py
# Sortie: "<mode>: added=X total=Y -> <final_jsonl>"
# ============================================================

import os
import sys
import json
import tempfile

USAGE = "usage: merge_dumps_final.py <movies|tv|people|networks|keywords|companies> <src_jsonl> <final_jsonl>"

# ---------- Racine projet (scripts/dumps_daily/ → ../..) ----------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

# ---------- I/O atomique ----------
def atomic_stream_merge(write_callback, final_path: str):
    d = os.path.dirname(final_path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_merge_", dir=d)
    os.close(fd)
    try:
        write_callback(tmp_path)
        os.replace(tmp_path, final_path)
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise

# ---------- Lecture JSONL (stream) ----------
def iter_jsonl_lines(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if s:
                yield s

# ---------- Normalisations minimales ----------
def normalize_payload(mode: str, obj: dict) -> dict:
    if mode == "movies":
        if "title" not in obj and "original_title" in obj:
            obj["title"] = obj.get("original_title", "")
    elif mode == "tv":
        if "name" not in obj and "original_name" in obj:
            obj["name"] = obj.get("original_name", "")
    elif mode in ("people", "networks", "keywords", "companies"):
        obj.setdefault("name", obj.get("original_name", obj.get("original_title", "")))
    return obj

# ---------- Parsing tolérant ----------
def parse_id_and_payload(mode: str, line: str):
    try:
        obj = json.loads(line)
    except Exception:
        return None, None
    iid = obj.get("id", None)
    try:
        iid = int(iid)
    except Exception:
        return None, None
    obj = normalize_payload(mode, obj)
    return iid, json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

# ---------- Merge principal ----------
def merge(mode: str, src_path: str, final_path: str):
    if mode not in ("movies", "tv", "people", "networks", "keywords", "companies"):
        print(USAGE, file=sys.stderr); sys.exit(2)

    existing_ids = set()
    added = 0
    total_after = 0

    def _write(tmp_path: str):
        nonlocal added, total_after
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as out:
            # Replay du final historique en nettoyant les doublons internes
            for ln in iter_jsonl_lines(final_path):
                iid, payload = parse_id_and_payload(mode, ln)
                if iid is None or iid in existing_ids:
                    continue
                existing_ids.add(iid)
                out.write(payload + "\n")
                total_after += 1
            # Ajouts depuis la source du jour
            for ln in iter_jsonl_lines(src_path):
                iid, payload = parse_id_and_payload(mode, ln)
                if iid is None or iid in existing_ids:
                    continue
                existing_ids.add(iid)
                out.write(payload + "\n")
                added += 1
                total_after += 1

    atomic_stream_merge(_write, final_path)
    print(f"{mode}: added={added} total={total_after} -> {final_path}")

# ---------- Entrée CLI ----------
def main():
    if len(sys.argv) != 4:
        print(USAGE, file=sys.stderr); sys.exit(2)
    mode_arg, src_arg, final_arg = sys.argv[1], sys.argv[2], sys.argv[3]

    if not os.path.exists(src_arg):
        total_current = sum(1 for _ in iter_jsonl_lines(final_arg))
        print(f"{mode_arg}: added=0 total={total_current} -> {final_arg}")
        return

    merge(mode_arg, src_arg, final_arg)

if __name__ == "__main__":
    main()
# Fin du script
