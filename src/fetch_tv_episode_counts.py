import os, sys, json, gzip, time, argparse, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dotenv import load_dotenv
import requests
load_dotenv()

# --- Auth: Bearer ---
API_BEARER = os.getenv("TMDB_BEARER")
if not API_BEARER:
    print("TMDB_BEARER manquant dans l'environnement", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://api.themoviedb.org/3/tv/{}"
HEADERS = {
    "Authorization": f"Bearer {API_BEARER}",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "User-Agent": "absolute-cinema"
}

# --- Rate limit: ~20 req/s partagée entre threads ---
_rate_lock = threading.Lock()
_rate_window = deque()  # timestamps des 1s dernières

def rate_limited(max_per_sec=20):
    with _rate_lock:
        now = time.time()
        # purge >1s
        while _rate_window and now - _rate_window[0] > 1.0:
            _rate_window.popleft()
        if len(_rate_window) >= max_per_sec:
            sleep_for = 1.0 - (now - _rate_window[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
            # après sleep, purge et enregistre
            now = time.time()
            while _rate_window and now - _rate_window[0] > 1.0:
                _rate_window.popleft()
        _rate_window.append(time.time())

def parse_ids(path):
    ids = set()
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s: 
                continue
            # NDJSON {"id": ...}
            try:
                j = json.loads(s)
                _id = j.get("id")
                if isinstance(_id, int):
                    ids.add(_id)
                    continue
            except json.JSONDecodeError:
                pass
            # fallback: un id par ligne
            if s.isdigit():
                ids.add(int(s))
    return list(ids)

def fetch_one(tv_id, session, max_retries=6):
    url = BASE_URL.format(tv_id)
    backoff = 1.0
    for _ in range(max_retries):
        try:
            rate_limited(20)
            r = session.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                d = r.json()
                return {
                    "id": d.get("id"),
                    "name": d.get("name"),
                    "number_of_episodes": d.get("number_of_episodes")
                }
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            return None
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True, help="NDJSON TMDB (contient des id) ou fichier d'IDs (1 par ligne)")
    ap.add_argument("--out", dest="outfile", required=True, help="NDJSON sortie id/name/number_of_episodes")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    ids = parse_ids(args.infile)
    if args.limit > 0:
        ids = ids[:args.limit]

    session = requests.Session()
    with open(args.outfile, "w", encoding="utf-8") as out, ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(fetch_one, tv_id, session): tv_id for tv_id in ids}
        for fut in as_completed(futures):
            res = fut.result()
            if res and res.get("id") is not None:
                out.write(json.dumps(res, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()
