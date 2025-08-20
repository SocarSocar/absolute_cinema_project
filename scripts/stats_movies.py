import json
from collections import Counter

with open("data/out/movie_dumps.json", encoding="utf-8") as f:
    data = [json.loads(line) for line in f if line.strip()]

# Doublons exacts
full_counts = Counter(tuple(sorted(d.items())) for d in data)
full_dupes = [k for k, v in full_counts.items() if v > 1]

# Doublons sur id
id_counts = Counter(d["id"] for d in data)
id_dupes = [k for k, v in id_counts.items() if v > 1]

# Totaux
total_movies = len(data)

# Top 50 par popularité
valid = [d for d in data if isinstance(d.get("popularity"), (int, float))]
top_50 = sorted(valid, key=lambda x: x["popularity"], reverse=True)[:50]

print(f"Doublons exacts: {len(full_dupes)}")
print(f"Doublons d'id: {len(id_dupes)}")
print(f"Total films: {total_movies}")

print("\n=== Top 50 films par popularité ===")
for rank, movie in enumerate(top_50, 1):
    print(f"{rank}. {movie['title']} - Popularité: {movie['popularity']}")
