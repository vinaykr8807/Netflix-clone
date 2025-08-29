# Reads CSVs from provided URLs, writes to scripts/output, and upserts into movies, ratings, links.

import os, io, csv, time, json, math
from typing import List, Dict
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# Input CSV URLs (provided by you)
RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"
MOVIES_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"

OUTPUT_DIR = "scripts/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def fetch_csv(url: str) -> List[Dict[str, str]]:
    print(f"[v0] Fetching CSV: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    text = r.text
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(row) for row in reader]
    print(f"[v0] Loaded {len(rows)} rows from {url}")
    return rows

def write_local_csv(path: str, rows: List[Dict[str, str]], fieldnames: List[str]):
    print(f"[v0] Writing {len(rows)} rows -> {path}")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)

def supabase_upsert(table: str, rows: List[Dict], conflict: str = None, chunk_size: int = 500):
    base = _must_env("SUPABASE_URL").rstrip("/")
    key = _must_env("SUPABASE_SERVICE_ROLE_KEY")
    url = f"{base}/rest/v1/{table}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if conflict:
        headers["Prefer"] = f"resolution=merge-duplicates,return=minimal"
        url += f"?on_conflict={conflict}"
    else:
        headers["Prefer"] = "return=minimal"

    total = len(rows)
    for i in range(0, total, chunk_size):
        chunk = rows[i:i+chunk_size]
        print(f"[v0] Upserting {len(chunk)} rows into {table} [{i}/{total}]")
        resp = requests.post(url, headers=headers, data=json.dumps(chunk))
        if resp.status_code >= 400:
            print(f"[v0] Error upserting into {table}: {resp.status_code} {resp.text}")
            raise RuntimeError(resp.text)

def main():
    _must_env("SUPABASE_URL")
    _must_env("SUPABASE_SERVICE_ROLE_KEY")

    movies = fetch_csv(MOVIES_URL)     # movieId, title, genres
    links = fetch_csv(LINKS_URL)       # movieId, imdbId, tmdbId
    ratings = fetch_csv(RATINGS_URL)   # userId, movieId, rating, timestamp

    # Normalize keys to match DB schema
    movies_rows = [
        {"movie_id": m["movieId"], "title": m.get("title", ""), "genres": m.get("genres", "")}
        for m in movies if m.get("movieId")
    ]

    links_rows = [
        {"movie_id": l["movieId"], "imdb_id": l.get("imdbId", ""), "tmdb_id": l.get("tmdbId", "")}
        for l in links if l.get("movieId")
    ]

    ratings_rows = []
    for r in ratings:
        if not r.get("userId") or not r.get("movieId") or not r.get("rating"):
            continue
        try:
            rating = float(r["rating"])
        except:
            continue
        ts = None
        if r.get("timestamp"):
            try:
                ts = int(r["timestamp"])
            except:
                ts = None
        ratings_rows.append({
            "user_id": r["userId"],
            "movie_id": r["movieId"],
            "rating": rating,
            "ts": ts,
        })

    # Write local processed CSV used by later steps
    write_local_csv(
        os.path.join(OUTPUT_DIR, "interaction_log_processed.csv"),
        ratings_rows,
        fieldnames=["user_id", "movie_id", "rating", "ts"]
    )

    # Upsert in DB
    # 1) movies first (conflict on movie_id)
    supabase_upsert("movies", movies_rows, conflict="movie_id", chunk_size=1000)
    # 2) links second (conflict on movie_id)
    supabase_upsert("links", links_rows, conflict="movie_id", chunk_size=1000)
    # 3) ratings (conflict on composite key user_id,movie_id NOT supported directly by on_conflict param; we use merge-duplicates with both keys present)
    supabase_upsert("ratings", ratings_rows, conflict="user_id,movie_id", chunk_size=1000)

    print("[v0] Ingestion complete.")

if __name__ == "__main__":
    main()
