"""
Fetch CSVs from remote URLs and ingest into Supabase processed_* tables via REST.
Also writes scripts/output/interaction_log_processed.csv for downstream steps.

Env:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
"""

import os, io, csv, json, time, requests
from typing import List, Dict

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"
MOVIES_URL  = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL   = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"

OUTPUT_DIR = "scripts/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def fetch_csv(url: str) -> List[Dict[str, str]]:
    print(f"[v0] Fetching CSV: {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    rows = [dict(row) for row in reader]
    print(f"[v0] Loaded {len(rows)} rows")
    return rows

def upsert(table: str, rows: List[Dict], conflict: str | None, chunk: int = 2000):
    base = _must_env("SUPABASE_URL").rstrip("/")
    key = _must_env("SUPABASE_SERVICE_ROLE_KEY")
    url = f"{base}/rest/v1/{table}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    if conflict:
        url += f"?on_conflict={conflict}"
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    total = len(rows)
    for i in range(0, total, chunk):
        batch = rows[i:i+chunk]
        print(f"[v0] Upserting {len(batch)} rows into {table} [{i}/{total}]")
        resp = requests.post(url, headers=headers, data=json.dumps(batch))
        if resp.status_code >= 400:
            print(resp.text[:1000])
            raise RuntimeError(f"Upsert failed for {table}: {resp.status_code}")

def main():
    _must_env("SUPABASE_URL"); _must_env("SUPABASE_SERVICE_ROLE_KEY")

    movies = fetch_csv(MOVIES_URL)     # movieId,title,genres
    links  = fetch_csv(LINKS_URL)      # movieId,imdbId,tmdbId
    ratings= fetch_csv(RATINGS_URL)    # userId,movieId,rating,timestamp

    processed_movies = [
        {"movie_id": int(m["movieId"]), "title": m.get("title") or "", "genres": m.get("genres") or ""}
        for m in movies if m.get("movieId")
    ]
    processed_links = [
        {"movie_id": int(l["movieId"]), "imdb_id": (l.get("imdbId") or ""), "tmdb_id": (int(l["tmdbId"]) if l.get("tmdbId") else None)}
        for l in links if l.get("movieId")
    ]

    processed_interactions = []
    for r in ratings:
        if not r.get("userId") or not r.get("movieId") or not r.get("rating"):
            continue
        try:
            user_id = int(r["userId"]); movie_id = int(r["movieId"]); val = float(r["rating"])
        except:
            continue
        ts_iso = None
        if r.get("timestamp"):
            try:
                ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(r["timestamp"])))
            except:
                ts_iso = None
        processed_interactions.append({"user_id": user_id, "movie_id": movie_id, "value": val, "ts": ts_iso})

    # Write local CSV for downstream steps
    out_csv = os.path.join(OUTPUT_DIR, "interaction_log_processed.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["user_id","movie_id","value","ts"])
        w.writeheader()
        for row in processed_interactions:
            w.writerow(row)
    print(f"[v0] Wrote {len(processed_interactions)} interactions -> {out_csv}")

    # Upsert processed tables (requires unique index for processed_interactions to truly upsert)
    upsert("processed_movies", processed_movies, conflict="movie_id", chunk=4000)
    upsert("processed_links",  processed_links,  conflict="movie_id", chunk=4000)
    upsert("processed_interactions", processed_interactions, conflict="user_id,movie_id", chunk=4000)

    print("[v0] Ingestion to processed_* complete.")

if __name__ == "__main__":
    main()
