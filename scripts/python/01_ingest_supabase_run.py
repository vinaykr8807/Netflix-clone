# Requires env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
# Inputs: ratings.csv, movies.csv, links.csv (URLs below)
# Output: Populates processed_interactions, raw_movies, raw_links

import os
import json
import csv
import time
from urllib import request, parse, error

RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"
MOVIES_URL  = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL   = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def _check_env():
    if not SUPABASE_URL or not SERVICE_KEY:
        raise RuntimeError("[v0] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment")

def fetch_csv(url: str):
    print(f"[v0] Downloading CSV: {url}")
    with request.urlopen(url) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    rows = list(csv.DictReader(text.splitlines()))
    print(f"[v0] Loaded {len(rows)} rows from {url}")
    return rows

def _rest_url(table: str, params: dict) -> str:
    qp = parse.urlencode(params, doseq=True)
    return f"{SUPABASE_URL}/rest/v1/{table}?{qp}"

def _headers(prefer: str = "resolution=merge-duplicates") -> dict:
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }

def batched(iterable, n=1000):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch

def upsert(table: str, rows: list, on_conflict: str):
    if not rows:
        return 0
    url = _rest_url(table, {"on_conflict": on_conflict})
    data = json.dumps(rows).encode("utf-8")
    req = request.Request(url, data=data, headers=_headers())
    try:
        with request.urlopen(req) as resp:
            # Prefer: resolution=merge-duplicates returns 201/204; body can be empty
            status = resp.status
            print(f"[v0] Upserted {len(rows)} rows into {table} (HTTP {status})")
            return len(rows)
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[v0] ERROR upserting into {table}: HTTP {e.code} {e.reason}\n{body}")
        raise

def main():
    _check_env()
    t0 = time.time()

    ratings = fetch_csv(RATINGS_URL)
    movies  = fetch_csv(MOVIES_URL)
    links   = fetch_csv(LINKS_URL)

    # Normalize ratings -> processed_interactions
    proc_rows = []
    for r in ratings:
        try:
            user_id  = int(r.get("userId") or 0)
            movie_id = int(r.get("movieId") or 0)
            value    = float(r.get("rating") or 0)
            ts_epoch = int(r.get("timestamp") or 0)
            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_epoch)) if ts_epoch else None
        except Exception:
            continue
        if user_id and movie_id:
            proc_rows.append({
                "user_id": user_id,
                "movie_id": movie_id,
                "value": value,
                "ts": ts_iso,
            })

    # Normalize movies -> raw_movies
    movie_rows = []
    for m in movies:
        try:
            movie_id = int(m.get("movieId") or 0)
        except Exception:
            continue
        if movie_id:
            movie_rows.append({
                "movie_id": movie_id,
                "title": m.get("title"),
                "genres": m.get("genres"),
            })

    # Normalize links -> raw_links
    link_rows = []
    for l in links:
        try:
            movie_id = int(l.get("movieId") or 0)
        except Exception:
            continue
        if movie_id:
            imdb_id = l.get("imdbId")
            tmdb_id_raw = l.get("tmdbId")
            tmdb_id = int(tmdb_id_raw) if tmdb_id_raw and tmdb_id_raw.isdigit() else None
            link_rows.append({
                "movie_id": movie_id,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
            })

    print(f"[v0] Prepared rows -> processed_interactions: {len(proc_rows)}, raw_movies: {len(movie_rows)}, raw_links: {len(link_rows)}")

    total = 0
    for batch in batched(proc_rows, 2000):
        total += upsert("processed_interactions", batch, "user_id,movie_id")
    print(f"[v0] Upserted processed_interactions total: {total}")

    total = 0
    for batch in batched(movie_rows, 2000):
        total += upsert("raw_movies", batch, "movie_id")
    print(f"[v0] Upserted raw_movies total: {total}")

    total = 0
    for batch in batched(link_rows, 2000):
        total += upsert("raw_links", batch, "movie_id")
    print(f"[v0] Upserted raw_links total: {total}")

    print(f"[v0] Ingestion complete in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
