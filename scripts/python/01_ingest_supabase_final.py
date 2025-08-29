# Uses: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY from environment
# Inputs: movies.csv, links.csv, ratings.csv (URLs below)
# Output: Upserts into raw_movies, raw_links, processed_interactions and writes scripts/output/interaction_log_processed.csv

import os
import sys
import csv
import json
import math
from urllib import request, parse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

MOVIES_CSV = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_CSV = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"
RATINGS_CSV = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"

BATCH_SIZE = 1000

def fail(msg: str):
    print(f"[v0] ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def http_post_json(path: str, rows: List[Dict], on_conflict: str = ""):
    if not rows:
        return
    if not SUPABASE_URL or not SERVICE_ROLE:
        fail("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    if on_conflict:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}on_conflict={parse.quote(on_conflict)}"
    payload = json.dumps(rows).encode("utf-8")
    headers = {
        "apikey": SERVICE_ROLE,
        "Authorization": f"Bearer {SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    req = request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=120) as resp:
            _ = resp.read()
            # 201 or 200 expected for upsert
            if resp.status not in (200, 201, 204):
                fail(f"Upsert to {path} returned status {resp.status}")
    except Exception as e:
        fail(f"HTTP error upserting to {path}: {e}")

def download_csv(url: str) -> List[Dict]:
    print(f"[v0] Downloading {url} ...")
    try:
        with request.urlopen(url, timeout=120) as resp:
            raw = resp.read()
    except Exception as e:
        fail(f"Failed to download {url}: {e}")
    # Best effort decode
    text = None
    for enc in ("utf-8", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        fail("Could not decode CSV bytes")
    reader = csv.DictReader(text.splitlines())
    rows = [dict(r) for r in reader]
    print(f"[v0] Parsed {len(rows)} rows from {url}")
    return rows

def to_int_or_none(s: str):
    try:
        return int(s)
    except Exception:
        return None

def chunked(rows: List[Dict], size: int):
    for i in range(0, len(rows), size):
        yield rows[i:i+size]

def main():
    print("[v0] Starting ingestion...")
    movies = download_csv(MOVIES_CSV)
    links = download_csv(LINKS_CSV)
    ratings = download_csv(RATINGS_CSV)

    # Prepare raw_movies
    movies_payload = []
    for r in movies:
        movie_id = to_int_or_none(r.get("movieId", "").strip())
        if movie_id is None:
            continue
        movies_payload.append({
            "movie_id": movie_id,
            "title": r.get("title", ""),
            "genres": r.get("genres", ""),
        })
    print(f"[v0] Upserting raw_movies in {math.ceil(len(movies_payload)/BATCH_SIZE)} batch(es)...")
    for batch in chunked(movies_payload, BATCH_SIZE):
        http_post_json("raw_movies", batch, on_conflict="movie_id")

    # Prepare raw_links
    links_payload = []
    for r in links:
        movie_id = to_int_or_none(r.get("movieId", "").strip())
        if movie_id is None:
            continue
        imdb_id = r.get("imdbId", "").strip()
        tmdb_id = to_int_or_none(r.get("tmdbId", "").strip())
        links_payload.append({
            "movie_id": movie_id,
            "imdb_id": imdb_id if imdb_id else None,
            "tmdb_id": tmdb_id,
        })
    print(f"[v0] Upserting raw_links in {math.ceil(len(links_payload)/BATCH_SIZE)} batch(es)...")
    for batch in chunked(links_payload, BATCH_SIZE):
        http_post_json("raw_links", batch, on_conflict="movie_id")

    # Prepare processed_interactions from ratings: epoch -> ISO UTC
    processed_payload = []
    out_dir = Path("scripts/output")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "interaction_log_processed.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "movie_id", "value", "ts"])
        for r in ratings:
            user_id = to_int_or_none(r.get("userId", "").strip())
            movie_id = to_int_or_none(r.get("movieId", "").strip())
            try:
                value = float(r.get("rating", "0").strip())
            except Exception:
                value = None
            ts_epoch = to_int_or_none(str(r.get("timestamp", "")).strip())
            if None in (user_id, movie_id, value, ts_epoch):
                continue
            ts_iso = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            processed_payload.append({
                "user_id": user_id,
                "movie_id": movie_id,
                "value": value,
                "ts": ts_iso,
            })
            w.writerow([user_id, movie_id, value, ts_iso])

    print(f"[v0] Upserting processed_interactions ({len(processed_payload)} rows) in {math.ceil(len(processed_payload)/BATCH_SIZE)} batch(es)...")
    for batch in chunked(processed_payload, BATCH_SIZE):
        http_post_json("processed_interactions", batch, on_conflict="user_id,movie_id")

    print(f"[v0] Wrote {out_path}")
    print("[v0] Ingestion complete.")

if __name__ == "__main__":
    main()
