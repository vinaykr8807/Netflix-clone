# Fetches CSVs from your provided URLs
# Upserts into Supabase via REST API using SUPABASE_SERVICE_ROLE_KEY
# Writes a processed CSV to scripts/output/interaction_log_processed.csv for downstream compatibility
import csv
import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse  # added
from typing import List, Dict, Any
from datetime import datetime, timezone

RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"
MOVIES_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

OUTPUT_DIR = "scripts/output"
PROCESSED_INTERACTIONS_CSV = os.path.join(OUTPUT_DIR, "interaction_log_processed.csv")

def fail(msg: str) -> None:
    print(f"[v0] ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def fetch_text(url: str) -> str:
    print(f"[v0] Downloading: {url}")
    try:
        with urllib.request.urlopen(url) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            data = resp.read()
            return data.decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        fail(f"HTTPError fetching {url}: {e.code} {e.reason}")
    except urllib.error.URLError as e:
        fail(f"URLError fetching {url}: {e.reason}")

def read_csv_from_text(text: str) -> List[Dict[str, str]]:
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = [row for row in reader]
    print(f"[v0] Parsed {len(rows)} rows")
    return rows

def chunk(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i+size] for i in range(0, len(items), size)]

def supabase_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
    if not rows:
        return
    assert SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY, "Missing Supabase env vars"
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?on_conflict={urllib.parse.quote(on_conflict)}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    body = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.getcode()
            if status not in (200, 201, 204):
                text = resp.read().decode("utf-8", errors="replace")
                fail(f"Upsert to {table} failed with status {status}: {text}")
    except urllib.error.HTTPError as e:
        error_text = e.read().decode("utf-8", errors="replace")
        fail(f"HTTPError upserting into {table}: {e.code} {e.reason} - {error_text}")
    except urllib.error.URLError as e:
        fail(f"URLError upserting into {table}: {e.reason}")

def ensure_output_dir() -> None:
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

def to_int(val: str) -> int:
    try:
        return int(val)
    except Exception:
        return 0

def to_iso_ts(epoch_str: str) -> str | None:
    if not epoch_str:
        return None
    try:
        dt = datetime.fromtimestamp(int(epoch_str), tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def map_movies(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        out.append({
            "movie_id": to_int(r.get("movieId") or r.get("movie_id") or "0"),
            "title": (r.get("title") or "")[:1024],
            "genres": (r.get("genres") or "")[:512],
        })
    return out

def map_links(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        out.append({
            "movie_id": to_int(r.get("movieId") or r.get("movie_id") or "0"),
            "imdb_id": (r.get("imdbId") or r.get("imdb_id") or "")[:32],
            "tmdb_id": to_int(r.get("tmdbId") or r.get("tmdb_id") or "0"),
        })
    return out

def map_ratings(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        try:
            rating_val = float(r.get("rating") or r.get("score") or "0")
        except ValueError:
            rating_val = 0.0
        ts_iso = to_iso_ts(r.get("timestamp") or r.get("ts") or "")
        out.append({
            "user_id": to_int((r.get("userId") or r.get("user_id") or "0")),
            "movie_id": to_int((r.get("movieId") or r.get("movie_id") or "0")),
            # raw_ratings schema
            "rating": rating_val,
            "ts": ts_iso,
            # processed_interactions schema mirrors with different column names:
            # value (REAL) and ts (TIMESTAMPTZ)
            "value": rating_val,
        })
    return out

def write_processed_interactions_csv(rows: List[Dict[str, Any]]) -> None:
    ensure_output_dir()
    with open(PROCESSED_INTERACTIONS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "movie_id", "value", "ts"])
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "user_id": r["user_id"],
                "movie_id": r["movie_id"],
                "value": r["value"],
                "ts": r["ts"],
            })
    print(f"[v0] Wrote {len(rows)} rows to {PROCESSED_INTERACTIONS_CSV}")

def main() -> None:
    print("[v0] Starting Supabase ingestion (stdlib)")

    if not SUPABASE_URL:
        fail("SUPABASE_URL or NEXT_PUBLIC_SUPABASE_URL is not set")
    if not SUPABASE_SERVICE_ROLE_KEY:
        fail("SUPABASE_SERVICE_ROLE_KEY is not set (required for REST upserts)")

    # Download CSVs
    movies_csv = fetch_text(MOVIES_URL)
    links_csv = fetch_text(LINKS_URL)
    ratings_csv = fetch_text(RATINGS_URL)

    # Parse
    movies_rows = read_csv_from_text(movies_csv)
    links_rows = read_csv_from_text(links_csv)
    ratings_rows = read_csv_from_text(ratings_csv)

    # Map to DB payloads
    movies_payload = map_movies(movies_rows)
    links_payload = map_links(links_rows)
    ratings_payload = map_ratings(ratings_rows)

    print(f"[v0] Movies: {len(movies_payload)}, Links: {len(links_payload)}, Ratings: {len(ratings_payload)}")

    # Upsert in chunks (keep payload well under 10MB)
    BATCH = 500
    # raw_movies
    for i, batch in enumerate(chunk(movies_payload, BATCH), start=1):
        print(f"[v0] Upserting raw_movies batch {i} ({len(batch)})")
        supabase_upsert("raw_movies", batch, on_conflict="movie_id")
        time.sleep(0.05)
    # raw_links
    for i, batch in enumerate(chunk(links_payload, BATCH), start=1):
        print(f"[v0] Upserting raw_links batch {i} ({len(batch)})")
        supabase_upsert("raw_links", batch, on_conflict="movie_id")
        time.sleep(0.05)
    # raw_ratings (note: uses user_id,movie_id for conflict)
    raw_ratings_payload = [{"user_id": r["user_id"], "movie_id": r["movie_id"], "rating": r["rating"], "ts": r["ts"]} for r in ratings_payload]
    for i, batch in enumerate(chunk(raw_ratings_payload, BATCH), start=1):
        print(f"[v0] Upserting raw_ratings batch {i} ({len(batch)})")
        supabase_upsert("raw_ratings", batch, on_conflict="user_id,movie_id")
        time.sleep(0.05)
    # processed_interactions mirrors ratings with (user_id,movie_id,value,ts)
    processed_payload = [{"user_id": r["user_id"], "movie_id": r["movie_id"], "value": r["value"], "ts": r["ts"]} for r in ratings_payload]
    for i, batch in enumerate(chunk(processed_payload, BATCH), start=1):
        print(f"[v0] Upserting processed_interactions batch {i} ({len(batch)})")
        supabase_upsert("processed_interactions", batch, on_conflict="user_id,movie_id")
        time.sleep(0.05)

    # Write processed CSV for compatibility with any existing scripts
    write_processed_interactions_csv(processed_payload)

    print("[v0] Ingestion complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        fail(f"Unhandled exception: {e}")
