#!/usr/bin/env python3
# Dependency-free ingestion with safe REST upserts and robust logging.
# - Converts UNIX 'timestamp' -> ISO-8601 UTC
# - Upserts raw_movies/raw_links on movie_id, processed_interactions on (user_id, movie_id)
# - Adapts batch size on 413 Payload Too Large
# - Prints table counts to verify upload
#
# Env required: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
# CSV sources: set below or pass CLI args if you prefer.

import os, sys, json, time, csv, math
from datetime import datetime, timezone
from urllib import request, parse, error

MOVIES_CSV = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_CSV  = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"
RATINGS_CSV= "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

def log(msg):
    print(f"[v0] {msg}", flush=True)

def fetch_csv(url):
    log(f"Downloading {url}")
    with request.urlopen(url) as resp:
        data = resp.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(data.splitlines())
    rows = list(reader)
    log(f"Fetched {len(rows)} rows from {url}")
    return rows

def iso_from_epoch(sec):
    try:
        s = int(float(sec))
        return datetime.fromtimestamp(s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def http_json(method, url, payload=None, headers=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = request.Request(url=url, data=data, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with request.urlopen(req) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), resp.headers, body
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"[v0] HTTP {e.code} {url} :: {body}")

def supabase_insert(table, rows, on_conflict=None, batch_size=1000):
    # Use upsert via Prefer: resolution=merge-duplicates + on_conflict
    if not rows:
        return 0
    base = f"{SUPABASE_URL}/rest/v1/{table}"
    q = []
    if on_conflict:
        q.append(("on_conflict", on_conflict))
    url = base + ("?" + parse.urlencode(q) if q else "")
    headers = {
        "apikey": SERVICE_ROLE,
        "Authorization": f"Bearer {SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    total = 0
    bs = max(100, batch_size)
    i = 0
    while i < len(rows):
        chunk = rows[i:i+bs]
        try:
            code, hdrs, body = http_json("POST", url, chunk, headers)
            total += len(chunk)
            i += bs
        except RuntimeError as e:
            msg = str(e)
            if "413" in msg and bs > 100:
                bs = max(100, bs // 2)
                log(f"Payload too large; reducing batch size to {bs} and retrying …")
                continue
            elif "409" in msg:
                # 409 with upsert almost always means on_conflict mismatch;
                # surface a clear hint.
                raise RuntimeError(f"[v0] Upsert conflict on {table}. Ensure unique index matches on_conflict={on_conflict}. Error: {msg}")
            else:
                raise
    return total

def supabase_count(table):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=*&limit=1"
    headers = {
        "apikey": SERVICE_ROLE,
        "Authorization": f"Bearer {SERVICE_ROLE}",
        "Prefer": "count=exact",
    }
    code, hdrs, body = http_json("GET", url, None, headers)
    cr = hdrs.get("Content-Range") or hdrs.get("content-range")
    if cr and "/" in cr:
        try:
            return int(cr.split("/")[-1])
        except Exception:
            pass
    # Fallback: try RPC count if available (optional)
    return -1

def main():
    if not SUPABASE_URL or not SERVICE_ROLE:
        log("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)
    log("Env OK: using SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")

    movies = fetch_csv(MOVIES_CSV)
    links  = fetch_csv(LINKS_CSV)
    ratings= fetch_csv(RATINGS_CSV)

    # Prepare rows
    movies_rows = []
    for r in movies:
        try:
            mid = int(r.get("movieId") or r.get("movie_id") or 0)
        except Exception:
            continue
        title = (r.get("title") or "").strip()
        genres = (r.get("genres") or "").strip()
        if mid:
            movies_rows.append({"movie_id": mid, "title": title, "genres": genres})
    log(f"raw_movies rows prepared: {len(movies_rows)}")

    links_rows = []
    for r in links:
        try:
            mid = int(r.get("movieId") or r.get("movie_id") or 0)
        except Exception:
            continue
        imdb_id = (r.get("imdbId") or r.get("imdb_id") or "").strip()
        tmdb_raw = (r.get("tmdbId") or r.get("tmdb_id") or "").strip()
        tmdb_id = None
        if tmdb_raw:
            try:
                tmdb_id = int(tmdb_raw)
            except Exception:
                tmdb_id = None
        if mid:
            row = {"movie_id": mid}
            if imdb_id != "":
                row["imdb_id"] = imdb_id
            if tmdb_id is not None:
                row["tmdb_id"] = tmdb_id
            links_rows.append(row)
    log(f"raw_links rows prepared: {len(links_rows)}")

    interactions_rows = []
    out_dir = "scripts/output"
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "interaction_log_processed.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["user_id", "movie_id", "value", "ts"])
        for r in ratings:
            try:
                uid = int(r.get("userId") or r.get("user_id") or 0)
                mid = int(r.get("movieId") or r.get("movie_id") or 0)
                val_raw = r.get("rating") or r.get("value") or "0"
                val = float(val_raw)
                ts_raw = r.get("timestamp") or r.get("ts") or ""
                ts_iso = iso_from_epoch(ts_raw) if ts_raw else None
            except Exception:
                continue
            if uid and mid:
                row = {"user_id": uid, "movie_id": mid, "value": val}
                if ts_iso:
                    row["ts"] = ts_iso
                interactions_rows.append(row)
                wr.writerow([uid, mid, val, ts_iso or ""])
    log(f"processed_interactions rows prepared: {len(interactions_rows)}")
    log(f"Wrote {out_csv}")

    # Upsert in batches
    try:
        inserted_movies = supabase_insert("raw_movies", movies_rows, on_conflict="movie_id", batch_size=1000)
        log(f"Upserted into raw_movies: {inserted_movies}")
    except RuntimeError as e:
        log(f"ERROR while upserting raw_movies: {e}")
        raise

    try:
        inserted_links = supabase_insert("raw_links", links_rows, on_conflict="movie_id", batch_size=1000)
        log(f"Upserted into raw_links: {inserted_links}")
    except RuntimeError as e:
        log(f"ERROR while upserting raw_links: {e}")
        raise

    # For processed_interactions we require a unique index (user_id, movie_id)
    try:
        inserted_inter = supabase_insert("processed_interactions", interactions_rows, on_conflict="user_id,movie_id", batch_size=2000)
        log(f"Upserted into processed_interactions: {inserted_inter}")
    except RuntimeError as e:
        log(f"ERROR while upserting processed_interactions: {e}")
        log("HINT: Run scripts/sql/013_add_unique_indexes.sql then re-run this script.")
        raise

    # Counts
    for t in ("raw_movies", "raw_links", "processed_interactions"):
        cnt = supabase_count(t)
        log(f"{t} count: {cnt}")

    log("Ingestion complete.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FAILED: {e}")
        sys.exit(1)
