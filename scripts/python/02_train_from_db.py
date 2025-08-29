#          and upserts JSONB recommendations per user into the recommendations table.
# Requires env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import os
import json
import math
from collections import defaultdict
from urllib import request, error

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

PAGE_SIZE = 5000

def _check_env():
    if not SUPABASE_URL or not SERVICE_KEY:
        raise RuntimeError("[v0] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

def _headers(extra=None):
    h = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    if extra:
        h.update(extra)
    return h

def _get(table: str, select: str, range_from: int, range_to: int):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
    req = request.Request(url, headers=_headers({
        "Range": f"{range_from}-{range_to}",
        "Range-Unit": "items",
    }))
    with request.urlopen(req) as resp:
        data = resp.read().decode("utf-8", errors="replace")
        return json.loads(data or "[]")

def _upsert(table: str, rows: list, on_conflict: str):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    data = json.dumps(rows).encode("utf-8")
    req = request.Request(url, data=data, headers=_headers({"Prefer": "resolution=merge-duplicates"}))
    with request.urlopen(req) as resp:
        # noop: 201/204
        return

def fetch_all(table: str, select: str):
    items = []
    start = 0
    while True:
        chunk = _get(table, select, start, start + PAGE_SIZE - 1)
        items.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return items

def bayesian_score(votes, avg, C, m=20):
    # Weighted rating (IMDb formula)
    v = votes
    R = avg
    return (v/(v+m))*R + (m/(v+m))*C

def main():
    _check_env()

    print("[v0] Loading processed_interactions (user_id, movie_id, value)...")
    interactions = fetch_all("processed_interactions", "user_id,movie_id,value")
    if not interactions:
        print("[v0] No interactions found. Did you run scripts/python/01_ingest_supabase_run.py?")
        return

    print(f"[v0] Interactions: {len(interactions)}")

    # Compute movie aggregates
    movie_sum = defaultdict(float)
    movie_cnt = defaultdict(int)
    users_seen = defaultdict(set)

    for r in interactions:
        uid = int(r["user_id"])
        mid = int(r["movie_id"])
        val = float(r["value"])
        movie_sum[mid] += val
        movie_cnt[mid] += 1
        users_seen[uid].add(mid)

    # Global mean rating C
    total_sum = sum(movie_sum.values())
    total_cnt = sum(movie_cnt.values()) or 1
    C = total_sum / total_cnt
    print(f"[v0] Global mean rating C={C:.4f}")

    # Precompute scores per movie
    movie_avg = {mid: (movie_sum[mid]/movie_cnt[mid]) for mid in movie_cnt}
    movie_score = {mid: bayesian_score(movie_cnt[mid], movie_avg[mid], C, m=20) for mid in movie_cnt}

    # Load metadata for join
    print("[v0] Loading raw_movies (for titles) and raw_links (for tmdb_id)...")
    movies = {int(m["movie_id"]): m for m in fetch_all("raw_movies", "movie_id,title,genres")}
    links  = {int(l["movie_id"]): l for l in fetch_all("raw_links", "movie_id,tmdb_id,imdb_id")}

    # Build recommendations per user
    rec_rows = []
    TOP_K = 20
    users = list(users_seen.keys())
    print(f"[v0] Building recommendations for {len(users)} users...")
    for uid in users:
        seen = users_seen[uid]
        # Candidate movies: not seen
        candidates = [mid for mid in movie_score.keys() if mid not in seen]
        # Sort by score desc
        candidates.sort(key=lambda m: movie_score.get(m, 0.0), reverse=True)
        top = candidates[:TOP_K]

        items = []
        for mid in top:
            md = movies.get(mid) or {}
            lk = links.get(mid) or {}
            items.append({
                "movie_id": mid,
                "title": md.get("title"),
                "genres": md.get("genres"),
                "tmdb_id": lk.get("tmdb_id"),
                "score": round(float(movie_score.get(mid, 0.0)), 6),
            })

        rec_rows.append({
            "user_id": uid,
            "items": items,
        })

        if len(rec_rows) >= 500:
            _upsert("recommendations", rec_rows, "user_id")
            rec_rows = []

    if rec_rows:
        _upsert("recommendations", rec_rows, "user_id")

    print("[v0] Upserted recommendations for all users.")

if __name__ == "__main__":
    try:
        main()
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[v0] HTTPError {e.code}: {e.reason}\n{body}")
        raise
    except Exception as ex:
        print(f"[v0] ERROR: {ex}")
        raise
