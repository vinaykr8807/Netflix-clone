# Uses only stdlib. Requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.

import os
import sys
import json
import math
from urllib import request, parse
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Tuple

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def fail(msg: str):
    print(f"[v0] ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def http_get(path: str, select: str, range_start: int, range_end: int):
    if not SUPABASE_URL or not SERVICE_ROLE:
        fail("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")
    qs = f"select={parse.quote(select)}"
    url = f"{SUPABASE_URL}/rest/v1/{path}?{qs}"
    headers = {
        "apikey": SERVICE_ROLE,
        "Authorization": f"Bearer {SERVICE_ROLE}",
        "Prefer": "return=representation",
        "Range": f"{range_start}-{range_end}",
    }
    req = request.Request(url, headers=headers, method="GET")
    with request.urlopen(req, timeout=120) as resp:
        data = resp.read().decode("utf-8")
        items = json.loads(data) if data else []
        return items, resp

def http_post_upsert(path: str, rows: List[Dict], on_conflict: str):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{path}?on_conflict={parse.quote(on_conflict)}"
    payload = json.dumps(rows).encode("utf-8")
    headers = {
        "apikey": SERVICE_ROLE,
        "Authorization": f"Bearer {SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    req = request.Request(url, data=payload, headers=headers, method="POST")
    with request.urlopen(req, timeout=120) as resp:
        if resp.status not in (200, 201, 204):
            fail(f"Upsert to {path} returned {resp.status}")

def fetch_all(path: str, select: str, page: int = 20000):
    # Paginate using Range headers: 0-19999, 20000-39999, ...
    offset = 0
    all_rows = []
    while True:
        rows, resp = http_get(path, select, offset, offset + page - 1)
        if not rows:
            break
        all_rows.extend(rows)
        total = resp.headers.get("content-range", "")
        print(f"[v0] Fetched {len(rows)} from {path} [{total}]")
        offset += page
    return all_rows

def bayesian_score(movie_sum: float, movie_count: int, global_mean: float, m: float = 50.0):
    # Weighted average of movie mean and global mean
    if movie_count <= 0:
        return global_mean
    R = movie_sum / movie_count
    v = float(movie_count)
    return (v / (v + m)) * R + (m / (v + m)) * global_mean

def main():
    print("[v0] Loading processed_interactions (user_id, movie_id, value)...")
    interactions = fetch_all("processed_interactions", "user_id,movie_id,value")
    if not interactions:
        print("[v0] No interactions found. Did you run 01_ingest_supabase_final.py?")
        sys.exit(0)

    print("[v0] Loading raw_movies (movie_id,title,genres)...")
    movies = fetch_all("raw_movies", "movie_id,title,genres")
    print("[v0] Loading raw_links (movie_id,tmdb_id,imdb_id)...")
    links = fetch_all("raw_links", "movie_id,tmdb_id,imdb_id")

    # Build lookup tables
    title_by_movie: Dict[int, str] = {}
    for r in movies:
        mid = r.get("movie_id")
        if mid is not None:
            title_by_movie[int(mid)] = r.get("title") or ""

    tmdb_by_movie: Dict[int, int] = {}
    for r in links:
        mid = r.get("movie_id")
        tmdb = r.get("tmdb_id")
        if mid is not None and tmdb is not None:
            try:
                tmdb_by_movie[int(mid)] = int(tmdb)
            except Exception:
                continue

    # Aggregations
    sum_by_movie: Dict[int, float] = defaultdict(float)
    cnt_by_movie: Dict[int, int] = defaultdict(int)
    seen_by_user: Dict[int, set] = defaultdict(set)
    for r in interactions:
        try:
            uid = int(r["user_id"])
            mid = int(r["movie_id"])
            val = float(r["value"])
        except Exception:
            continue
        sum_by_movie[mid] += val
        cnt_by_movie[mid] += 1
        seen_by_user[uid].add(mid)

    total_sum = sum(sum_by_movie[m] for m in sum_by_movie.keys())
    total_cnt = sum(cnt_by_movie[m] for m in cnt_by_movie.keys())
    global_mean = (total_sum / total_cnt) if total_cnt > 0 else 3.0
    print(f"[v0] Global mean: {global_mean:.4f} | Movies rated: {len(cnt_by_movie)} | Users: {len(seen_by_user)}")

    # Compute scores
    score_by_movie: Dict[int, float] = {}
    for mid, cnt in cnt_by_movie.items():
        score_by_movie[mid] = bayesian_score(sum_by_movie[mid], cnt, global_mean, m=50.0)

    # Build per-user recommendations
    TOP_N = 20
    rec_rows = []
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Precompute sorted movie list once
    sorted_movies = sorted(score_by_movie.items(), key=lambda kv: kv[1], reverse=True)
    movie_ids_sorted = [mid for mid, _ in sorted_movies]

    for uid, seen in seen_by_user.items():
        items = []
        added = 0
        for mid in movie_ids_sorted:
            if mid in seen:
                continue
            score = score_by_movie.get(mid, global_mean)
            items.append({
                "movie_id": mid,
                "tmdb_id": tmdb_by_movie.get(mid),
                "title": title_by_movie.get(mid, ""),
                "score": round(float(score), 4),
            })
            added += 1
            if added >= TOP_N:
                break
        rec_rows.append({
            "user_id": uid,
            "items": items,
            "updated_at": now_iso,
        })

    print(f"[v0] Upserting recommendations for {len(rec_rows)} users...")
    # Batch upsert
    BATCH = 500
    for i in range(0, len(rec_rows), BATCH):
        http_post_upsert("recommendations", rec_rows[i:i+BATCH], on_conflict="user_id")

    print("[v0] Training/Upsert complete.")

if __name__ == "__main__":
    main()
