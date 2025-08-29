import os
import sys
import json
import math
import time
import urllib.request
import urllib.error
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Set

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def fail(msg: str) -> None:
    print(f"[v0] ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

def sb_get(table: str, select: str, range_from: int = 0, range_to: int = 9999, filters: str = "") -> Tuple[List[Dict[str, Any]], int, int, int]:
    assert SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?select={urllib.parse.quote(select)}"
    if filters:
        url += f"&{filters}"
    req = urllib.request.Request(url, headers=sb_headers(), method="GET")
    # PostgREST pagination
    req.add_header("Range-Unit", "items")
    req.add_header("Range", f"{range_from}-{range_to}")
    try:
        with urllib.request.urlopen(req) as resp:
            content_range = resp.headers.get("Content-Range", "0-0/0")
            body = resp.read().decode("utf-8", errors="replace") or "[]"
            items = json.loads(body)
            # parse "start-end/total"
            parts = content_range.split("/")
            total = int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else len(items)
            start_end = parts[0].split("-")
            start = int(start_end[0]) if start_end and start_end[0].isdigit() else range_from
            end = int(start_end[1]) if len(start_end) > 1 and start_end[1].isdigit() else range_to
            return items, start, end, total
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        fail(f"HTTPError GET {table}: {e.code} {e.reason} - {err}")
    except urllib.error.URLError as e:
        fail(f"URLError GET {table}: {e.reason}")

def sb_get_all(table: str, select: str) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    start = 0
    page = 10000
    while True:
        items, s, e, total = sb_get(table, select, range_from=start, range_to=start + page - 1)
        all_items.extend(items)
        if e + 1 >= total:
            break
        start = e + 1
        time.sleep(0.05)
    print(f"[v0] Fetched {len(all_items)} rows from {table}")
    return all_items

def sb_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
    if not rows:
        return
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?on_conflict={urllib.parse.quote(on_conflict)}"
    headers = sb_headers()
    headers["Prefer"] = "resolution=merge-duplicates"
    body = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            if resp.getcode() not in (200, 201, 204):
                txt = resp.read().decode("utf-8", errors="replace")
                fail(f"Upsert {table} failed: {txt}")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="replace")
        fail(f"HTTPError upserting {table}: {e.code} {e.reason} - {txt}")

def main() -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        fail("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    # Load data
    interactions = sb_get_all("processed_interactions", "user_id,movie_id,value")
    movies = sb_get_all("processed_movies", "movie_id,title")
    links = sb_get_all("processed_links", "movie_id,tmdb_id")

    title_by_movie: Dict[int, str] = {int(m["movie_id"]): (m["title"] or "") for m in movies}
    tmdb_by_movie: Dict[int, int] = {int(l["movie_id"]): int(l["tmdb_id"] or 0) for l in links}

    # Compute global average score per movie
    sum_by_movie: Dict[int, float] = defaultdict(float)
    cnt_by_movie: Dict[int, int] = defaultdict(int)
    users_seen: Dict[int, Set[int]] = defaultdict(set)

    for row in interactions:
        uid = int(row["user_id"])
        mid = int(row["movie_id"])
        val = float(row["value"])
        sum_by_movie[mid] += val
        cnt_by_movie[mid] += 1
        users_seen[uid].add(mid)

    avg_by_movie: Dict[int, float] = {}
    for mid, total in sum_by_movie.items():
        c = cnt_by_movie[mid]
        if c > 0:
            avg_by_movie[mid] = total / c

    # Build recommendations per user (top-N unseen by global avg)
    NOW = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    all_movies = list(avg_by_movie.keys())
    all_movies.sort(key=lambda m: avg_by_movie.get(m, 0), reverse=True)

    rows: List[Dict[str, Any]] = []
    for uid, seen in users_seen.items():
        items: List[Dict[str, Any]] = []
        for mid in all_movies:
            if mid in seen:
                continue
            items.append({
                "movieId": mid,
                "title": title_by_movie.get(mid),
                "tmdbId": tmdb_by_movie.get(mid),
                "score": round(avg_by_movie.get(mid, 0.0), 6),
            })
            if len(items) >= 20:
                break
        rows.append({"user_id": uid, "items": items, "updated_at": NOW})

    print(f"[v0] Upserting recommendations for {len(rows)} users")
    # recommendations has PK user_id; conflict on user_id
    for i in range(0, len(rows), 200):
        sb_upsert("recommendations", rows[i:i+200], on_conflict="user_id")
        time.sleep(0.05)

    print("[v0] Prediction complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        fail(f"Unhandled exception: {e}")
