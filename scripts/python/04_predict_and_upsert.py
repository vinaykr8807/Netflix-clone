"""
Predict top-N per user using global movie_scores and exclude titles each user has already seen.
No psycopg2 required; uses Supabase REST.

Inputs (from earlier steps):
- scripts/output/interaction_log_processed.csv
- scripts/output/movie_scores.csv  (movie_id,score)

Writes:
- Upserts into public.recommendations with on_conflict=user_id.

Env:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
"""

import os, csv, json, requests
from collections import defaultdict

OUTPUT_DIR = "scripts/output"
INTERACTIONS = os.path.join(OUTPUT_DIR, "interaction_log_processed.csv")
SCORES = os.path.join(OUTPUT_DIR, "movie_scores.csv")

def _must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def supabase_get_all(table: str, select: str = "*"):
    base = _must_env("SUPABASE_URL").rstrip("/")
    key = _must_env("SUPABASE_SERVICE_ROLE_KEY")
    url = f"{base}/rest/v1/{table}?select={select}"
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    out = []
    start = 0
    page = 1000
    while True:
        hdrs = headers | {"Range": f"{start}-{start+page-1}"}
        r = requests.get(url, headers=hdrs, timeout=120)
        if r.status_code not in (200, 206):
            raise RuntimeError(f"GET {table} failed: {r.status_code} {r.text[:300]}")
        batch = r.json()
        out.extend(batch)
        if len(batch) < page:
            break
        start += page
    return out

def supabase_upsert(table: str, rows, conflict: str):
    base = _must_env("SUPABASE_URL").rstrip("/")
    key = _must_env("SUPABASE_SERVICE_ROLE_KEY")
    url = f"{base}/rest/v1/{table}?on_conflict={conflict}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    r = requests.post(url, headers=headers, data=json.dumps(rows))
    if r.status_code >= 400:
        raise RuntimeError(f"Upsert {table} failed: {r.status_code} {r.text[:300]}")

def load_scores(path: str):
    scores = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                mid = int(row["movie_id"]); s = float(row["score"])
                scores.append((mid, s))
            except:
                continue
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores

def main(top_k: int = 10):
    if not os.path.exists(INTERACTIONS):
        raise FileNotFoundError(f"{INTERACTIONS} not found. Run 01_ingest_supabase_v2.py first.")
    if not os.path.exists(SCORES):
        raise FileNotFoundError(f"{SCORES} not found. Run 03_train_baseline.py first.")

    # Build per-user "seen" set and user list
    seen = defaultdict(set)
    with open(INTERACTIONS, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                uid = int(row["user_id"]); mid = int(row["movie_id"])
                seen[uid].add(mid)
            except:
                continue
    users = sorted(seen.keys())
    print(f"[v0] Users with interactions: {len(users)}")

    # Load metadata from Supabase for title/tmdb_id
    movies = { int(m["movie_id"]): (m.get("title") or "") for m in supabase_get_all("processed_movies", "movie_id,title") }
    links  = { int(l["movie_id"]): (int(l["tmdb_id"]) if l.get("tmdb_id") is not None else None) for l in supabase_get_all("processed_links", "movie_id,tmdb_id") }

    # Load global scores
    ranked = load_scores(SCORES)
    ranked_movie_ids = [mid for mid,_ in ranked]
    score_map = {mid: s for mid,s in ranked}

    # Build per-user recs (exclude seen)
    payload = []
    for uid in users:
        recs = []
        for mid in ranked_movie_ids:
            if mid in seen[uid]:
                continue
            recs.append({
                "movieId": mid,
                "title": movies.get(mid, ""),
                "tmdbId": links.get(mid, None),
                "score": float(score_map.get(mid, 0.0)),
            })
            if len(recs) >= top_k:
                break
        payload.append({"user_id": uid, "items": recs})

    # Upsert recommendations
    print(f"[v0] Upserting recommendations for {len(payload)} users")
    CHUNK = 1000
    for i in range(0, len(payload), CHUNK):
        supabase_upsert("recommendations", payload[i:i+CHUNK], conflict="user_id")

    print("[v0] Done.")

if __name__ == "__main__":
    main()
