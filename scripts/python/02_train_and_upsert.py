# Dependency-free (stdlib only) and uses Supabase REST via SUPABASE_SERVICE_ROLE_KEY

import os, sys, json, time
from urllib import request, parse

SUPABASE_URL = (os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL") or "").rstrip("/")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def _need_env():
    missing = [k for k in ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"] if not os.getenv(k)]
    if missing:
        print(f"[v0] Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

def _rest_select(table: str, select: str):
    # Paged GET using Range headers to avoid large responses
    assert SUPABASE_URL and SERVICE_KEY
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={parse.quote(select)}"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Accept": "application/json",
    }
    page = 10000
    start = 0
    out = []
    while True:
        req = request.Request(url, method="GET", headers={**headers, "Range-Unit":"items", "Range": f"{start}-{start+page-1}"})
        with request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            out.extend(data)
            if len(data) < page:
                break
            start += page
    return out

def _rest_upsert_recommendations(rows):
    if not rows:
        print("[v0] No rows to upsert into recommendations.")
        return
    url = f"{SUPABASE_URL}/rest/v1/recommendations?on_conflict=user_id"
    body = json.dumps(rows).encode("utf-8")
    req = request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "resolution=merge-duplicates")
    req.add_header("apikey", SERVICE_KEY)
    req.add_header("Authorization", f"Bearer {SERVICE_KEY}")
    with request.urlopen(req) as resp:
        code = resp.getcode()
        if code not in (200, 201, 204):
            txt = resp.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Upsert recommendations failed: {code} {txt}")
    print(f"[v0] Upserted {len(rows)} users into recommendations.")

def bayesian_score(movie_means, movie_counts, global_mean, mid, m_prior=25.0):
    c = movie_counts.get(mid, 0)
    if c == 0:
        return global_mean
    return (m_prior * global_mean + c * movie_means[mid]) / (m_prior + c)

def main():
    _need_env()
    print("[v0] Loading processed_interactions (user_id, movie_id, value)...")
    interactions = _rest_select("processed_interactions", "user_id,movie_id,value")
    if not interactions:
        print("[v0] No interactions found. Did you run 01_ingest_supabase_stdlib.py?", file=sys.stderr)
        sys.exit(1)

    print("[v0] Loading raw_movies (movie_id,title,genres) ...")
    movies = { int(m["movie_id"]): m for m in _rest_select("raw_movies", "movie_id,title,genres") }

    print("[v0] Loading raw_links (movie_id,tmdb_id) ...")
    links = { int(l["movie_id"]): l for l in _rest_select("raw_links", "movie_id,tmdb_id") }

    # Compute per-movie mean and global mean from values
    sums, counts = {}, {}
    user_seen = {}
    for r in interactions:
        try:
            uid = int(r["user_id"]); mid = int(r["movie_id"]); val = float(r["value"])
        except Exception:
            continue
        sums[mid] = sums.get(mid, 0.0) + val
        counts[mid] = counts.get(mid, 0) + 1
        user_seen.setdefault(uid, set()).add(mid)

    if not counts:
        print("[v0] No valid ratings to compute.", file=sys.stderr)
        sys.exit(1)

    movie_means = { mid: (sums[mid] / counts[mid]) for mid in sums }
    global_mean = (sum(sums.values()) / sum(counts.values()))
    print(f"[v0] Global mean: {global_mean:.4f} | Movies rated: {len(movie_means)} | Users: {len(user_seen)}")

    # Precompute movie scores (Bayesian shrunk mean)
    movie_scores = {}
    for mid in movie_means.keys():
        movie_scores[mid] = bayesian_score(movie_means, counts, global_mean, mid, m_prior=25.0)

    # For each user, top-N unseen movies by score
    N = 20
    payload = []
    for uid, seen in user_seen.items():
        candidates = []
        for mid, score in movie_scores.items():
            if mid in seen:
                continue
            candidates.append((mid, score))
        candidates.sort(key=lambda x: x[1], reverse=True)
        top = candidates[:N]

        items = []
        for mid, score in top:
            m = movies.get(mid) or {}
            l = links.get(mid) or {}
            items.append({
                "movieId": mid,
                "title": m.get("title"),
                "tmdbId": l.get("tmdb_id"),
                "score": round(float(score), 5),
            })

        payload.append({
            "user_id": uid,
            "items": items,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        })

        # Batch every 200 users
        if len(payload) >= 200:
            _rest_upsert_recommendations(payload)
            payload = []

    if payload:
        _rest_upsert_recommendations(payload)

    print("[v0] Training complete. Recommendations updated.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[v0] ERROR: {e}", file=sys.stderr)
        sys.exit(1)
