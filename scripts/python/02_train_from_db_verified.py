import os, json, math
from urllib import request, parse, error
from collections import defaultdict
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def _check_env():
  missing = []
  if not SUPABASE_URL: missing.append("SUPABASE_URL")
  if not SUPABASE_SERVICE_ROLE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
  if missing: raise RuntimeError(f"[v0] Missing env: {', '.join(missing)}")

def _http(method, path, params=None, headers=None, body=None):
  url = f"{SUPABASE_URL}{path}"
  if params:
    q = parse.urlencode(params, doseq=True)
    url = f"{url}?{q}"
  data = json.dumps(body).encode("utf-8") if body is not None else None
  req = request.Request(url, method=method, data=data)
  req.add_header("apikey", SUPABASE_SERVICE_ROLE_KEY)
  req.add_header("Authorization", f"Bearer {SUPABASE_SERVICE_ROLE_KEY}")
  if body is not None:
    req.add_header("Content-Type", "application/json")
  if headers:
    for k,v in headers.items(): req.add_header(k,v)
  try:
    with request.urlopen(req, timeout=120) as resp:
      raw = resp.read()
      txt = raw.decode("utf-8") if raw else ""
      if txt:
        try: return json.loads(txt)
        except json.JSONDecodeError: return txt
      return None
  except error.HTTPError as e:
    msg = e.read().decode("utf-8", errors="ignore")
    raise RuntimeError(f"[v0] HTTP {e.code} {e.reason} at {url} :: {msg}")
  except error.URLError as e:
    raise RuntimeError(f"[v0] URL error at {url} :: {e}")

def fetch_all(table, select="*", page_size=5000):
  print(f"[v0] Fetching {table}…")
  rows = []
  start = 0
  while True:
    end = start + page_size - 1
    headers = {"Range": f"rows={start}-{end}"}
    params = {"select": select}
    batch = _http("GET", f"/rest/v1/{table}", params=params, headers=headers)
    if not batch or (isinstance(batch, list) and len(batch) == 0):
      print(f"[v0] No rows found in {table}.")
      break
    rows.extend(batch)
    print(f"[v0] {table} fetched {len(batch)} rows (total {len(rows)})")
    if len(batch) < page_size:
      break
    start += page_size
  return rows

def bayesian_scores(interactions, m_min=50, prior=3.5):
  per_movie = defaultdict(lambda: {"sum":0.0, "cnt":0})
  total = 0.0; n = 0
  for r in interactions:
    v = float(r["value"])
    total += v; n += 1
    pm = per_movie[int(r["movie_id"])]
    pm["sum"] += v; pm["cnt"] += 1
  global_mean = (total / n) if n else prior
  scores = {}
  for mid, agg in per_movie.items():
    c = agg["cnt"]; a = agg["sum"]/c
    score = (c/(c+m_min))*a + (m_min/(c+m_min))*global_mean
    scores[mid] = score
  return global_mean, scores

def main():
  _check_env()
  inter = fetch_all("processed_interactions", select="user_id,movie_id,value")
  if not inter:
    print("[v0] No interactions found in processed_interactions. Re-run 01_ingest_supabase_verified.py.")
    return
  movies = fetch_all("raw_movies", select="movie_id,title")
  links = fetch_all("raw_links", select="movie_id,tmdb_id")
  title_by_movie = {int(m["movie_id"]): m.get("title") for m in movies}
  tmdb_by_movie = {int(l["movie_id"]): l.get("tmdb_id") for l in links}

  # Build by user
  by_user = defaultdict(set)
  flat = []
  for r in inter:
    uid = int(r["user_id"]); mid = int(r["movie_id"]); val = float(r["value"])
    by_user[uid].add(mid); flat.append({"movie_id": mid, "value": val})

  global_mean, movie_scores = bayesian_scores(flat)
  print(f"[v0] Global mean: {round(global_mean,4)} | Movies rated: {len(movie_scores)} | Users: {len(by_user)}")

  # Top-N per user
  TOPN = 20
  items_by_user = {}
  for uid, seen in by_user.items():
    candidates = [(mid, s) for mid, s in movie_scores.items() if mid not in seen]
    candidates.sort(key=lambda x: x[1], reverse=True)
    top = candidates[:TOPN]
    items = []
    for mid, s in top:
      items.append({
        "movie_id": mid,
        "score": round(float(s),5),
        "title": title_by_movie.get(mid),
        "tmdb_id": tmdb_by_movie.get(mid),
      })
    items_by_user[uid] = items

  # Upsert into recommendations
  print("[v0] Upserting recommendations…")
  BATCH = 500
  batch = []; total = 0
  now = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
  for uid, items in items_by_user.items():
    batch.append({"user_id": uid, "items": items, "updated_at": now})
    if len(batch) >= BATCH:
      _http("POST", "/rest/v1/recommendations", headers={"Prefer":"resolution=merge-duplicates"}, params={"on_conflict":"user_id"}, body=batch)
      total += len(batch); batch = []
  if batch:
    _http("POST", "/rest/v1/recommendations", headers={"Prefer":"resolution=merge-duplicates"}, params={"on_conflict":"user_id"}, body=batch)
    total += len(batch)
  print(f"[v0] Upserted recommendations for users: {total}")

if __name__ == "__main__":
  try:
    main()
  except Exception as e:
    print(f"[v0] ERROR: {e}")
    raise
