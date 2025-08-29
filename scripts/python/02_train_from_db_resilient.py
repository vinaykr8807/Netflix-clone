#!/usr/bin/env python3
import os, json
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
  print("[v0] Env OK")

def _http(method, path, params=None, headers=None, body=None, timeout=120):
  url = f"{SUPABASE_URL}{path}"
  if params:
    q = parse.urlencode(params, doseq=True); url = f"{url}?{q}"
  data = json.dumps(body).encode("utf-8") if body is not None else None
  req = request.Request(url, method=method, data=data)
  req.add_header("apikey", SUPABASE_SERVICE_ROLE_KEY)
  req.add_header("Authorization", f"Bearer {SUPABASE_SERVICE_ROLE_KEY}")
  if body is not None: req.add_header("Content-Type", "application/json")
  if headers:
    for k, v in headers.items(): req.add_header(k, v)
  try:
    with request.urlopen(req, timeout=timeout) as resp:
      raw = resp.read()
      txt = raw.decode("utf-8") if raw else ""
      if txt:
        try: return json.loads(txt)
        except json.JSONDecodeError: return txt
      return None
  except error.HTTPError as e:
    msg = e.read().decode("utf-8", errors="ignore")
    raise RuntimeError(f"[v0] HTTP {e.code} {e.reason} {url} :: {msg}")
  except error.URLError as e:
    raise RuntimeError(f"[v0] URL error {url} :: {e}")

def fetch_all(table, select="*", page_size=5000):
  rows = []; start = 0
  while True:
    end = start + page_size - 1
    headers = {"Range": f"rows={start}-{end}"}
    params = {"select": select}
    batch = _http("GET", f"/rest/v1/{table}", params=params, headers=headers)
    if not batch or (isinstance(batch, list) and len(batch) == 0):
      break
    rows.extend(batch)
    print(f"[v0] {table} fetched {len(batch)} (total {len(rows)})")
    if len(batch) < page_size: break
    start += page_size
  return rows

def bayesian_scores(interactions, m_min=50, prior=3.5):
  by_movie = defaultdict(lambda: {"sum":0.0,"cnt":0})
  total=0.0; n=0
  for r in interactions:
    v=float(r["value"]); total+=v; n+=1
    agg=by_movie[int(r["movie_id"])]; agg["sum"]+=v; agg["cnt"]+=1
  mean = (total/n) if n else prior
  scores={}
  for mid,agg in by_movie.items():
    c=agg["cnt"]; a=agg["sum"]/c
    scores[mid]=(c/(c+m_min))*a + (m_min/(c+m_min))*mean
  return mean, scores

def _insert_recs(rows, prefer="resolution=merge-duplicates", conflict="user_id", initial_batch=1000):
  total, batch_size = 0, max(1, initial_batch)
  i=0
  while i < len(rows):
    batch = rows[i:i+batch_size]
    try:
      _http("POST", "/rest/v1/recommendations", params={"on_conflict":conflict}, headers={"Prefer": prefer}, body=batch)
      total += len(batch); i += len(batch)
    except RuntimeError as e:
      msg=str(e)
      if "HTTP 413" in msg or "Payload too large" in msg or "HTTP 400" in msg:
        if batch_size == 1: raise
        old=batch_size; batch_size=max(1,batch_size//2)
        print(f"[v0] recommendations reduce batch {old} → {batch_size} due to: {msg[:140]}...")
      else:
        raise
  print(f"[v0] Upserted recommendations for users: {total}")

def train():
  _check_env()
  inter = fetch_all("processed_interactions", select="user_id,movie_id,value")
  if not inter:
    print("[v0] No interactions found. Run 01_ingest_supabase_resilient.py first.")
    return
  movies = fetch_all("raw_movies", select="movie_id,title")
  links = fetch_all("raw_links", select="movie_id,tmdb_id")
  title_by_movie = {int(m["movie_id"]): m.get("title") for m in movies}
  tmdb_by_movie = {int(l["movie_id"]): l.get("tmdb_id") for l in links}

  by_user = defaultdict(set)
  flat=[]
  for r in inter:
    uid=int(r["user_id"]); mid=int(r["movie_id"]); val=float(r["value"])
    by_user[uid].add(mid); flat.append({"movie_id": mid, "value": val})

  mean, scores = bayesian_scores(flat)
  print(f"[v0] Global mean: {round(mean,4)} | Movies rated: {len(scores)} | Users: {len(by_user)}")

  TOPN=20; now=datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
  rec_rows=[]
  for uid, seen in by_user.items():
    cands=[(mid,s) for mid,s in scores.items() if mid not in seen]
    cands.sort(key=lambda x:x[1], reverse=True)
    top=cands[:TOPN]
    items=[{"movie_id": mid, "score": round(float(s),5), "title": title_by_movie.get(mid), "tmdb_id": tmdb_by_movie.get(mid)} for mid,s in top]
    rec_rows.append({"user_id": uid, "items": items, "updated_at": now})

  _insert_recs(rec_rows)

if __name__ == "__main__":
  try:
    train()
  except Exception as e:
    print(f"[v0] ERROR: {e}")
    raise
