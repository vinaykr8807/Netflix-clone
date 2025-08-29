#!/usr/bin/env python3
import os, csv, json, time
from urllib import request, parse, error
from datetime import datetime, timezone
from pathlib import Path

MOVIES_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"
RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

OUT_DIR = Path("scripts/output"); OUT_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_INTERACTIONS_CSV = OUT_DIR / "interaction_log_processed.csv"

def _check_env():
  missing = []
  if not SUPABASE_URL: missing.append("SUPABASE_URL")
  if not SUPABASE_SERVICE_ROLE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
  if missing: raise RuntimeError(f"[v0] Missing env: {', '.join(missing)}")
  print("[v0] Env OK")

def _http(method, path, body=None, params=None, headers=None, timeout=120):
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

def _download(url: str, dest: Path):
  print(f"[v0] Downloading {url}")
  with request.urlopen(url, timeout=300) as resp, open(dest, "wb") as f:
    f.write(resp.read())

def _read_csv_rows(path: Path):
  with open(path, "r", newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r: yield row

def _epoch_to_iso(ts_str: str):
  try:
    ts = int(float(ts_str))
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
  except Exception:
    return ts_str

def _to_int_or_none(v):
  if v is None: return None
  s = str(v).strip()
  if not s: return None
  try: return int(s)
  except Exception: return None

def _insert_rows(table: str, rows: list, prefer="return=minimal", initial_batch=1000):
  total, batch_size = 0, max(1, initial_batch)
  i = 0
  while i < len(rows):
    batch = rows[i:i+batch_size]
    try:
      _http("POST", f"/rest/v1/{table}", body=batch, headers={"Prefer": prefer})
      total += len(batch); i += len(batch)
    except RuntimeError as e:
      msg = str(e)
      if "HTTP 413" in msg or "Payload too large" in msg or "Request body too large" in msg or "row is too large" in msg or "HTTP 400" in msg:
        if batch_size == 1:
          raise
        old = batch_size; batch_size = max(1, batch_size // 2)
        print(f"[v0] {table} reduce batch {old} → {batch_size} due to: {msg[:140]}...")
      else:
        raise
  print(f"[v0] {table} inserted: {total}")

def ingest():
  _check_env()
  tmp = Path("scripts/tmp"); tmp.mkdir(parents=True, exist_ok=True)
  movies_csv, links_csv, ratings_csv = tmp/"movies.csv", tmp/"links.csv", tmp/"ratings.csv"

  _download(MOVIES_URL, movies_csv)
  _download(LINKS_URL, links_csv)
  _download(RATINGS_URL, ratings_csv)

  # raw_movies
  movies_rows = []
  for r in _read_csv_rows(movies_csv):
    mid = _to_int_or_none(r.get("movieId"))
    if mid is None: continue
    movies_rows.append({"movie_id": mid, "title": (r.get("title") or "").strip(), "genres": (r.get("genres") or "").strip()})
  print(f"[v0] raw_movies rows prepared: {len(movies_rows)}")
  _insert_rows("raw_movies", movies_rows)

  # raw_links
  links_rows = []
  for r in _read_csv_rows(links_csv):
    mid = _to_int_or_none(r.get("movieId"))
    if mid is None: continue
    imdb_id = (r.get("imdbId") or "").strip() or None
    tmdb_id = _to_int_or_none(r.get("tmdbId"))
    links_rows.append({"movie_id": mid, "imdb_id": imdb_id, "tmdb_id": tmdb_id})
  print(f"[v0] raw_links rows prepared: {len(links_rows)}")
  _insert_rows("raw_links", links_rows)

  # raw_ratings + processed_interactions
  raw_rows, proc_rows = [], []
  with open(PROCESSED_INTERACTIONS_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["user_id","movie_id","value","ts"])
    for r in _read_csv_rows(ratings_csv):
      uid = _to_int_or_none(r.get("userId")); mid = _to_int_or_none(r.get("movieId"))
      if uid is None or mid is None: continue
      try: rating = float(r.get("rating"))
      except Exception: continue
      ts_iso = _epoch_to_iso(r.get("timestamp"))
      raw_rows.append({"user_id": uid, "movie_id": mid, "rating": rating, "ts": ts_iso})
      proc_rows.append({"user_id": uid, "movie_id": mid, "value": rating, "ts": ts_iso})
      w.writerow([uid, mid, rating, ts_iso])
  print(f"[v0] raw_ratings rows prepared: {len(raw_rows)} | processed_interactions rows prepared: {len(proc_rows)}")
  _insert_rows("raw_ratings", raw_rows)
  _insert_rows("processed_interactions", proc_rows)
  print(f"[v0] Wrote {PROCESSED_INTERACTIONS_CSV}")
  print("[v0] Ingestion complete.")

if __name__ == "__main__":
  try:
    ingest()
  except Exception as e:
    print(f"[v0] ERROR: {e}")
    raise
