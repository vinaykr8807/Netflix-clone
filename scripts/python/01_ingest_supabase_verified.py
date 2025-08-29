import os, csv, json, time, math
from urllib import request, parse, error
from datetime import datetime, timezone
from pathlib import Path

MOVIES_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"
RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

BATCH = 500
OUT_DIR = Path("scripts/output")
OUT_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_INTERACTIONS_CSV = OUT_DIR / "interaction_log_processed.csv"

def _check_env():
  missing = []
  if not SUPABASE_URL: missing.append("SUPABASE_URL")
  if not SUPABASE_SERVICE_ROLE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
  if missing:
    raise RuntimeError(f"[v0] Missing required env vars: {', '.join(missing)}")
  print("[v0] Env OK: using SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")

def _http(method, path, body=None, params=None, headers=None):
  url = f"{SUPABASE_URL}{path}"
  if params:
    q = parse.urlencode(params, doseq=True)
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}{q}"
  data = None
  if body is not None:
    data = json.dumps(body).encode("utf-8")
  req = request.Request(url, method=method, data=data)
  req.add_header("apikey", SUPABASE_SERVICE_ROLE_KEY)
  req.add_header("Authorization", f"Bearer {SUPABASE_SERVICE_ROLE_KEY}")
  if body is not None:
    req.add_header("Content-Type", "application/json")
  if headers:
    for k, v in headers.items():
      req.add_header(k, v)
  try:
    with request.urlopen(req, timeout=120) as resp:
      content = resp.read()
      txt = content.decode("utf-8") if content else ""
      if txt:
        try:
          return json.loads(txt)
        except json.JSONDecodeError:
          return txt
      return None
  except error.HTTPError as e:
    msg = e.read().decode("utf-8", errors="ignore")
    raise RuntimeError(f"[v0] HTTP {e.code} {e.reason} at {url} :: {msg}")
  except error.URLError as e:
    raise RuntimeError(f"[v0] URL error at {url} :: {e}")

def _download(url, dest: Path):
  print(f"[v0] Downloading {url}")
  with request.urlopen(url, timeout=300) as resp, open(dest, "wb") as f:
    f.write(resp.read())

def _read_csv_rows(path: Path):
  with open(path, "r", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
      yield row

def _epoch_to_iso(ts_str: str):
  try:
    ts = int(float(ts_str))
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
  except Exception:
    return ts_str

def upsert_movies(movies_rows):
  print("[v0] Inserting into raw_movies… (no upsert)")
  batch = []
  total = 0
  for r in movies_rows:
    try:
      movie_id = int(r["movieId"])
    except Exception:
      continue
    title = (r.get("title") or "").strip()
    genres = (r.get("genres") or "").strip()
    batch.append({"movie_id": movie_id, "title": title, "genres": genres})
    if len(batch) >= BATCH:
      _http("POST", "/rest/v1/raw_movies", body=batch, headers={"Prefer":"return=minimal"})
      total += len(batch); batch = []
  if batch:
    _http("POST", "/rest/v1/raw_movies", body=batch, headers={"Prefer":"return=minimal"})
    total += len(batch)
  print(f"[v0] raw_movies inserted: {total}")

def _to_int_or_none(value):
  if value is None: return None
  s = str(value).strip()
  if not s: return None
  try:
    return int(s)
  except Exception:
    return None

def upsert_links(links_rows):
  print("[v0] Inserting into raw_links… (no upsert)")
  batch = []
  total = 0
  for r in links_rows:
    movie_id = _to_int_or_none(r.get("movieId"))
    if movie_id is None: continue
    imdb_id = (r.get("imdbId") or "").strip() or None
    tmdb_id = _to_int_or_none(r.get("tmdbId"))
    batch.append({"movie_id": movie_id, "imdb_id": imdb_id, "tmdb_id": tmdb_id})
    if len(batch) >= BATCH:
      _http("POST", "/rest/v1/raw_links", body=batch, headers={"Prefer":"return=minimal"})
      total += len(batch); batch = []
  if batch:
    _http("POST", "/rest/v1/raw_links", body=batch, headers={"Prefer":"return=minimal"})
    total += len(batch)
  print(f"[v0] raw_links inserted: {total}")

def upsert_ratings_and_processed(ratings_rows):
  print("[v0] Inserting into raw_ratings + processed_interactions… (no upsert)")
  batch_raw = []
  batch_proc = []
  total_proc = 0
  with open(PROCESSED_INTERACTIONS_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f); writer.writerow(["user_id","movie_id","value","ts"])
    for r in ratings_rows:
      user_id = _to_int_or_none(r.get("userId"))
      movie_id = _to_int_or_none(r.get("movieId"))
      if user_id is None or movie_id is None: continue
      try:
        rating = float(r.get("rating"))
      except Exception:
        continue
      ts_iso = _epoch_to_iso(r.get("timestamp"))
      # raw_ratings uses 'rating'
      batch_raw.append({"user_id": user_id, "movie_id": movie_id, "rating": rating, "ts": ts_iso})
      # processed_interactions uses 'value'
      proc_obj = {"user_id": user_id, "movie_id": movie_id, "value": rating, "ts": ts_iso}
      batch_proc.append(proc_obj)
      writer.writerow([user_id, movie_id, rating, ts_iso])
      if len(batch_raw) >= BATCH:
        _http("POST", "/rest/v1/raw_ratings", body=batch_raw, headers={"Prefer":"return=minimal"})
        _http("POST", "/rest/v1/processed_interactions", body=batch_proc, headers={"Prefer":"return=minimal"})
        total_proc += len(batch_proc); batch_raw = []; batch_proc = []
  if batch_raw:
    _http("POST", "/rest/v1/raw_ratings", body=batch_raw, headers={"Prefer":"return=minimal"})
    _http("POST", "/rest/v1/processed_interactions", body=batch_proc, headers={"Prefer":"return=minimal"})
    total_proc += len(batch_proc)
  print(f"[v0] processed_interactions inserted total: {total_proc}")
  print(f"[v0] Wrote {PROCESSED_INTERACTIONS_CSV}")

def main():
  _check_env()
  tmp_dir = Path("scripts/tmp"); tmp_dir.mkdir(parents=True, exist_ok=True)
  movies_csv = tmp_dir / "movies.csv"
  links_csv = tmp_dir / "links.csv"
  ratings_csv = tmp_dir / "ratings.csv"

  _download(MOVIES_URL, movies_csv)
  _download(LINKS_URL, links_csv)
  _download(RATINGS_URL, ratings_csv)

  upsert_movies(_read_csv_rows(movies_csv))
  upsert_links(_read_csv_rows(links_csv))
  upsert_ratings_and_processed(_read_csv_rows(ratings_csv))

  print("[v0] Ingestion complete.")

if __name__ == "__main__":
  try:
    main()
  except Exception as e:
    print(f"[v0] ERROR: {e}")
    raise
