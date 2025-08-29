"""
Download CSVs from provided URLs, preprocess them, and load into Postgres (Supabase).
Requires env: POSTGRES_URL_NON_POOLING
Outputs: scripts/output/interaction_log_processed.csv, movies_processed.csv, links_processed.csv
"""

import os
import io
import sys
import time
import json
import math
import pandas as pd
import numpy as np
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from urllib.parse import urlparse

RATINGS_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/ratings-IiLogJYWPkkZnWuBdi1fdTeSyBNBts.csv"
MOVIES_URL = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/movies-QFbRyA2vveCs7siryKfN7JeU3KMxLc.csv"
LINKS_URL  = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/links-m0RXllpAEUKtKYQc8gPNYV1ldCL0iE.csv"

OUT_DIR = os.path.join("scripts", "output")
os.makedirs(OUT_DIR, exist_ok=True)

def fetch_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def to_timestamptz(unix_seconds: pd.Series) -> pd.Series:
    # Convert Unix seconds to UTC ISO format (psycopg can ingest)
    return pd.to_datetime(unix_seconds, unit="s", utc=True)

def preprocess():
    print("[v0] Downloading CSVs...")
    ratings = fetch_csv(RATINGS_URL)
    movies = fetch_csv(MOVIES_URL)
    links  = fetch_csv(LINKS_URL)

    # Standardize column names
    ratings = ratings.rename(columns={"userId":"user_id","movieId":"movie_id","rating":"value","timestamp":"timestamp"})
    movies  = movies.rename(columns={"movieId":"movie_id"})
    links   = links.rename(columns={"movieId":"movie_id","imdbId":"imdb_id","tmdbId":"tmdb_id"})

    # Clean and types
    ratings = ratings.dropna(subset=["user_id","movie_id","value","timestamp"])
    # Enforce dtypes
    ratings["user_id"]  = pd.to_numeric(ratings["user_id"], errors="coerce").astype("Int64")
    ratings["movie_id"] = pd.to_numeric(ratings["movie_id"], errors="coerce").astype("Int64")
    ratings["value"]    = pd.to_numeric(ratings["value"], errors="coerce").astype(float)
    ratings = ratings.dropna(subset=["user_id","movie_id","value","timestamp"])
    ratings["ts"] = to_timestamptz(ratings["timestamp"])
    ratings = ratings.drop(columns=["timestamp"])
    ratings = ratings.sort_values("ts").reset_index(drop=True)

    movies["movie_id"] = pd.to_numeric(movies["movie_id"], errors="coerce").astype("Int64")
    links["movie_id"]  = pd.to_numeric(links["movie_id"], errors="coerce").astype("Int64")
    links["tmdb_id"]   = pd.to_numeric(links["tmdb_id"], errors="coerce")

    # Save processed CSVs
    ratings_out = os.path.join(OUT_DIR, "interaction_log_processed.csv")
    movies_out  = os.path.join(OUT_DIR, "movies_processed.csv")
    links_out   = os.path.join(OUT_DIR, "links_processed.csv")

    ratings[["user_id","movie_id","value","ts"]].to_csv(ratings_out, index=False)
    movies[["movie_id","title","genres"]].to_csv(movies_out, index=False)
    links[["movie_id","imdb_id","tmdb_id"]].to_csv(links_out, index=False)

    print(f"[v0] Saved processed: {ratings_out}, {movies_out}, {links_out}")
    return ratings, movies, links

def load_to_db(ratings: pd.DataFrame, movies: pd.DataFrame, links: pd.DataFrame):
    dsn = os.getenv("POSTGRES_URL_NON_POOLING") or os.getenv("POSTGRES_URL")
    if not dsn:
        print("[v0] No POSTGRES_URL provided; skipping DB upload.")
        return

    print("[v0] Connecting to Postgres...")
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Ensure tables exist (idempotent)
            sql_path = os.path.join("scripts","sql","001_create_ml_tables.sql")
            if os.path.exists(sql_path):
                cur.execute(open(sql_path,"r",encoding="utf-8").read())
                conn.commit()

            # Upsert processed tables
            print("[v0] Inserting processed_interactions...")
            execute_values(
                cur,
                """
                INSERT INTO public.processed_interactions (user_id, movie_id, value, ts)
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                list(ratings[["user_id","movie_id","value","ts"]].itertuples(index=False, name=None))
            )

            print("[v0] Upserting processed_movies...")
            execute_values(
                cur,
                """
                INSERT INTO public.processed_movies (movie_id, title, genres)
                VALUES %s
                ON CONFLICT (movie_id) DO UPDATE SET
                  title = EXCLUDED.title,
                  genres = EXCLUDED.genres
                """,
                list(movies[["movie_id","title","genres"]].itertuples(index=False, name=None))
            )

            print("[v0] Upserting processed_links...")
            execute_values(
                cur,
                """
                INSERT INTO public.processed_links (movie_id, imdb_id, tmdb_id)
                VALUES %s
                ON CONFLICT (movie_id) DO UPDATE SET
                  imdb_id = EXCLUDED.imdb_id,
                  tmdb_id = EXCLUDED.tmdb_id
                """,
                list(links[["movie_id","imdb_id","tmdb_id"]].itertuples(index=False, name=None))
            )
        conn.commit()
    print("[v0] DB upload complete.")

if __name__ == "__main__":
    try:
        ratings, movies, links = preprocess()
        load_to_db(ratings, movies, links)
        print("[v0] Preprocess and DB load finished.")
    except Exception as e:
        print("[v0] Error:", str(e))
        sys.exit(1)
