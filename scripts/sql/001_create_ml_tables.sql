/*
  Create tables for raw data, processed interactions, and recommendations.
  Safe to run multiple times due to IF NOT EXISTS.
*/

-- Raw tables (optional; for reference if you want to keep raw imports)
CREATE TABLE IF NOT EXISTS public.raw_ratings (
  user_id BIGINT,
  movie_id BIGINT,
  rating REAL,
  ts TIMESTAMPTZ
);

-- Enable safe upserts for raw_ratings using composite key (required by PostgREST on_conflict)
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_ratings_user_movie
ON public.raw_ratings (user_id, movie_id);

CREATE TABLE IF NOT EXISTS public.raw_movies (
  movie_id BIGINT PRIMARY KEY,
  title TEXT,
  genres TEXT
);

CREATE TABLE IF NOT EXISTS public.raw_links (
  movie_id BIGINT PRIMARY KEY,
  imdb_id TEXT,
  tmdb_id BIGINT
);

-- Processed interactions (renamed rating -> value)
CREATE TABLE IF NOT EXISTS public.processed_interactions (
  user_id BIGINT,
  movie_id BIGINT,
  value REAL,
  ts TIMESTAMPTZ
);

-- enable safe upserts for processed_interactions
-- A unique index is required so PostgREST upsert (?on_conflict=user_id,movie_id) works.
CREATE UNIQUE INDEX IF NOT EXISTS ux_processed_interactions_user_movie
ON public.processed_interactions (user_id, movie_id);

-- Processed reference tables
CREATE TABLE IF NOT EXISTS public.processed_movies (
  movie_id BIGINT PRIMARY KEY,
  title TEXT,
  genres TEXT
);

CREATE TABLE IF NOT EXISTS public.processed_links (
  movie_id BIGINT PRIMARY KEY,
  imdb_id TEXT,
  tmdb_id BIGINT
);

-- Recommendations output table
CREATE TABLE IF NOT EXISTS public.recommendations (
  user_id BIGINT PRIMARY KEY,
  items JSONB NOT NULL, -- [{ movieId, title, tmdbId, score }]
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_processed_interactions_user_ts ON public.processed_interactions (user_id, ts);
CREATE INDEX IF NOT EXISTS idx_processed_interactions_movie ON public.processed_interactions (movie_id);
