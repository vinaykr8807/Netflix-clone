/* Ensure required tables exist for ingestion/training without clobbering existing data */

-- processed_interactions: user ratings normalized for training
CREATE TABLE IF NOT EXISTS public.processed_interactions (
  user_id BIGINT NOT NULL,
  movie_id BIGINT NOT NULL,
  value DOUBLE PRECISION NOT NULL, -- rating 0.5..5.0
  ts BIGINT,                       -- unix seconds
  PRIMARY KEY (user_id, movie_id)
);

-- raw_movies: movie metadata (title, genres)
CREATE TABLE IF NOT EXISTS public.raw_movies (
  movie_id BIGINT PRIMARY KEY,
  title TEXT,
  genres TEXT
);

-- raw_links: external ids mapping (IMDB/TMDB)
CREATE TABLE IF NOT EXISTS public.raw_links (
  movie_id BIGINT PRIMARY KEY,
  imdb_id TEXT,
  tmdb_id BIGINT
);

-- recommendations: JSONB list of top items per user
CREATE TABLE IF NOT EXISTS public.recommendations (
  user_id BIGINT PRIMARY KEY,
  items JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_proc_interactions_user ON public.processed_interactions (user_id);
CREATE INDEX IF NOT EXISTS idx_proc_interactions_movie ON public.processed_interactions (movie_id);
CREATE INDEX IF NOT EXISTS idx_recs_updated_at ON public.recommendations (updated_at);

-- No RLS changes here. We assume server-side service role is used for writes.
