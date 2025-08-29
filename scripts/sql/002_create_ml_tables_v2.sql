/* Create or ensure ingestion and recommendation tables exist (idempotent) */
-- Raw source tables
create table if not exists public.raw_movies (
  movie_id text primary key,
  title text not null,
  genres text not null default ''
);

create table if not exists public.raw_links (
  movie_id text primary key,
  imdb_id text,
  tmdb_id text
);

create table if not exists public.raw_ratings (
  user_id text not null,
  movie_id text not null,
  rating double precision not null,
  rated_at bigint,
  primary key (user_id, movie_id)
);

-- Processed/normalized interactions (mirror of ratings for downstream steps)
create table if not exists public.processed_interactions (
  user_id text not null,
  movie_id text not null,
  rating double precision,
  rated_at bigint,
  primary key (user_id, movie_id)
);

-- Recommendations table
create table if not exists public.recommendations (
  user_id text not null,
  movie_id text not null,
  score double precision not null,
  rank int not null,
  created_at timestamptz not null default now(),
  primary key (user_id, movie_id)
);

-- Helpful indexes
create index if not exists idx_raw_ratings_user on public.raw_ratings(user_id);
create index if not exists idx_raw_ratings_movie on public.raw_ratings(movie_id);
create index if not exists idx_processed_interactions_user on public.processed_interactions(user_id);
create index if not exists idx_processed_interactions_movie on public.processed_interactions(movie_id);
create index if not exists idx_recommendations_user on public.recommendations(user_id);
create index if not exists idx_recommendations_score on public.recommendations(score);
