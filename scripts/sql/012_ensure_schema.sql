-- Safe schema ensure for CSV→DB→recs pipeline.
-- Only creates tables if missing and adds helpful non-unique indexes.
-- Matches live schema types to avoid conflicts.

create table if not exists public.raw_movies (
  movie_id bigint primary key,
  title text,
  genres text
);
create index if not exists idx_raw_movies_movie on public.raw_movies (movie_id);

create table if not exists public.raw_links (
  movie_id bigint primary key,
  imdb_id text,
  tmdb_id bigint
);
create index if not exists idx_raw_links_movie on public.raw_links (movie_id);

create table if not exists public.raw_ratings (
  user_id bigint,
  movie_id bigint,
  rating real,
  ts timestamptz
);
create index if not exists idx_raw_ratings_user on public.raw_ratings (user_id);
create index if not exists idx_raw_ratings_movie on public.raw_ratings (movie_id);

create table if not exists public.processed_interactions (
  user_id bigint not null,
  movie_id bigint not null,
  value real not null,
  ts timestamptz not null
  -- note: no PK here to avoid conflicts with existing data; training doesn’t require it
);
create index if not exists idx_processed_interactions_user on public.processed_interactions (user_id);
create index if not exists idx_processed_interactions_movie on public.processed_interactions (movie_id);

create table if not exists public.recommendations (
  user_id bigint primary key,
  items jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now()
);
