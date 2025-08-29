-- Movies catalog
create table if not exists ml_movies (
  movie_id bigint primary key,
  title text not null,
  genres text
);

-- Links (to TMDB/IMDB)
create table if not exists ml_links (
  movie_id bigint primary key references ml_movies(movie_id) on delete cascade,
  imdb_id text,
  tmdb_id text
);

-- Ratings (interactions)
create table if not exists ml_ratings (
  user_id bigint not null,
  movie_id bigint not null references ml_movies(movie_id) on delete cascade,
  value real not null,
  ts bigint not null,
  constraint ml_ratings_pk primary key (user_id, movie_id)
);

-- Recommendations results per user
create table if not exists ml_recommendations (
  user_id bigint primary key,
  items jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now()
);

-- Helpful indexes
create index if not exists ml_ratings_movie_idx on ml_ratings(movie_id);
create index if not exists ml_ratings_user_idx on ml_ratings(user_id);
create index if not exists ml_recs_updated_idx on ml_recommendations(updated_at);
